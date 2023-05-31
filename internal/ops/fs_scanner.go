package ops

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/studio1767/s3backup/internal/job"
)

func NewFsScanner(ctx context.Context, source string, job *job.Job) <-chan *EntryInfo {

	// make sure we have a trailing slash... assumed in the main loop
	if !strings.HasSuffix(source, "/") {
		source += "/"
	}

	// convert the include/exclude files to maps for easier lookup
	include_top_dirs := make(map[string]bool)
	exclude_top_dirs := make(map[string]bool)
	skip_dirs := make(map[string]bool)
	skip_dir_items := make(map[string]bool)

	for _, dir := range job.IncludeTopDirs {
		include_top_dirs[dir] = true
	}
	for _, dir := range job.ExcludeTopDirs {
		exclude_top_dirs[dir] = true
	}
	for _, dir := range job.SkipDirs {
		skip_dirs[dir] = true
	}
	for _, dir := range job.SkipDirItems {
		skip_dir_items[dir] = true
	}

	out := make(chan *EntryInfo, 10)
	fs := fsScanner{
		ctx:              ctx,
		out:              out,
		source:           source,
		include_top_dirs: include_top_dirs,
		exclude_top_dirs: exclude_top_dirs,
		skip_dirs:        skip_dirs,
		skip_dir_items:   skip_dir_items,
	}
	go func() {
		defer close(fs.out)
		fs.run(fs.source, 0)
	}()

	return out
}

type fsScanner struct {
	ctx              context.Context
	out              chan<- *EntryInfo
	source           string
	include_top_dirs map[string]bool
	exclude_top_dirs map[string]bool
	skip_dirs        map[string]bool
	skip_dir_items   map[string]bool
}

func (fs *fsScanner) run(dir string, level int) {
	// check for the existance of skip_dir_files
	for skip, _ := range fs.skip_dir_items {
		_, err := os.Stat(filepath.Join(dir, skip))
		if err == nil {
			// no error, so the file exists... bail out
			return
		}
	}

	// read the directory contents
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	// loop over the source files
	for _, entry := range entries {
		// check for context done
		select {
		case <-fs.ctx.Done():
			return
		default:
		}

		if entry.Type().IsRegular() {

			fpath := filepath.Join(dir, entry.Name())
			rpath := strings.TrimPrefix(fpath, fs.source)

			info, err := entry.Info()
			if err != nil {
				continue
			}

			fs.out <- &EntryInfo{
				Status:  StatusNew,
				RelPath: rpath,
				RawSize: info.Size(),
				ModTime: info.ModTime().Unix(),
				Mode:    info.Mode(),
				Action:  NoAction,
			}

		} else if entry.Type().IsDir() {
			// run some checks to see if we're skipping this directory
			skip_dir := false

			// if we're at level 0, check the top level include/exclude lists
			if level == 0 {
				if len(fs.include_top_dirs) > 0 {
					skip_dir = !fs.include_top_dirs[entry.Name()]
				}
				if len(fs.exclude_top_dirs) > 0 && fs.exclude_top_dirs[entry.Name()] {
					skip_dir = true
				}
			}

			// check skip_dirs at all levels
			if skip_dir == false && len(fs.skip_dirs) > 0 {
				skip_dir = fs.skip_dirs[entry.Name()]
			}

			// scan into the subdirectory
			if skip_dir == false {
				fpath := filepath.Join(dir, entry.Name())
				fs.run(fpath, level+1)
			}

		}
	}
}
