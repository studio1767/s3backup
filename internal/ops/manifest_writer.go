package ops

import (
	"context"
	"fmt"
	"io"
	"net/url"
)

func NewManifestWriter(ctx context.Context, in <-chan *EntryInfo, mwriter io.Writer) <-chan *EntryInfo {

	out := make(chan *EntryInfo, 10)
	mw := manifestWriter{
		ctx:    ctx,
		in:     in,
		out:    out,
		writer: mwriter,
	}
	go mw.run()

	return out
}

// Writes ItemInfo objects to the manifest.
type manifestWriter struct {
	ctx    context.Context
	in     <-chan *EntryInfo
	out    chan<- *EntryInfo
	writer io.Writer
}

func (mw *manifestWriter) run() {
	defer close(mw.out)

	for {
		// check the channels
		select {
		case <-mw.ctx.Done():
			return
		case info, ok := <-mw.in:
			if !ok {
				return
			}
			mw.process(info)
		}
	}
}

func (mw *manifestWriter) process(info *EntryInfo) {
	// don't write missing items to the manifest
	if info.Status == StatusNotFound {
		mw.out <- info
		return
	}

	// if something has gone wrong write a zero modtime to
	//   force retrying an upload next time around
	mtime := info.ModTime
	if info.Action == Failed {
		mtime = 0
	}

	// encode the path
	path := url.PathEscape(info.RelPath)

	line := fmt.Sprintf("%d,%d,0%o,%s,%s\n",
		info.RawSize,
		mtime,
		info.Mode&0777,
		info.Hash,
		path,
	)
	_, err := mw.writer.Write([]byte(line))
	if err != nil {
		info.Action = Failed
		info.ActionMessage = "failed writing entry to manifest"
	}

	mw.out <- info
}
