package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	humanize "github.com/dustin/go-humanize"

	"github.com/studio1767/s3backup/internal/job"
	"github.com/studio1767/s3backup/internal/manifest"
	"github.com/studio1767/s3backup/internal/ops"
	"github.com/studio1767/s3backup/internal/s3io"
)

func main() {
	// process the command line
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-v] [-p aws-profile] [-s secrets-file] [-c] <bucket> <job> [<label>]\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	verbose := flag.Bool("v", false, "verbose reporting")
	compress := flag.Bool("c", false, "compress data before backing up")
	profile := flag.String("p", "default", "aws profile for credentials and configuration")
	secrets_file := flag.String("s", "default", "yaml file containing secret passphrases for metadata")
	flag.Parse()

	if flag.NArg() != 2 && flag.NArg() != 3 {
		fmt.Fprintf(os.Stderr, "Error: incorrect arguments provided\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	jobname := flag.Arg(1)
	label := ""
	if flag.NArg() == 3 {
		label = flag.Arg(2)
	}

	// create the s3 client
	client, err := s3io.NewClient(*profile, bucket, "default", *secrets_file)
	if err != nil {
		log.Fatal(err)
	}

	// download the job
	job, _, err := job.Download(client, jobname)
	if err != nil {
		log.Fatal(err)
	}

	// backup the sources
	for idx, source := range job.Sources {
		fmt.Printf("--------------------------------------------------------------\n")

		if label != "" && label != source.Label {
			fmt.Printf("Skipping %s/%s\n", job.Name, source.Label)
			continue
		}

		fi, err := os.Stat(source.Path)
		if err != nil {
			fmt.Printf("Error: failed to stat source: %s: %s\n", source.Label, err)
			continue
		}
		if fi.IsDir() == false {
			fmt.Printf("Error: source is not a directory: %s\n", source.Path)
			continue
		}

		err = backupSource(client, job, idx, *compress, *verbose)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func backupSource(client s3io.Client, job *job.Job, idx int, compress, verbose bool) error {
	source := job.Sources[idx]

	// download the manifest for the label
	mreader, mkey, err := manifest.Download(client, job.Name, source.Label)

	var nomanifest *manifest.ErrNoSuchManifest
	if err != nil && errors.As(err, &nomanifest) == false {
		return err
	}
	if mreader != nil {
		defer mreader.Close()
		defer os.Remove(mreader.Name())
	}

	if mreader == nil {
		fmt.Printf("Processing %s/%s\n", job.Name, source.Label)
	} else {
		fmt.Printf("Processing %s/%s - %s\n", job.Name, source.Label, mkey)
	}

	// create the manifest file to write to
	stamp := time.Now().Unix()
	tmpfile := filepath.Join(os.TempDir(), fmt.Sprintf("manifest-%05d.csv", stamp))
	mwriter, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer mwriter.Close()
	defer os.Remove(mwriter.Name())

	// context to cancel the operation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// build the file processing chain
	ch := ops.NewFsScanner(ctx, source.Path, job)

	if len(job.IncludeExtensions) > 0 {
		ch = ops.NewFileExtensionFilter(ctx, ch, job.IncludeExtensions, true)
	}
	if len(job.ExcludeExtensions) > 0 {
		ch = ops.NewFileExtensionFilter(ctx, ch, job.ExcludeExtensions, false)
	}

	// build the manifest processing chain
	if mreader != nil {
		// create the manifest reader
		mch := ops.NewManifestScanner(ctx, mreader)

		// and combine the streams...
		ch = ops.NewStreamComparer(ctx, ch, mch)
	}

	// build the tail of the chain
	ch = ops.NewHashGenerator(ctx, ch, source.Path)
	ch = ops.NewUploader(ctx, ch, client, source.Path, compress)
	ch = ops.NewManifestWriter(ctx, ch, mwriter)

	// run the chain
	total := 0
	count_ok := 0
	count_new := 0
	count_modified := 0
	count_notfound := 0
	count_noaction := 0
	count_uploaded := 0
	count_failed := 0
	var bytes_uploaded int64 = 0
	for ei := range ch {
		total++

		switch ei.Status {
		case ops.StatusOk:
			count_ok++
		case ops.StatusNew:
			count_new++
		case ops.StatusModified:
			count_modified++
		case ops.StatusNotFound:
			count_notfound++
		}

		switch ei.Action {
		case ops.NoAction:
			count_noaction++
		case ops.Uploaded:
			count_uploaded++
			bytes_uploaded += ei.UploadedSize
		case ops.Failed:
			count_failed++
		}

		if ei.Status == ops.StatusNew || ei.Status == ops.StatusModified {
			if ei.Action == ops.Uploaded {
				fmt.Printf("- uploaded: %s (%d, %d)\n", ei.RelPath, ei.RawSize, ei.UploadedSize)
			} else if verbose && ei.Action == ops.NoAction {
				fmt.Printf("-  present: %s (%d)\n", ei.RelPath, ei.RawSize)
			}
		}
		if ei.Action == ops.Failed {
			fmt.Printf("-   failed: %s\n", ei.RelPath)
		}
		if verbose && ei.Status == ops.StatusNotFound {
			fmt.Printf("-  missing: %s\n", ei.RelPath)
		}
	}

	// upload the manifest
	if count_new > 0 || count_modified > 0 {
		mwriter.Seek(0, io.SeekStart)
		key, err := manifest.Upload(client, mwriter, job.Name, source.Label)
		if err != nil {
			return err
		}
		fmt.Printf("- uploaded: %s\n", key)
	}

	fmt.Println()
	fmt.Printf("Backup Summary\n")
	fmt.Printf(" files:\n")
	fmt.Printf("        total: %d\n", total)
	fmt.Printf("   unmodified: %d\n", count_ok)
	fmt.Printf("          new: %d\n", count_new)
	fmt.Printf("     modified: %d\n", count_modified)
	fmt.Printf("    not found: %d\n", count_notfound)
	fmt.Printf(" actions:\n")
	fmt.Printf("    no action: %d\n", count_noaction)
	fmt.Printf("     uploaded: %d (%s bytes)\n", count_uploaded, humanize.Comma(bytes_uploaded))
	fmt.Printf("       failed: %d\n", count_failed)
	fmt.Println()

	return nil
}
