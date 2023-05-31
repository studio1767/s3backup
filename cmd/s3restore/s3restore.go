package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"

	humanize "github.com/dustin/go-humanize"

	"github.com/studio1767/s3backup/internal/manifest"
	"github.com/studio1767/s3backup/internal/ops"
	"github.com/studio1767/s3backup/internal/s3io"
)

func main() {
	// process the command line
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s  [-p <profile>] [-c] [-f] [-o] [-s secrets-file] [-i identities-file] <bucket> <manifest-key> <restore-root> [<pattern>]\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	profile := flag.String("p", "default", "aws s3 credentials profile")
	check_mode := flag.Bool("c", false, "run in check mode")
	force := flag.Bool("f", false, "force download even if destination not empty")
	overwrite := flag.Bool("o", false, "overwrite any existing files")
	secrets_file := flag.String("s", "default", "yaml file containing secret passphrases to decrypt the manifests")
	identities_file := flag.String("i", "default", "file containing identities to decrypt data")
	flag.Parse()

	if flag.NArg() != 3 && flag.NArg() != 4 {
		fmt.Fprintf(os.Stderr, "Error: incorrect arguments provided\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	manifest_key := flag.Arg(1)
	restore_root := flag.Arg(2)

	pattern := ".*"
	if flag.NArg() == 4 {
		pattern = flag.Arg(3)
	}

	// create the client
	client, err := s3io.NewClient(*profile, bucket, *identities_file, *secrets_file)
	if err != nil {
		log.Fatal(err)
	}

	if client.HasIdentities() == false {
		log.Fatal(&s3io.ErrIdentitiesNotFound{})
	}

	// run some sanity checks on the restore root
	st, err := os.Stat(restore_root)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(restore_root, 0755)
			if err != nil {
				log.Fatalf("failed to create restore root: %s", err)
			}
		} else {
			log.Fatalf("failed to stat restore root: %s", err)
		}

	} else {
		if st.IsDir() == false {
			log.Fatal("the restore root is not a directory")
		}
	}

	// make sure the restore root is empty ...
	//   ...not if we're only checking
	//   ...not if we're forcing the download
	if *check_mode == false && *force == false {
		entries, err := os.ReadDir(restore_root)
		if err != nil {
			log.Fatalf("failed to read restore root: %s", err)
		}
		if len(entries) != 0 {
			log.Fatal("restore root is not empty; use -f to force restore")
		}
	}

	// run the restore for the manifest
	err = restore_manifest(client, manifest_key, pattern, restore_root, *check_mode, *overwrite)
	if err != nil {
		log.Fatal(err)
	}
}

func restore_manifest(client s3io.Client, mkey string, pattern string, restore_root string, check_mode bool, overwrite bool) error {
	// download the manifest file
	mreader, err := manifest.DownloadWithKey(client, mkey)
	if err != nil {
		return err
	}
	defer mreader.Close()
	defer os.Remove(mreader.Name())

	fmt.Printf("Processing %s\n", mkey)

	// create the pattern matcher
	regex := regexp.MustCompile(pattern)

	// start the scanner
	ch := ops.NewManifestScanner(context.Background(), mreader)

	// process the scanned results
	num_total := 0
	var total_bytes int64 = 0
	num_fails := 0
	var fail_bytes int64 = 0
	num_skipped := 0
	var skip_bytes int64 = 0

	for info := range ch {
		matches := regex.FindStringSubmatch(info.RelPath)
		if matches == nil {
			continue
		}

		num_total += 1
		total_bytes += info.RawSize

		if check_mode == true {
			fmt.Printf("- found: %s (%s bytes)\n", info.RelPath, humanize.Comma(info.RawSize))
			continue
		}

		// check the download file
		fpath := filepath.Join(restore_root, info.RelPath)
		if overwrite == false {
			_, err := os.Stat(fpath)
			if err == nil {
				fmt.Printf("-    skipping: %s (%s bytes)\n", info.RelPath, humanize.Comma(info.RawSize))
				num_skipped += 1
				skip_bytes += info.RawSize
				continue
			}
		}

		fmt.Printf("- downloading: %s (%s bytes)\n", info.RelPath, humanize.Comma(info.RawSize))
		_, err := restore_file(client, info, fpath)
		if err != nil {
			num_fails += 1
			fail_bytes += info.RawSize

			fmt.Printf(" - failed: %s", err)
		}
	}

	fmt.Println()
	fmt.Printf("Restore Summary\n")
	fmt.Printf("-   total files: %d\n", num_total)
	fmt.Printf("-   total bytes: %s\n", humanize.Comma(total_bytes))
	fmt.Printf("- success files: %d\n", num_total-num_skipped-num_fails)
	fmt.Printf("- success bytes: %s\n", humanize.Comma(total_bytes-skip_bytes-fail_bytes))
	fmt.Printf("- skipped files: %d\n", num_skipped)
	fmt.Printf("- skipped bytes: %s\n", humanize.Comma(skip_bytes))
	fmt.Printf("-  failed files: %d\n", num_fails)
	fmt.Printf("-  failed bytes: %s\n", humanize.Comma(fail_bytes))
	fmt.Println()

	return nil
}

func restore_file(client s3io.Client, info *ops.EntryInfo, fpath string) (int64, error) {
	// construct the key from the hash
	key := fmt.Sprintf("data/%s/%s", info.Hash[:4], info.Hash)

	// create the directories to the file
	fdir := filepath.Dir(fpath)
	err := os.MkdirAll(fdir, 0755)
	if err != nil {
		return 0, err
	}

	// create the file
	sink, err := os.Create(fpath)
	if err != nil {
		return 0, err
	}
	defer sink.Close()

	// download to the file
	size, err := client.Download(key, sink)
	if err != nil {
		os.Remove(fpath)
		return 0, err
	}

	// set the file mode to match
	err = os.Chmod(fpath, info.Mode)
	if err != nil {
		return 0, err
	}

	return size, nil
}
