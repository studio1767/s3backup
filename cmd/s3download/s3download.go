package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	humanize "github.com/dustin/go-humanize"

	"github.com/studio1767/s3backup/internal/s3io"
)

func main() {
	// process the command line
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s  [-p <profile>] [-o] [-s secrets-file] [-i identities-file] <bucket> <key> <restore_root>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	profile := flag.String("p", "default", "aws s3 credentials profile")
	secrets_file := flag.String("s", "default", "yaml file containing secret passphrases to decrypt metadata files")
	identities_file := flag.String("i", "default", "file containing identities to decrypt data files")
	overwrite := flag.Bool("o", false, "overwrite any existing files")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Fprintf(os.Stderr, "Error: incorrect arguments provided\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	key := flag.Arg(1)
	restore_root := flag.Arg(2)

	// create the client
	client, err := s3io.NewClient(*profile, bucket, *identities_file, *secrets_file)
	if err != nil {
		log.Fatal(err)
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

	// run the restore for the manifest
	err = download(client, key, restore_root, *overwrite)
	if err != nil {
		log.Fatal(err)
	}
}

func download(client s3io.Client, key, restore_root string, overwrite bool) error {

	fmt.Printf("Processing %s\n", key)

	// check the download file
	tokens := strings.Split(key, "/")
	fname := tokens[len(tokens)-1]
	fpath := filepath.Join(restore_root, fname)

	fmt.Printf("- downloading to %s\n", fpath)

	if overwrite == false {
		_, err := os.Stat(fpath)
		if err == nil {
			fmt.Printf("- unable to download: file already exists\n")
			return nil
		}
	}

	// create the file
	sink, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("Failed to create file: %w", err)
	}
	defer sink.Close()

	// download the file
	size, err := client.Download(key, sink)
	if err != nil {
		os.Remove(fpath)
		return fmt.Errorf("Download failed: %w", err)
	}

	fmt.Printf("- success: %s (%s)\n", fpath, humanize.Comma(size))

	return nil
}
