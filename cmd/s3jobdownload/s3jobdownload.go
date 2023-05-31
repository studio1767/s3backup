package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/studio1767/s3backup/internal/s3io"
)

func main() {
	// process the command line
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s  [-p <profile>] [-s secrets-file] <bucket> <jobname>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	profile := flag.String("p", "default", "aws s3 credentials profile")
	secrets_file := flag.String("s", "default", "yaml file containing secret passphrases to decrypt the job")
	flag.Parse()

	if flag.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "Error: incorrect arguments provided\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	jobname := flag.Arg(1)

	// create the client
	client, err := s3io.NewClient(*profile, bucket, "default", *secrets_file)
	if err != nil {
		log.Fatal(err)
	}

	// run the restore for the manifest
	path, err := download(client, jobname)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("downloaded to %s\n", path)
}

func download(client s3io.Client, jobname string) (string, error) {
	// create the key prefix for the job
	prefix := fmt.Sprintf("jobs/%s/", jobname)

	// get the latest job config
	key, _, err := client.LatestMatching(prefix)
	if err != nil {
		return "", err
	}

	// get the filename from the key
	ktokens := strings.Split(key, "/")
	fname := ktokens[len(ktokens)-1]

	// create a local file to save to
	sink, err := os.Create(fname)
	if err != nil {
		return "", err
	}
	defer sink.Close()

	// download to the file
	_, err = client.Download(key, sink)
	if err != nil {
		os.Remove(fname)
		return "", fmt.Errorf("Download failed: %w", err)
	}

	return fname, nil
}
