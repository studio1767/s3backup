package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/studio1767/s3backup/internal/job"
	"github.com/studio1767/s3backup/internal/s3io"
)

func main() {
	// process the command line
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s  [-p <profile>] [-s secrets-file] <bucket> <jobname> <jobfile>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	profile := flag.String("p", "default", "aws s3 credentials profile")
	secrets_file := flag.String("s", "default", "yaml file containing secret passphrases to encrypt the job")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Fprintf(os.Stderr, "Error: incorrect arguments provided\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	jobname := flag.Arg(1)
	jobfile := flag.Arg(2)

	// create the client
	client, err := s3io.NewClient(*profile, bucket, "default", *secrets_file)
	if err != nil {
		log.Fatal(err)
	}

	// upload the jobfile
	key, err := upload(client, jobname, jobfile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("uploaded to %s\n", key)
}

func upload(client s3io.Client, jobname, jobfile string) (string, error) {

	// open the file
	source, err := os.Open(jobfile)
	if err != nil {
		return "", err
	}
	defer source.Close()

	// download the file
	key, err := job.Upload(client, source, jobname)
	if err != nil {
		return "", err
	}

	return key, nil
}
