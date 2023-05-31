package manifest_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/studio1767/s3backup/internal/job"
	"github.com/studio1767/s3backup/internal/manifest"
	"github.com/studio1767/s3backup/internal/s3io"
)

func TestManifest(t *testing.T) {
	// basic setup to get the client
	profile := os.Getenv("S3BU_TEST_PROFILE")
	require.NotEmpty(t, profile, "missing environment variable S3BU_TEST_PROFILE")

	bucket := os.Getenv("S3BU_TEST_BUCKET")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_BUCKET")

	jobname := os.Getenv("S3BU_TEST_JOBNAME")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_JOBNAME")

	// create the client
	client, err := s3io.NewClient(profile, bucket, "default", "default")
	require.NoError(t, err)

	// download the job so we can extract the sources
	job, jobkey, err := job.Download(client, jobname)
	require.NoError(t, err)

	fmt.Printf("job: %s/%s\n", bucket, jobkey)

	// process the manifests
	for _, source := range job.Sources {
		fmt.Printf("label: %s\n", source.Label)

		// download the manifest
		mreader, mkey, err := manifest.Download(client, jobname, source.Label)
		if err != nil {
			var nomanifest *manifest.ErrNoSuchManifest
			if errors.As(err, &nomanifest) {
				fmt.Printf("manifest: not found\n")
				continue
			}
		}
		require.NoError(t, err)
		defer mreader.Close()

		fmt.Printf("manifest: %s/%s\n", bucket, mkey)

		scanner := bufio.NewScanner(mreader)
		lines := 0
		for scanner.Scan() {
			lines++
			scanner.Text()
		}
		fmt.Println(lines)

		// upload the manifest
		mreader.Seek(0, io.SeekStart)

		mkey, err = manifest.Upload(client, mreader, jobname, source.Label)
		require.NoError(t, err)

		fmt.Printf("new manifest: %s\n", mkey)
	}
}
