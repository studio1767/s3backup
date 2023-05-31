package job_test

import (
	"fmt"
	"os"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/studio1767/s3backup/internal/job"
	"github.com/studio1767/s3backup/internal/s3io"
)

func TestJob(t *testing.T) {
	// basic setup to get the client
	profile := os.Getenv("S3BU_TEST_PROFILE")
	require.NotEmpty(t, profile, "missing environment variable S3BU_TEST_PROFILE")

	bucket := os.Getenv("S3BU_TEST_BUCKET")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_BUCKET")

	jobname := os.Getenv("S3BU_TEST_JOBNAME")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_JOBNAME")

	client, err := s3io.NewClient(profile, bucket, "default", "default")
	require.NoError(t, err)

	job, jobkey, err := job.Download(client, jobname)
	require.NoError(t, err)

	fmt.Printf("job: %s/%s\n", bucket, jobkey)
	fmt.Println(job)
}
