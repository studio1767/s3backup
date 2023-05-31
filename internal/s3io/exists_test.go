package s3io_test

import (
	"fmt"
	"os"
	"time"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/studio1767/s3backup/internal/s3io"
)

func TestExists(t *testing.T) {
	// basic setup to get the client
	profile := os.Getenv("S3BU_TEST_PROFILE")
	require.NotEmpty(t, profile, "missing environment variable S3BU_TEST_PROFILE")

	bucket := os.Getenv("S3BU_TEST_BUCKET")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_BUCKET")

	client, err := s3io.NewClient(profile, bucket, "default", "default")
	require.NoError(t, err)

	// generate a key to test with
	now := time.Now()
	key := fmt.Sprintf("test-%s", now.Format("20060102150405"))

	exists, err := client.Exists(key)
	require.NoError(t, err)
	require.Equal(t, exists, false)
}
