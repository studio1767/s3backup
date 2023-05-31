package s3io_test

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"time"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/studio1767/s3backup/internal/s3io"
)

func TestUpDown(t *testing.T) {
	// basic setup to get the client
	profile := os.Getenv("S3BU_TEST_PROFILE")
	require.NotEmpty(t, profile, "missing environment variable S3BU_TEST_PROFILE")

	bucket := os.Getenv("S3BU_TEST_BUCKET")
	require.NotEmpty(t, bucket, "missing environment variable S3BU_TEST_BUCKET")

	client, err := s3io.NewClient(profile, bucket, "default", "default")
	require.NoError(t, err)

	// generate a prefix to test with
	now := time.Now()
	prefix := fmt.Sprintf("test-%s/", now.Format("20060102150405"))

	// create some data to upload
	num_buffers := 4
	buffers := make([][]byte, num_buffers)
	for i := 0; i < num_buffers; i++ {
		size := 5*1024*1024 + mrand.Int31n(5*1024*1024)
		buffer := make([]byte, size)
		_, err := crand.Read(buffer)
		require.NoError(t, err)

		fmt.Printf("buffer %d: %d bytes\n", i, size)

		buffers[i] = buffer
	}

	// upload the buffers
	for idx, buffer := range buffers {
		key := fmt.Sprintf("%s%09d", prefix, idx)
		ubuffer := bytes.NewReader(buffer)

		var size int64
		var err error

		switch idx % 4 {
		case 0:
			size, err = client.Upload(key, ubuffer)
			require.Equal(t, len(buffer), int(size))
		case 1:
			_, err = client.UploadCompressed(key, ubuffer)
		case 2:
			_, err = client.UploadEncrypted(key, ubuffer, true)
		default:
			_, err = client.UploadPassphrase(key, ubuffer, true)
		}

		require.NoError(t, err)
	}

	// download the buffers
	for idx, buffer := range buffers {
		key := fmt.Sprintf("%s%09d", prefix, idx)
		dbuffer := bytes.NewBuffer(nil)

		size, err := client.Download(key, dbuffer)

		require.NoError(t, err)
		require.Equal(t, len(buffer), int(size))
		require.Equal(t, len(buffer), dbuffer.Len(), fmt.Sprintf("iteration %d", idx))
		require.Equal(t, buffer, dbuffer.Bytes())
	}

	// the last file that was uploaded
	expected_key := fmt.Sprintf("%s%09d", prefix, len(buffers)-1)

	key, _, err := client.LatestMatching(prefix)
	require.NoError(t, err)
	require.Equal(t, expected_key, key)
}
