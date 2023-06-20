package s3io_test

import (
	"bytes"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/studio1767/s3backup/internal/s3io"
)

func TestWriteCounterIsZeroWhenCreated(t *testing.T) {
	wc := s3io.NewWriteCounter(new(bytes.Buffer))
	defer wc.Close()

	require.Equal(t, 0, wc.TotalWrites())
	require.Equal(t, int64(0), wc.TotalBytes())
}

func TestOneWriteCountsOne(t *testing.T) {
	wc := s3io.NewWriteCounter(new(bytes.Buffer))
	defer wc.Close()

	var bsize int64 = 1024
	data := make([]byte, bsize)
	size, err := wc.Write(data)

	require.NoError(t, err)
	require.Equal(t, bsize, int64(size))
	require.Equal(t, 1, wc.TotalWrites())
	require.Equal(t, bsize, wc.TotalBytes())
}

func TestFiveWritesCountsFive(t *testing.T) {
	wc := s3io.NewWriteCounter(new(bytes.Buffer))
	defer wc.Close()

	var bsize int64 = 1024
	data := make([]byte, bsize)
	for i := 0; i < 5; i++ {
		size, err := wc.Write(data)
		require.NoError(t, err)
		require.Equal(t, bsize, int64(size))
	}

	require.Equal(t, 5, wc.TotalWrites())
	require.Equal(t, 5*bsize, wc.TotalBytes())
}

func TestReadCounterIsZeroWhenCreated(t *testing.T) {
	rc := s3io.NewReadCounter(new(bytes.Buffer))
	defer rc.Close()

	require.Equal(t, 0, rc.TotalReads())
	require.Equal(t, int64(0), rc.TotalBytes())
}

func TestOneReadCountsOne(t *testing.T) {
	srcData := make([]byte, 8192)
	rc := s3io.NewReadCounter(bytes.NewBuffer(srcData))
	defer rc.Close()

	var bsize int64 = 1024
	dstData := make([]byte, bsize)
	size, err := rc.Read(dstData)

	require.NoError(t, err)
	require.Equal(t, bsize, int64(size))
	require.Equal(t, 1, rc.TotalReads())
	require.Equal(t, bsize, rc.TotalBytes())
}

func TestFiveReadsCountsFive(t *testing.T) {
	srcData := make([]byte, 8192)
	rc := s3io.NewReadCounter(bytes.NewBuffer(srcData))
	defer rc.Close()

	var bsize int64 = 1024
	dstData := make([]byte, bsize)

	for i := 0; i < 5; i++ {
		size, err := rc.Read(dstData)
		require.NoError(t, err)
		require.Equal(t, bsize, int64(size))
	}

	require.Equal(t, 5, rc.TotalReads())
	require.Equal(t, 5*bsize, rc.TotalBytes())
}
