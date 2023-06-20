package s3io_test

import (
	"bytes"
	"crypto/rand"

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

	var dsize int64 = 1024
	data := make([]byte, dsize)
	size, err := wc.Write(data)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(size))
	require.Equal(t, 1, wc.TotalWrites())
	require.Equal(t, dsize, wc.TotalBytes())
}

func TestFiveWritesCountsFive(t *testing.T) {
	wc := s3io.NewWriteCounter(new(bytes.Buffer))
	defer wc.Close()

	var dsize int64 = 1024
	data := make([]byte, dsize)
	for i := 0; i < 5; i++ {
		size, err := wc.Write(data)
		require.NoError(t, err)
		require.Equal(t, dsize, int64(size))
	}

	require.Equal(t, 5, wc.TotalWrites())
	require.Equal(t, 5*dsize, wc.TotalBytes())
}

func TestWrittenDataMatches(t *testing.T) {
	wbuffer := bytes.NewBuffer(nil)
	wc := s3io.NewWriteCounter(wbuffer)
	defer wc.Close()

	var dsize int64 = 1024
	data := make([]byte, dsize)
	rndsize, err := rand.Read(data)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(rndsize))

	size, err := wc.Write(data)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(size))
	require.Equal(t, 1, wc.TotalWrites())
	require.Equal(t, dsize, wc.TotalBytes())
	require.Equal(t, dsize, int64(wbuffer.Len()))
	require.Zero(t, bytes.Compare(data, wbuffer.Bytes()))
}

func TestReadCounterIsZeroWhenCreated(t *testing.T) {
	rc := s3io.NewReadCounter(new(bytes.Buffer))
	defer rc.Close()

	require.Equal(t, 0, rc.TotalReads())
	require.Equal(t, int64(0), rc.TotalBytes())
}

func TestOneReadCountsOne(t *testing.T) {
	var dsize int64 = 1024

	srcData := make([]byte, dsize)
	rc := s3io.NewReadCounter(bytes.NewBuffer(srcData))
	defer rc.Close()

	dstData := make([]byte, dsize)
	size, err := rc.Read(dstData)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(size))
	require.Equal(t, 1, rc.TotalReads())
	require.Equal(t, dsize, rc.TotalBytes())
}

func TestFiveReadsCountsFive(t *testing.T) {
	var dsize int64 = 1024

	srcData := make([]byte, dsize*5)
	rc := s3io.NewReadCounter(bytes.NewBuffer(srcData))
	defer rc.Close()

	dstData := make([]byte, dsize)

	for i := 0; i < 5; i++ {
		size, err := rc.Read(dstData)
		require.NoError(t, err)
		require.Equal(t, dsize, int64(size))
	}

	require.Equal(t, 5, rc.TotalReads())
	require.Equal(t, 5*dsize, rc.TotalBytes())
}

func TestReadDataMatches(t *testing.T) {
	var dsize int64 = 1024

	srcData := make([]byte, dsize)
	rndsize, err := rand.Read(srcData)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(rndsize))

	rc := s3io.NewReadCounter(bytes.NewBuffer(srcData))
	defer rc.Close()

	dstData := make([]byte, dsize)
	size, err := rc.Read(dstData)

	require.NoError(t, err)
	require.Equal(t, dsize, int64(size))
	require.Equal(t, 1, rc.TotalReads())
	require.Equal(t, dsize, rc.TotalBytes())
	require.Zero(t, bytes.Compare(srcData, dstData))
}
