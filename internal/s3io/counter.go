package s3io

import (
	"io"
)

type ReadCounter interface {
	Read(p []byte) (int, error)
	Close() error

	TotalReads() int
	TotalBytes() int64
}

type WriteCounter interface {
	Write(p []byte) (int, error)
	Close() error

	TotalWrites() int
	TotalBytes() int64
}

func NewReadCounter(in io.Reader) ReadCounter {
	rc := readCounter{
		in:    in,
		reads: 0,
		bytes: 0,
	}
	return &rc
}

func NewWriteCounter(out io.Writer) WriteCounter {
	wc := writeCounter{
		out:    out,
		writes: 0,
		bytes:  0,
	}
	return &wc
}

type readCounter struct {
	in    io.Reader
	reads int
	bytes int64
}

func (rc *readCounter) Read(p []byte) (int, error) {
	size, err := rc.in.Read(p)

	rc.reads += 1
	rc.bytes += int64(size)

	return size, err
}

func (rc *readCounter) Close() error {
	rc.in = nil
	return nil
}

func (rc *readCounter) TotalReads() int {
	return rc.reads
}

func (rc *readCounter) TotalBytes() int64 {
	return rc.bytes
}

type writeCounter struct {
	out    io.Writer
	writes int
	bytes  int64
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	size, err := wc.out.Write(p)

	wc.writes += 1
	wc.bytes += int64(size)

	return size, err
}

func (wc *writeCounter) Close() error {
	wc.out = nil
	return nil
}

func (wc *writeCounter) TotalWrites() int {
	return wc.writes
}

func (wc *writeCounter) TotalBytes() int64 {
	return wc.bytes
}
