package ops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// This operator will generate the content hash for the file and insert it into the
// ItemInfo object.
func NewHashGenerator(ctx context.Context, in <-chan *EntryInfo, root string) <-chan *EntryInfo {
	out := make(chan *EntryInfo, 10)
	hg := hashGenerator{
		ctx:  ctx,
		in:   in,
		out:  out,
		root: root,
	}
	go hg.run()

	return out
}

type hashGenerator struct {
	ctx  context.Context
	in   <-chan *EntryInfo
	out  chan<- *EntryInfo
	root string
}

func (hg *hashGenerator) run() {
	defer close(hg.out)

	for {
		// check the channels
		select {
		case <-hg.ctx.Done():
			return
		case info, ok := <-hg.in:
			if !ok {
				return
			}
			hg.process(info)
		}
	}
}

func (hg *hashGenerator) process(info *EntryInfo) {
	// check the status first
	if info.Action == Failed {
		hg.out <- info
		return
	}

	// only re-generate the hash if we need to
	if info.Status == StatusNew || info.Status == StatusModified || len(info.Hash) == 0 {
		// full path to the file
		fpath := filepath.Join(hg.root, info.RelPath)

		// open for reading
		in, err := os.Open(fpath)
		if err != nil {
			info.Action = Failed
			info.ActionMessage = fmt.Sprintf("failed to open %s", fpath)
			hg.out <- info
			return
		}
		defer in.Close()

		// generate the hash
		h := sha256.New()
		_, err = io.Copy(h, in)
		if err != nil {
			info.Action = Failed
			info.ActionMessage = fmt.Sprintf("failed to generate hash for %s", fpath)
		} else {
			info.Hash = hex.EncodeToString(h.Sum(nil))
		}
	}

	// pass it on
	hg.out <- info
}
