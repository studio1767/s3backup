package ops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/studio1767/s3backup/internal/s3io"
)

// This operator uploads files to amazon S3. It uses the 'State' field of the
// ItemInfo structure to guide it's decision making.
// If the state is Changed or NewOrMoved, it runs the upload code. Note that
// as an additional check, the content hash is generated before any upload, and
// if the key exists in S3, no upload happens since the content is already there.
func NewUploader(ctx context.Context, in <-chan *EntryInfo, client s3io.Client, root string, compress bool) <-chan *EntryInfo {
	out := make(chan *EntryInfo, 10)
	ul := uploader{
		ctx:      ctx,
		in:       in,
		out:      out,
		client:   client,
		root:     root,
		compress: compress,
	}
	go ul.run()

	return out
}

type uploader struct {
	ctx      context.Context
	in       <-chan *EntryInfo
	out      chan<- *EntryInfo
	client   s3io.Client
	root     string
	compress bool
}

func (ul *uploader) run() {
	defer close(ul.out)

	for {
		// check the channels
		select {
		case <-ul.ctx.Done():
			return
		case info, ok := <-ul.in:
			if !ok {
				return
			}
			ul.process(info)
		}
	}
}

func (ul *uploader) process(info *EntryInfo) {
	// check the status first
	if info.Action == Failed {
		ul.out <- info
		return
	}

	if info.Status == StatusModified || info.Status == StatusNew {

		// generate the key and check if it already exists
		key := fmt.Sprintf("data/%s/%s", info.Hash[:4], info.Hash)
		if exists, _ := ul.client.Exists(key); exists {
			info.Action = NoAction
			ul.out <- info
			return
		}

		// open the file for reading
		fpath := filepath.Join(ul.root, info.RelPath)
		file, err := os.Open(fpath)
		if err != nil {
			info.Action = Failed
			info.ActionMessage = fmt.Sprintf("failed to open %s", fpath)
			ul.out <- info
			return
		}
		defer file.Close()

		// try and upload
		nbytes, err := ul.client.UploadEncrypted(key, file, ul.compress)
		if err != nil {
			info.Action = Failed
			info.ActionMessage = fmt.Sprintf("failed to upload %s", info.RelPath)
		} else {
			info.Action = Uploaded
			info.UploadedSize = nbytes
		}
	}

	ul.out <- info
}
