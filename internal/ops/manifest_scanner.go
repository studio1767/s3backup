package ops

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

func NewManifestScanner(ctx context.Context, mreader io.ReadCloser) <-chan *EntryInfo {

	out := make(chan *EntryInfo, 10)
	scanner := manifestScanner{
		ctx:    ctx,
		out:    out,
		reader: mreader,
	}
	go scanner.run()

	return out
}

type manifestScanner struct {
	ctx    context.Context
	out    chan<- *EntryInfo
	reader io.ReadCloser
}

func (ms *manifestScanner) run() {
	defer close(ms.out)

	scanner := bufio.NewScanner(ms.reader)
	for scanner.Scan() {
		// check for cancel
		select {
		case <-ms.ctx.Done():
			return
		default:
		}

		// split the line into tokens
		line := scanner.Text()
		tokens := strings.Split(line, ",")
		if len(tokens) != 5 {
			continue
		}

		// convert the tokens to the correct type
		var size int64
		var mtime int64
		var mode os.FileMode

		fmt.Sscanf(tokens[0], "%d", &size)
		fmt.Sscanf(tokens[1], "%d", &mtime)
		fmt.Sscanf(tokens[2], "%o", &mode)

		hash := tokens[3]
		path, err := url.PathUnescape(tokens[4])
		if err != nil {
			continue
		}

		// pass the message on
		ei := EntryInfo{
			Status:  StatusOk,
			RelPath: path,
			Hash:    hash,
			RawSize: size,
			ModTime: mtime,
			Mode:    mode,
			Action:  NoAction,
		}

		ms.out <- &ei
	}
}
