package ops

import (
	"context"
	"strings"
)

// Filters out files from the stream based on their file extension.
// If 'include' is true, files that don't match are dropped from the
// stream; if 'include' is false, files that do match are dropped from
// the stream.
// The matching is case-insensitive and extensions start with a '.'.

func NewFileExtensionFilter(ctx context.Context, in <-chan *EntryInfo, extensions []string, include bool) <-chan *EntryInfo {

	// convert extensions into map and make sure they start with a '.'
	var ext []string
	for _, extension := range extensions {
		if len(extension) == 0 {
			continue
		}
		if !strings.HasPrefix(extension, ".") {
			extension = "." + extension
		}
		ext = append(ext, extension)
	}

	out := make(chan *EntryInfo, 10)
	filter := fileExtensionFilter{
		ctx:        ctx,
		in:         in,
		out:        out,
		extensions: ext,
		include:    include,
	}
	go filter.run()

	return out
}

type fileExtensionFilter struct {
	ctx        context.Context
	in         <-chan *EntryInfo
	out        chan<- *EntryInfo
	extensions []string
	include    bool
}

func (filter *fileExtensionFilter) run() {
	defer close(filter.out)

	for {
		// check the channels
		select {
		case <-filter.ctx.Done():
			return
		case info, ok := <-filter.in:
			if !ok {
				return
			}
			filter.process(info)
		}
	}
}

func (filter *fileExtensionFilter) process(info *EntryInfo) {
	// pass on failure information
	if info.Action == Failed {
		filter.out <- info
		return
	}

	// see if this file has one of our matching extensions
	ext_match := false
	for _, ext := range filter.extensions {
		if strings.HasSuffix(info.RelPath, ext) {
			ext_match = true
			break
		}
	}

	// case 1: matching for include
	//    include if filter.include == true && ext_match == true
	//    ... or if ext_match == filter.include
	// case 2: matching for exclude
	//    if filter.include == false && ext_match == false, then we include
	//    ... or if ext_match == filter.include
	if ext_match == filter.include {
		filter.out <- info
	}
}
