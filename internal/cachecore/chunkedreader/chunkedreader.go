// Package chunkedreader provides functionality for reading a stream in chunks.
package chunkedreader

import (
	"context"
	"errors"
	"io"

	"vfs-cache/internal/cachecore/fs"
)

var (
	ErrorFileClosed  = errors.New("file already closed")
	ErrorInvalidSeek = errors.New("invalid seek position")
)

type ChunkedReader interface {
	io.Reader
	io.Seeker
	io.Closer
	fs.RangeSeeker
	Open() (ChunkedReader, error)
}

// New returns a ChunkedReader for the Object.
// An initialChunkSize of <= 0 disables chunked reading. If maxChunkSize is
// greater than initialChunkSize, sequential chunks double up to maxChunkSize.
func New(ctx context.Context, o fs.Object, initialChunkSize int64, maxChunkSize int64, streams int) ChunkedReader {
	if initialChunkSize <= 0 {
		initialChunkSize = -1
	}
	if maxChunkSize != -1 && maxChunkSize < initialChunkSize {
		maxChunkSize = initialChunkSize
	}
	if streams < 0 {
		streams = 0
	}
	if streams <= 1 || o.Size() < 0 {
		return newSequential(ctx, o, initialChunkSize, maxChunkSize)
	}
	return newParallel(ctx, o, initialChunkSize, streams)
}
