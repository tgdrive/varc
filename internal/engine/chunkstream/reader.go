// Package chunkstream provides sequential and parallel ranged readers.
package chunkstream

import (
	"context"
	"errors"
	"io"

	"github.com/tgdrive/varc/internal/engine/objectio"
)

var (
	ErrorFileClosed  = errors.New("file already closed")
	ErrorInvalidSeek = errors.New("invalid seek position")
)

type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	objectio.RangeSeeker
	Open() (Reader, error)
}

// New returns a ranged Reader for o.
// An initialChunkSize of <= 0 disables chunked reading. If maxChunkSize is
// greater than initialChunkSize, sequential chunks double up to maxChunkSize.
func New(ctx context.Context, o objectio.Object, initialChunkSize int64, maxChunkSize int64, streams int) Reader {
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
