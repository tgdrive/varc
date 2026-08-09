package objectio

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	errRetryReaderClosed = errors.New("file already closed")
	errTooManyRetryOpens = errors.New("failed to reopen: too many retries")
)

type noLowLevelRetrier interface {
	NoLowLevelRetry() bool
}

// RetryReader reopens an object at the exact consumed offset when a read
// fails, up to the low-level retry count attached to the context.
type RetryReader struct {
	ctx      context.Context
	mu       sync.Mutex
	src      Object
	baseSpan *Span
	rc       io.ReadCloser
	size     int64
	start    int64
	end      int64 // exclusive; negative means unknown/unbounded
	offset   int64 // relative to start
	maxTries int
	tries    int
	err      error
	opened   bool
}

// OpenRetrying opens src and returns a reader which resumes a failed stream at
// the exact byte already consumed. The initial open counts as the first try.
func OpenRetrying(ctx context.Context, src Object, span *Span) (*RetryReader, error) {
	h := &RetryReader{
		ctx:      ctx,
		src:      src,
		size:     src.Size(),
		start:    0,
		end:      src.Size(),
		maxTries: GetConfig(ctx).LowLevelRetries,
	}
	if span != nil {
		copySpan := *span
		h.baseSpan = &copySpan
		h.start = copySpan.Start
		if copySpan.End >= 0 {
			h.end = copySpan.End + 1
		}
	}
	if err := h.open(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *RetryReader) spanForOpen() *Span {
	if h.offset == 0 {
		if h.baseSpan == nil {
			return nil
		}
		span := *h.baseSpan
		return &span
	}
	end := int64(-1)
	if h.end >= 0 {
		end = h.end - 1
	}
	return &Span{Start: h.start + h.offset, End: end}
}

// open opens the current position. The caller must hold h.mu when concurrent
// access is possible.
func (h *RetryReader) open() error {
	h.tries++
	if h.tries > h.maxTries {
		h.err = errTooManyRetryOpens
		return h.err
	}
	h.rc, h.err = h.src.Open(h.ctx, h.spanForOpen())
	if h.err != nil {
		return h.err
	}
	h.opened = true
	return nil
}

func (h *RetryReader) reopen() error {
	if h.opened {
		h.opened = false
		_ = h.rc.Close()
	}
	return h.open()
}

func isNoLowLevelRetryError(err error) bool {
	var marker noLowLevelRetrier
	return errors.As(err, &marker) && marker.NoLowLevelRetry()
}

// Read fills p unless it reaches EOF or a non-recoverable error. Transient
// read failures reopen the same range at the number of bytes already consumed.
func (h *RetryReader) Read(p []byte) (n int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return 0, h.err
	}

	var nn int
	for n < len(p) && err == nil {
		nn, err = h.rc.Read(p[n:])
		n += nn
		h.offset += int64(nn)
		if err != nil && err != io.EOF {
			h.err = err
			if !isNoLowLevelRetryError(err) {
				if h.reopen() == nil {
					err = nil
				}
			}
		}
	}
	return n, err
}

func (h *RetryReader) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.opened {
		return errRetryReaderClosed
	}
	h.opened = false
	h.err = errRetryReaderClosed
	return h.rc.Close()
}

var _ io.ReadCloser = (*RetryReader)(nil)
