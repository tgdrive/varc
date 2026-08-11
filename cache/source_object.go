package cache

import (
	"context"
	"io"
	"sync"

	"github.com/tgdrive/varc/internal/engine/objectio"
	"github.com/tgdrive/varc/source"
)

// sourceObject adapts an origin object to the cache engine's range contract.
type sourceObject struct {
	mu     sync.RWMutex
	origin source.Object
	meta   source.Metadata
}

func (o *sourceObject) Size() int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.meta.Size
}

func (o *sourceObject) update(origin source.Object, meta source.Metadata) {
	o.mu.Lock()
	o.origin = origin
	o.meta = meta
	o.mu.Unlock()
}

func (o *sourceObject) snapshot() (source.Object, source.Metadata) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.origin, o.meta
}

func (o *sourceObject) Open(ctx context.Context, span *objectio.Span) (io.ReadCloser, error) {
	origin, meta := o.snapshot()
	start := int64(0)
	end := meta.Size
	if span != nil {
		start = span.Start
		if span.End >= 0 {
			end = span.End + 1
		}
	}
	if start < 0 || start >= meta.Size {
		return nil, source.ErrRangeNotSatisfiable
	}
	if end > meta.Size {
		end = meta.Size
	}
	if end <= start {
		return nil, source.ErrRangeNotSatisfiable
	}
	return origin.OpenRange(ctx, start, end)
}

func (o *sourceObject) String() string {
	return "origin object"
}

var _ objectio.Object = (*sourceObject)(nil)
