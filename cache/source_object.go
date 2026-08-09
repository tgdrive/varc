package cache

import (
	"context"
	"io"
	"sync"

	"varc/internal/engine/objectio"
	"varc/source"
)

// sourceObject adapts Source to the narrow object contract used by the cache engine.
type sourceObject struct {
	src source.Source
	key string

	mu      sync.RWMutex
	meta    source.Metadata
	headers map[string][]string
}

func (o *sourceObject) Size() int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.meta.Size
}

func (o *sourceObject) updateRequest(meta source.Metadata, headers map[string][]string) {
	o.mu.Lock()
	o.meta = meta
	o.headers = cloneHeaderMap(headers)
	o.mu.Unlock()
}

func (o *sourceObject) snapshotRequest() (source.Metadata, map[string][]string) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.meta, cloneHeaderMap(o.headers)
}

func (o *sourceObject) Open(ctx context.Context, span *objectio.Span) (io.ReadCloser, error) {
	meta, headers := o.snapshotRequest()
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
	ctx = source.WithRequestHeaders(ctx, headers)
	return o.src.OpenRange(ctx, o.key, start, end, meta)
}

func (o *sourceObject) String() string { return o.key }

func cloneHeaderMap(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

var _ objectio.Object = (*sourceObject)(nil)
