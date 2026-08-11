// Package source defines origin objects consumed by the cache.
package source

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	// ErrNotFound reports that the requested source object does not exist.
	ErrNotFound = errors.New("source object not found")
	// ErrRangeNotSatisfiable reports that the requested byte range is outside the object.
	ErrRangeNotSatisfiable = errors.New("source range not satisfiable")
	// ErrRangeUnsupported reports that a source cannot provide ranged reads.
	ErrRangeUnsupported = errors.New("source does not support ranged reads")
	// ErrObjectChanged reports that the source changed after metadata was obtained.
	ErrObjectChanged = errors.New("source object changed")
)

// Metadata is the immutable information needed to validate cached bytes.
type Metadata struct {
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// Object is one immutable origin object. Implementations must return exactly
// the requested half-open range [start, end) from OpenRange or an error.
// Metadata validators should be used to prevent bytes from different object
// versions from being mixed.
type Object interface {
	Metadata() Metadata
	OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error)
}

// Source resolves object keys. It is used by integrations such as HTTP
// proxies; the cache itself accepts an Object directly.
type Source interface {
	Open(ctx context.Context, key string) (Object, error)
}
