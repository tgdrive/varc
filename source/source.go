// Package source defines the remote object interface consumed by the cache.
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

// Source provides metadata and half-open ranged reads [start, end).
//
// Implementations must return exactly the requested range from OpenRange or an
// error. expected is the metadata previously returned by Stat and should be
// used to prevent mixing bytes from different object versions when possible.
type Source interface {
	Stat(ctx context.Context, key string) (Metadata, error)
	OpenRange(ctx context.Context, key string, start, end int64, expected Metadata) (io.ReadCloser, error)
}
