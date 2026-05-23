package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/tgdrive/varc/internal/chunkedreader"
)

// ReadFileHandle represents a file handle for reading
type ReadFileHandle struct {
	*baseHandle
	ctx        context.Context
	mu         sync.Mutex
	closed     bool
	f          *File
	offset     int64
	size       int64
	chunkedReader chunkedreader.ChunkedReader
}

// newReadFileHandle creates a new read file handle
func newReadFileHandle(f *File) (*ReadFileHandle, error) {
	h := &ReadFileHandle{
		baseHandle: &baseHandle{},
		ctx:        f.ctx,
		f:          f,
		size:       f.Size(),
		offset:     0,
	}

	// Try to open via cache first
	cachePath := f.Path()
	item := f.d.engine.cache.Item(cachePath)
	if item == nil {
		return nil, fmt.Errorf("failed to get cache item for %s", cachePath)
	}

	// If there is no cached data on disk and no remote to fill it, fail fast.
	// This handles the case where the cache was explicitly purged (e.g. via
	// PURGE/Remove) but the file tree node still exists from a previous open.
	if !item.Exists() && f.remote == nil {
		return nil, fmt.Errorf("cache read: no cached data for %s", cachePath)
	}

	// Open the cache item (creates the file descriptor). If a remote
	// object is available, it will be used to fill cache misses; otherwise
	// the file serves only cached data.
	if err := item.Open(f.remote); err != nil {
		return nil, fmt.Errorf("cache read: failed to open cache item: %w", err)
	}
	if f.remote != nil {
		h.size = f.remote.Size()
	}

	// Get size from cache item (overrides -1 from remote if unknown)
	if sz, err := item.GetSize(); err == nil && sz >= 0 {
		h.size = sz
	}

	return h, nil
}

// String returns the file name
func (fh *ReadFileHandle) String() string {
	return fh.f.name
}

// Node returns the underlying File
func (fh *ReadFileHandle) Node() Node {
	return fh.f
}

// Size returns the file size
func (fh *ReadFileHandle) Size() int64 {
	return fh.size
}

// Read reads up to len(p) bytes into p
func (fh *ReadFileHandle) Read(p []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closed {
		return 0, os.ErrClosed
	}

	n, err = fh.readAt(p, fh.offset)
	if err != nil && err != io.EOF {
		return n, err
	}
	fh.offset += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off
func (fh *ReadFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	return fh.readAt(p, off)
}

// readAt reads from the cache item at the given offset
func (fh *ReadFileHandle) readAt(p []byte, off int64) (n int, err error) {
	item := fh.f.d.engine.cache.Item(fh.f.Path())
	if item != nil {
		return item.ReadAt(p, off)
	}
	return 0, io.EOF
}

// Seek sets the offset for the next Read
func (fh *ReadFileHandle) Seek(offset int64, whence int) (int64, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		fh.offset = offset
	case io.SeekEnd:
		fh.offset = fh.size + offset
	case io.SeekCurrent:
		fh.offset += offset
	}

	return fh.offset, nil
}

// Close closes the file handle
func (fh *ReadFileHandle) Close() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closed {
		return os.ErrClosed
	}
	fh.closed = true
	return nil
}

// Flush flushes the file - no-op for read-only
func (fh *ReadFileHandle) Flush() error {
	return nil
}

// Release releases the file handle
func (fh *ReadFileHandle) Release() error {
	return fh.Close()
}

// Stat returns file info
func (fh *ReadFileHandle) Stat() (os.FileInfo, error) {
	return fh.f, nil
}

// ModTime returns the modification time
func (fh *ReadFileHandle) ModTime() time.Time {
	return fh.f.ModTime()
}

// Name returns the file name
func (fh *ReadFileHandle) Name() string {
	return fh.f.Name()
}

// Write is not supported for read handles
func (fh *ReadFileHandle) Write(p []byte) (n int, err error) {
	return 0, EPERM
}

// WriteAt is not supported for read handles
func (fh *ReadFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, EPERM
}

// Truncate is not supported for read handles
func (fh *ReadFileHandle) Truncate(size int64) error {
	return EPERM
}

// WriteString is not supported for read handles
func (fh *ReadFileHandle) WriteString(s string) (n int, err error) {
	return 0, EPERM
}
