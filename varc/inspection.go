package varc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// CacheDir returns the cache root directory.
func (c *Cache) CacheDir() string { return c.dir }

// ChunkSize returns the normalized chunk size.
func (c *Cache) ChunkSize() int64 { return c.chunkSize }

// KeyPath returns the data path for key without creating it.
func (c *Cache) KeyPath(key string) string {
	return filepath.Join(c.dir, ShardKey(key, c.shardLevel))
}

// MetaPath returns the metadata path for key without creating it.
func (c *Cache) MetaPath(key string) string { return c.KeyPath(key) + ".meta" }

// RangeCached reports whether the exact byte range [start, end) is already
// present in the local cache for key. It never opens or touches the upstream
// source, so HTTP handlers can use it as a cheap preflight before creating a
// remote client.
//
// The end offset is exclusive. For an HTTP range bytes=10-19, call
// RangeCached(key, 10, 20). A zero-length range is considered cached when the
// entry metadata exists.
func (c *Cache) RangeCached(key string, start, end int64, opts ...OpenOption) (bool, error) {
	if key == "" {
		return false, errors.New("varc: key is required")
	}
	if start < 0 || end < start {
		return false, ErrInvalidRange
	}
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return false, err
	}
	if !ok || !fileExists(c.KeyPath(key)) {
		return false, ErrCacheMiss
	}
	var openOpt openOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&openOpt)
		}
	}
	if shouldInvalidate(meta, meta.Size, openOpt) {
		return false, ErrCacheMiss
	}
	if end > meta.Size {
		return false, io.EOF
	}
	return containsRange(normalizeRanges(meta.Ranges, meta.Size), start, end), nil
}

// Coverage returns cached bytes and total size for a key.
func (c *Cache) Coverage(key string) (cached int64, size int64, complete bool, err error) {
	if key == "" {
		return 0, 0, false, errors.New("varc: key is required")
	}
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return 0, 0, false, err
	}
	if !ok {
		return 0, 0, false, ErrCacheMiss
	}
	cached = rangesLen(normalizeRanges(meta.Ranges, meta.Size))
	size = meta.Size
	complete = containsRange(meta.Ranges, 0, meta.Size)
	return cached, size, complete, nil
}

// RenameKey moves cached data from oldKey to newKey when newKey does not exist.
// It is intended for consumers that discover a better stable key after opening
// by a temporary path.  Active readers are not moved.
func (c *Cache) RenameKey(oldKey, newKey string) error {
	if oldKey == "" || newKey == "" {
		return errors.New("varc: oldKey and newKey are required")
	}
	oldPath := c.KeyPath(oldKey)
	newPath := c.KeyPath(newKey)
	if fileExists(newPath) || fileExists(newPath+".meta") {
		return fmt.Errorf("varc: destination key already exists: %q", newKey)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), c.dirMode); err != nil {
		return err
	}
	meta, ok, err := loadMeta(oldPath + ".meta")
	if err != nil {
		return err
	}
	if !ok {
		return ErrCacheMiss
	}
	meta.Key = newKey
	meta.UpdatedAt = time.Now()
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}
	if err := saveMeta(newPath+".meta", meta, c.dirMode, c.syncWrites); err != nil {
		_ = os.Rename(newPath, oldPath)
		return err
	}
	_ = os.Remove(oldPath + ".meta")
	c.forgetState(oldPath)
	return nil
}

// SnapshotMeta returns raw metadata for debugging and admin endpoints.
func (c *Cache) SnapshotMeta(key string) (map[string]any, error) {
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrCacheMiss
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// FsEntry implements fs.FileInfo for cached entries.
type FsEntry struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (e FsEntry) Name() string       { return e.name }
func (e FsEntry) Size() int64        { return e.size }
func (e FsEntry) Mode() fs.FileMode  { return e.mode }
func (e FsEntry) ModTime() time.Time { return e.modTime }
func (e FsEntry) IsDir() bool        { return e.isDir }
func (e FsEntry) Sys() any           { return nil }

// FileInfo returns an fs.FileInfo-like object for a key's cached metadata.
func (c *Cache) FileInfo(key string) (fs.FileInfo, error) {
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrCacheMiss
	}
	name := filepath.Base(key)
	if name == "." || name == string(os.PathSeparator) || name == "" {
		name = ShardKey(key, 0)
	}
	return FsEntry{name: name, size: meta.Size, mode: 0o444, modTime: meta.ModTime}, nil
}

// IsComplete reports whether key is fully cached.
func (c *Cache) IsComplete(key string) bool {
	_, _, complete, err := c.Coverage(key)
	return err == nil && complete
}

// WaitComplete blocks until key is complete or ctx is done.  It observes active
// downloads created by readers; it does not start new downloads.
func (c *Cache) WaitComplete(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	path := c.KeyPath(key)
	st := c.acquireState(path)
	defer c.releaseState(st)
	st.mu.Lock()
	defer st.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.reloadMetaLocked(st); err != nil {
			return err
		}
		if !st.loaded || !fileExists(path) {
			if activeTasks(st.tasks) == 0 && st.readers == 0 {
				return ErrCacheMiss
			}
		} else if containsRange(st.meta.Ranges, 0, st.meta.Size) {
			return nil
		}
		if activeTasks(st.tasks) == 0 && st.readers == 0 {
			return ErrCacheMiss
		}
		if err := waitStateChange(ctx, st); err != nil {
			return err
		}
	}
}
