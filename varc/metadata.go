package varc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

func (c *Cache) reloadMetaLocked(st *entryState) error {
	meta, ok, err := loadMeta(st.metaPath)
	if err != nil {
		return err
	}
	if ok {
		if err := validateMeta(meta); err != nil {
			return err
		}
		meta.Ranges = normalizeRanges(meta.Ranges, meta.Size)
		st.meta = meta
		st.loaded = true
	}
	return nil
}

func saturatingAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func saturatingMul(a, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

func subtractRange(ranges []byteRange, start, end int64) []byteRange {
	out := ranges[:0]
	for _, r := range ranges {
		if r.End <= start || r.Start >= end {
			out = append(out, r)
			continue
		}
		if r.Start < start {
			out = append(out, byteRange{Start: r.Start, End: start})
		}
		if r.End > end {
			out = append(out, byteRange{Start: end, End: r.End})
		}
	}
	return out
}

func (c *Cache) reserveInflight(ctx context.Context, n int64) error {
	if c.maxInflightBytes <= 0 || n <= 0 {
		return nil
	}
	for {
		cur := c.inflightByte.Load()
		if cur+n <= c.maxInflightBytes || cur == 0 {
			if c.inflightByte.CompareAndSwap(cur, cur+n) {
				return nil
			}
			continue
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return ErrClosed
		}
	}
}

func (c *Cache) releaseInflight(n int64) {
	if c.maxInflightBytes <= 0 || n <= 0 {
		return
	}
	c.inflightByte.Add(-n)
}

// writeTaskCacheBlockLocked writes a progressive cache segment. The caller
// holds t.state.mu, which serializes concurrent chunk streams on the shared
// task-owned descriptor.
func (c *Cache) writeTaskCacheBlockLocked(t *downloadTask, buf []byte, off int64) error {
	st := t.state
	if err := os.MkdirAll(filepath.Dir(st.path), c.dirMode); err != nil {
		return err
	}
	if t.file == nil {
		f, err := os.OpenFile(st.path, os.O_CREATE|os.O_RDWR, c.fileMode)
		if err != nil {
			return err
		}
		t.file = f
	}
	if _, err := t.file.WriteAt(buf, off); err != nil {
		return err
	}
	if c.syncWrites {
		if err := t.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// closeTaskCacheFile releases the descriptor once its downloader has stopped.
// It runs after all parallel chunk workers joined, so no writes can race it.
func (c *Cache) closeTaskCacheFile(t *downloadTask) error {
	st := t.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if t.file == nil {
		return nil
	}
	f := t.file
	t.file = nil
	if t.generation == st.generation && !st.meta.ModTime.IsZero() {
		_ = os.Chtimes(st.path, st.meta.ModTime, st.meta.ModTime)
	}
	return f.Close()
}

func (c *Cache) finishTask(t *downloadTask, err error) {
	intentionalCancel := err != nil && errors.Is(err, context.Canceled) && t.ctx.Err() != nil
	reportableError := err != nil && !intentionalCancel
	if reportableError {
		c.metricDownloadErrors.Add(1)
	}
	t.state.mu.Lock()
	t.err = err
	t.done = true
	if t.generation != t.state.generation {
		t.state.notifyLocked()
		t.state.mu.Unlock()
		return
	}
	switch {
	case reportableError:
		t.state.lastError = err
		t.state.failureSeq++
	case err == nil:
		// A completed retry/task supersedes an older transient entry error.
		t.state.lastError = nil
	}
	t.state.notifyLocked()
	t.state.mu.Unlock()
}

func (c *Cache) pruneTasksLocked(st *entryState) {
	for k, t := range st.tasks {
		if t.done {
			delete(st.tasks, k)
		}
	}
}

func (c *Cache) cancelBlockingTasksLocked(st *entryState) {
	for _, t := range st.tasks {
		if !t.done && t.priority == priorityBlocking {
			t.cancel()
		}
	}
	c.schedulerMu.Lock()
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
	st.notifyLocked()
}

func (c *Cache) cancelTasksLocked(st *entryState) {
	for _, t := range st.tasks {
		if !t.done {
			t.cancel()
		}
	}
	c.schedulerMu.Lock()
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
	st.notifyLocked()
}

func (c *Cache) saveMetaLocked(st *entryState) error {
	st.meta.Version = metaVersion
	st.meta.Ranges = normalizeRanges(st.meta.Ranges, st.meta.Size)
	if st.meta.CreatedAt.IsZero() {
		st.meta.CreatedAt = time.Now()
	}
	if st.meta.AccessedAt.IsZero() {
		st.meta.AccessedAt = time.Now()
	}
	if st.meta.UpdatedAt.IsZero() {
		st.meta.UpdatedAt = time.Now()
	}
	if st.meta.ChunkSize <= 0 {
		st.meta.ChunkSize = c.chunkSize
	}
	if err := withFileLock(st.metaPath+".lock", c.dirMode, func() error {
		if disk, ok, err := loadMeta(st.metaPath); err != nil {
			return err
		} else if ok && sameMetaIdentity(disk, st.meta) {
			st.meta.Ranges = normalizeRanges(append(disk.Ranges, st.meta.Ranges...), st.meta.Size)
			for _, checksum := range disk.Checksums {
				st.meta.Checksums = addChecksum(st.meta.Checksums, checksum)
			}
			if st.meta.CreatedAt.IsZero() || (!disk.CreatedAt.IsZero() && disk.CreatedAt.Before(st.meta.CreatedAt)) {
				st.meta.CreatedAt = disk.CreatedAt
			}
		}
		return saveMeta(st.metaPath, st.meta, c.dirMode, c.syncWrites)
	}); err != nil {
		return err
	}
	c.metricMetaWrites.Add(1)
	return nil
}

func sameMetaIdentity(a, b cacheMeta) bool {
	return a.Key == b.Key && a.Size == b.Size && a.Fingerprint == b.Fingerprint
}

func withFileLock(path string, dirMode os.FileMode, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func loadMeta(path string) (cacheMeta, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cacheMeta{}, false, nil
		}
		return cacheMeta{}, true, err
	}
	var meta cacheMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return cacheMeta{}, true, fmt.Errorf("%w: %s: %v", ErrCorruptMeta, path, err)
	}
	return meta, true, nil
}

func saveMeta(path string, meta cacheMeta, dirMode os.FileMode, syncWrites bool) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(b); err != nil {
		cleanup()
		return err
	}
	if syncWrites {
		if err := tmp.Sync(); err != nil {
			cleanup()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Chmod(tmpName, defaultFileMode); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if syncWrites {
		_ = syncDir(filepath.Dir(path))
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func validateMeta(meta cacheMeta) error {
	if meta.Version == 0 {
		// v1 compatibility: older metadata did not carry Version or
		// timestamps.  Missing version is accepted and upgraded on the next save.
	} else if meta.Version > metaVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrCorruptMeta, meta.Version)
	}
	if meta.Size < 0 {
		return fmt.Errorf("%w: negative size", ErrCorruptMeta)
	}
	for _, r := range meta.Ranges {
		if r.Start < 0 || r.End < r.Start || r.End > meta.Size {
			return fmt.Errorf("%w: bad range %d-%d size=%d", ErrCorruptMeta, r.Start, r.End, meta.Size)
		}
	}
	return nil
}

// ListEntries scans the cache directory and returns metadata for all entries.
func (c *Cache) ListEntries(ctx context.Context) ([]EntryInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	var entries []EntryInfo
	var errs []error
	err := filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() || !strings.HasSuffix(path, ".meta") || isTempFile(path) {
			return nil
		}
		info := c.entryInfoFromMetaPath(path)
		entries = append(entries, info)
		return nil
	})
	if err != nil {
		return entries, err
	}
	if len(errs) > 0 {
		return entries, joinErrors(errs...)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].AccessedAt.Before(entries[j].AccessedAt) })
	return entries, nil
}

func (c *Cache) entryInfoFromMetaPath(metaPath string) EntryInfo {
	dataPath := strings.TrimSuffix(metaPath, ".meta")
	info := EntryInfo{Path: dataPath, MetaPath: metaPath}
	meta, ok, err := loadMeta(metaPath)
	if err != nil {
		info.MetadataErr = err
	} else if ok {
		info.MetadataOK = true
		info.Key = meta.Key
		info.Size = meta.Size
		info.Fingerprint = meta.Fingerprint
		info.ModTime = meta.ModTime
		info.CreatedAt = meta.CreatedAt
		info.UpdatedAt = meta.UpdatedAt
		info.AccessedAt = meta.AccessedAt
		info.Ranges = cloneRanges(normalizeRanges(meta.Ranges, meta.Size))
		info.CachedBytes = rangesLen(info.Ranges)
		if meta.Size > 0 {
			info.Percent = float64(info.CachedBytes) * 100 / float64(meta.Size)
		} else {
			info.Percent = 100
		}
		info.Attrs = cloneStringMap(meta.Attrs)
		info.Pinned = isPinnedAttrs(meta.Attrs)
		info.Complete = containsRange(info.Ranges, 0, meta.Size)
	}
	if st, err := os.Stat(dataPath); err == nil {
		info.OnDisk = true
		info.DataBytes = st.Size()
	}
	c.mu.Lock()
	if state := c.states[dataPath]; state != nil {
		state.mu.Lock()
		info.OpenReaders = state.readers
		info.ActiveFetches = activeTasks(state.tasks)
		state.mu.Unlock()
	}
	c.mu.Unlock()
	return info
}

func activeTasks(tasks map[string]*downloadTask) int {
	var n int
	for _, t := range tasks {
		if !t.done {
			n++
		}
	}
	return n
}
