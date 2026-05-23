// Package varc provides a small read-through disk cache for io.ReaderAt
// sources. It is built for media/range workloads: callers read byte ranges,
// varc fetches missing chunks from the source, stores them on disk, and serves
// repeated reads from the local cache.
package varc

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const mebi = 1048576

// Logger is the logging interface used by varc.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// OpenOption configures metadata for a cache entry.
type OpenOption func(*openOptions)

type openOptions struct {
	fingerprint string
	modTime     time.Time
}

// WithFingerprint sets a stable content fingerprint such as an ETag or hash.
// Changing the fingerprint invalidates stale cached data for the same key.
func WithFingerprint(fingerprint string) OpenOption {
	return func(o *openOptions) { o.fingerprint = fingerprint }
}

// WithModTime preserves the upstream modification time on the cache file.
func WithModTime(modTime time.Time) OpenOption {
	return func(o *openOptions) { o.modTime = modTime }
}

// Options configures Cache. ChunkSize, CacheDir, and ShardLevel are the core
// options used by the simple ReaderAt cache. Other fields are kept so existing
// config structs do not need churn while the API is simplified.
type Options struct {
	CacheDir          string
	BlockSize         int64
	ChunkSize         int64
	ChunkSizeLimit    int64
	ChunkStreams      int
	CacheMaxAge       time.Duration
	CacheMaxSize      int64
	CacheMinFreeSpace int64
	CachePollInterval time.Duration
	ReadAhead         int64
	FastFingerprint   bool
	HandleCaching     time.Duration
	ShardLevel        int
	Logger            Logger
}

// DefaultOptions returns production-safe defaults for a read-through cache.
func DefaultOptions() Options {
	return Options{
		CacheDir:          filepath.Join(os.TempDir(), "varc_cache"),
		BlockSize:         mebi,
		ChunkSize:         128 * mebi,
		ChunkSizeLimit:    -1,
		ChunkStreams:      2,
		CacheMaxAge:       time.Hour,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		CachePollInterval: time.Minute,
		HandleCaching:     5 * time.Second,
	}
}

// Cache is a sparse, read-through range cache.
type Cache struct {
	dir         string
	blockSize   int64
	chunkSize   int64
	shardLevel  int
	mu          sync.Mutex
	cond        *sync.Cond
	downloaders map[string][]*downloader
}

// Reader is a cache-backed reader returned by Cache.Open.
type Reader struct {
	cache  *Cache
	key    string
	path   string
	meta   cacheMeta
	src    io.ReaderAt
	pos    int64
	closed bool
	readMu sync.Mutex
}

type downloader struct {
	owner  *Reader
	path   string
	src    io.ReaderAt
	start  int64
	end    int64
	offset int64
	done   bool
	cancel bool
	err    error
	doneCh chan struct{}
}

type cacheMeta struct {
	Size        int64       `json:"size"`
	Fingerprint string      `json:"fingerprint,omitempty"`
	ModTime     time.Time   `json:"mod_time,omitempty"`
	Ranges      []byteRange `json:"ranges,omitempty"`
}

type byteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// ShardKey hashes key with MD5 and optionally applies directory sharding.
func ShardKey(key string, level int) string {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	if level <= 0 {
		return hash
	}
	var b strings.Builder
	for i := 0; i < level && i*2 < len(hash); i++ {
		b.WriteString(hash[i*2 : i*2+2])
		b.WriteByte('/')
	}
	b.WriteString(hash)
	return b.String()
}

// New creates a Cache. Zero-valued options are filled from DefaultOptions.
func New(ctx context.Context, opt Options) (*Cache, error) {
	_ = ctx
	merged := mergeOptions(DefaultOptions(), opt)
	if merged.BlockSize <= 0 {
		merged.BlockSize = mebi
	}
	if merged.ChunkSize <= 0 {
		merged.ChunkSize = 128 * mebi
	}
	if merged.BlockSize > merged.ChunkSize {
		merged.BlockSize = merged.ChunkSize
	}
	if err := os.MkdirAll(merged.CacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("varc: create cache dir: %w", err)
	}
	c := &Cache{
		dir:         merged.CacheDir,
		blockSize:   merged.BlockSize,
		chunkSize:   merged.ChunkSize,
		shardLevel:  merged.ShardLevel,
		downloaders: make(map[string][]*downloader),
	}
	c.cond = sync.NewCond(&c.mu)
	return c, nil
}

// Open opens key from the local cache, filling cache misses from src.
//
// If src is nil, Open is cache-only: it can read already cached ranges but
// cannot fill missing ranges. If src is non-nil, size must be >= 0.
func (c *Cache) Open(ctx context.Context, key string, size int64, src io.ReaderAt, opts ...OpenOption) (*Reader, error) {
	_ = ctx
	if key == "" {
		return nil, errors.New("varc: key is required")
	}
	if src != nil && size < 0 {
		return nil, errors.New("varc: size must be known when source is provided")
	}

	var openOpt openOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&openOpt)
		}
	}

	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	metaPath := path + ".meta"
	c.mu.Lock()
	meta, metaExists, err := loadMeta(metaPath)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	if src == nil {
		if !metaExists || !fileExists(path) {
			c.mu.Unlock()
			return nil, fmt.Errorf("varc: cache miss for %q", key)
		}
		size = meta.Size
	} else {
		stale := !metaExists || !fileExists(path)
		if !stale && openOpt.fingerprint != "" && meta.Fingerprint != openOpt.fingerprint {
			stale = true
		}
		if !stale && openOpt.fingerprint == "" && meta.Fingerprint == "" && meta.Size != size {
			stale = true
		}
		if stale {
			c.cancelDownloadersLocked(path)
			_ = os.Remove(path)
			_ = os.Remove(metaPath)
			meta = cacheMeta{Size: size, Fingerprint: openOpt.fingerprint, ModTime: openOpt.modTime}
		} else {
			meta.Size = size
			if openOpt.fingerprint != "" {
				meta.Fingerprint = openOpt.fingerprint
			}
			if !openOpt.modTime.IsZero() {
				meta.ModTime = openOpt.modTime
			}
		}
	}

	if meta.Size < 0 {
		c.mu.Unlock()
		return nil, fmt.Errorf("varc: unknown size for %q", key)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("varc: create cache key dir: %w", err)
	}
	if err := saveMeta(metaPath, meta); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if !meta.ModTime.IsZero() && fileExists(path) {
		_ = os.Chtimes(path, meta.ModTime, meta.ModTime)
	}
	c.mu.Unlock()

	return &Reader{cache: c, key: key, path: path, meta: meta, src: src}, nil
}

// Exists checks whether key has an existing cache file on disk.
func (c *Cache) Exists(key string) bool {
	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	return fileExists(path) && fileExists(path+".meta")
}

// Remove evicts key from the cache.
func (c *Cache) Remove(key string) error {
	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	c.mu.Lock()
	c.cancelDownloadersLocked(path)
	c.mu.Unlock()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + ".meta"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Stats returns simple cache statistics.
func (c *Cache) Stats() map[string]interface{} {
	var files int
	var bytesUsed int64
	_ = filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(path, ".meta") {
			return nil
		}
		files++
		if info, statErr := d.Info(); statErr == nil {
			bytesUsed += info.Size()
		}
		return nil
	})
	return map[string]interface{}{"files": files, "bytesUsed": bytesUsed}
}

// Close shuts down the cache. The simple ReaderAt cache has no background work.
func (c *Cache) Close() error { return nil }

func (r *Reader) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	n, err := r.readAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	return r.readAt(p, off)
}

func (r *Reader) readAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("varc: negative offset %d", off)
	}
	if off >= r.meta.Size {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	err := error(nil)
	if end > r.meta.Size {
		end = r.meta.Size
		err = io.EOF
	}
	if fillErr := r.ensureRange(off, end); fillErr != nil {
		return 0, fillErr
	}
	f, openErr := os.Open(r.path)
	if openErr != nil {
		return 0, openErr
	}
	defer f.Close()
	n, readErr := f.ReadAt(p[:end-off], off)
	if readErr != nil && readErr != io.EOF {
		return n, readErr
	}
	if err != nil {
		return n, err
	}
	return n, readErr
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		next = r.meta.Size + offset
	default:
		return r.pos, fmt.Errorf("varc: invalid whence %d", whence)
	}
	if next < 0 {
		return r.pos, fmt.Errorf("varc: negative seek position %d", next)
	}
	r.pos = next
	return r.pos, nil
}

func (r *Reader) Close() error {
	r.readMu.Lock()
	r.closed = true
	r.readMu.Unlock()
	r.cache.cancelReaderDownloaders(r)
	return nil
}

func (r *Reader) Size() int64 { return r.meta.Size }

func (r *Reader) ModTime() time.Time { return r.meta.ModTime }

func (r *Reader) ensureRange(start, end int64) error {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()

	for {
		if meta, ok, err := loadMeta(r.path + ".meta"); err != nil {
			return err
		} else if ok {
			r.meta = meta
		}
		if !fileExists(r.path) {
			r.meta.Ranges = nil
		}
		if containsRange(r.meta.Ranges, start, end) {
			return nil
		}
		missingStart, missingEnd, ok := firstMissingRange(r.meta.Ranges, start, end)
		if !ok {
			return nil
		}
		if r.src == nil {
			return fmt.Errorf("varc: cache miss for %q at %d-%d", r.key, missingStart, missingEnd-1)
		}
		r.cache.ensureDownloaderLocked(r, missingStart, missingEnd)
		r.cache.cond.Wait()
	}
}

func (c *Cache) ensureDownloaderLocked(owner *Reader, start, end int64) {
	c.pruneDownloadersLocked(owner.path)
	for _, d := range c.downloaders[owner.path] {
		if !d.done && !d.cancel && d.start <= start && d.end >= end {
			return
		}
	}
	chunkStart := start - start%c.blockSize
	chunkEnd := chunkStart + c.chunkSize
	if chunkEnd > owner.meta.Size {
		chunkEnd = owner.meta.Size
	}
	d := &downloader{
		owner:  owner,
		path:   owner.path,
		src:    owner.src,
		start:  chunkStart,
		end:    chunkEnd,
		offset: chunkStart,
		doneCh: make(chan struct{}),
	}
	c.downloaders[owner.path] = append(c.downloaders[owner.path], d)
	go c.runDownloader(d)
}

func (c *Cache) runDownloader(d *downloader) {
	defer func() {
		c.mu.Lock()
		d.done = true
		close(d.doneCh)
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	for {
		c.mu.Lock()
		if d.cancel || d.offset >= d.end {
			c.mu.Unlock()
			return
		}
		start := d.offset
		end := start + c.blockSize
		if end > d.end {
			end = d.end
		}
		c.mu.Unlock()

		buf := make([]byte, end-start)
		n, err := d.src.ReadAt(buf, start)
		if err != nil && err != io.EOF {
			c.finishDownloader(d, err)
			return
		}
		if n != len(buf) {
			c.finishDownloader(d, io.ErrUnexpectedEOF)
			return
		}

		c.mu.Lock()
		if d.cancel {
			c.mu.Unlock()
			return
		}
		if err := writeCacheBlock(d.path, buf, start, d.owner.meta.ModTime); err != nil {
			d.err = err
			d.cancel = true
			c.cond.Broadcast()
			c.mu.Unlock()
			return
		}
		meta, ok, err := loadMetaLocked(d.path + ".meta")
		if err != nil {
			d.err = err
			d.cancel = true
			c.cond.Broadcast()
			c.mu.Unlock()
			return
		}
		if !ok {
			meta = d.owner.meta
		}
		meta.Ranges = addRange(meta.Ranges, start, end)
		if err := saveMetaLocked(d.path+".meta", meta); err != nil {
			d.err = err
			d.cancel = true
			c.cond.Broadcast()
			c.mu.Unlock()
			return
		}
		d.owner.meta = meta
		d.offset = end
		c.cond.Broadcast()
		c.mu.Unlock()
	}
}

func (c *Cache) finishDownloader(d *downloader, err error) {
	c.mu.Lock()
	d.err = err
	d.cancel = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *Cache) pruneDownloadersLocked(path string) {
	dls := c.downloaders[path]
	kept := dls[:0]
	for _, d := range dls {
		if !d.done {
			kept = append(kept, d)
		}
	}
	if len(kept) == 0 {
		delete(c.downloaders, path)
		return
	}
	c.downloaders[path] = kept
}

func (c *Cache) cancelDownloadersLocked(path string) {
	for _, d := range c.downloaders[path] {
		d.cancel = true
	}
	c.cond.Broadcast()
}

func (c *Cache) cancelReaderDownloaders(owner *Reader) {
	c.mu.Lock()
	var done []chan struct{}
	for _, d := range c.downloaders[owner.path] {
		if d.owner == owner && !d.done {
			d.cancel = true
			done = append(done, d.doneCh)
		}
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	for _, ch := range done {
		<-ch
	}
}

func writeCacheBlock(path string, buf []byte, off int64, modTime time.Time) error {
	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if openErr != nil {
		return openErr
	}
	defer f.Close()
	if _, err := f.WriteAt(buf, off); err != nil {
		return err
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(path, modTime, modTime)
	}
	return nil
}

func loadMeta(path string) (cacheMeta, bool, error) {
	return loadMetaLocked(path)
}

func loadMetaLocked(path string) (cacheMeta, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cacheMeta{}, false, nil
		}
		return cacheMeta{}, true, err
	}
	var meta cacheMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return cacheMeta{}, true, err
	}
	return meta, true, nil
}

func saveMeta(path string, meta cacheMeta) error {
	return saveMetaLocked(path, meta)
}

func saveMetaLocked(path string, meta cacheMeta) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func containsRange(ranges []byteRange, start, end int64) bool {
	for _, r := range ranges {
		if r.Start <= start && r.End >= end {
			return true
		}
	}
	return false
}

func firstMissingRange(ranges []byteRange, start, end int64) (int64, int64, bool) {
	pos := start
	sorted := append([]byteRange(nil), ranges...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	for _, r := range sorted {
		if r.End <= pos {
			continue
		}
		if r.Start > pos {
			return pos, min(r.Start, end), true
		}
		if r.End > pos {
			pos = r.End
		}
		if pos >= end {
			return 0, 0, false
		}
	}
	if pos < end {
		return pos, end, true
	}
	return 0, 0, false
}

func addRange(ranges []byteRange, start, end int64) []byteRange {
	ranges = append(ranges, byteRange{Start: start, End: end})
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })
	merged := ranges[:0]
	for _, r := range ranges {
		if len(merged) == 0 || r.Start > merged[len(merged)-1].End {
			merged = append(merged, r)
			continue
		}
		if r.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = r.End
		}
	}
	return merged
}

func mergeOptions(defaults, override Options) Options {
	if override.CacheDir != "" {
		defaults.CacheDir = override.CacheDir
	}
	if override.BlockSize != 0 {
		defaults.BlockSize = override.BlockSize
	}
	if override.ChunkSize != 0 {
		defaults.ChunkSize = override.ChunkSize
	}
	if override.ChunkSizeLimit != 0 {
		defaults.ChunkSizeLimit = override.ChunkSizeLimit
	}
	if override.ChunkStreams != 0 {
		defaults.ChunkStreams = override.ChunkStreams
	}
	if override.CacheMaxAge != 0 {
		defaults.CacheMaxAge = override.CacheMaxAge
	}
	if override.CacheMaxSize != 0 {
		defaults.CacheMaxSize = override.CacheMaxSize
	}
	if override.CacheMinFreeSpace != 0 {
		defaults.CacheMinFreeSpace = override.CacheMinFreeSpace
	}
	if override.CachePollInterval != 0 {
		defaults.CachePollInterval = override.CachePollInterval
	}
	if override.ReadAhead != 0 {
		defaults.ReadAhead = override.ReadAhead
	}
	if override.FastFingerprint {
		defaults.FastFingerprint = true
	}
	if override.ShardLevel > 0 {
		defaults.ShardLevel = override.ShardLevel
	}
	if override.HandleCaching != 0 {
		defaults.HandleCaching = override.HandleCaching
	}
	if override.Logger != nil {
		defaults.Logger = override.Logger
	}
	return defaults
}
