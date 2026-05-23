// Package varc provides an importable read-through range cache.
//
// It is designed for applications that want rclone-derived sparse range
// caching without running the HTTP proxy. Implement RemoteObject for your
// upstream source, then call Cache.Open to get a reader backed by the local
// disk cache.
package varc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tgdrive/varc/internal"
	"github.com/tgdrive/varc/internal/types"
)

const mebi = 1048576

// Logger is the logging interface used by varc.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// OpenOption configures a remote object open. RangeOption is the standard option.
type OpenOption interface {
	Header() (key, value string)
}

// RangeOption requests a byte range from a RemoteObject.
type RangeOption struct {
	Start int64
	End   int64
}

// Header returns the HTTP Range header represented by the option.
func (o RangeOption) Header() (key, value string) {
	return (&types.RangeOption{Start: o.Start, End: o.End}).Header()
}

// RemoteObject is the minimal upstream object contract varc needs to fill cache misses.
type RemoteObject interface {
	Open(ctx context.Context, options ...OpenOption) (io.ReadCloser, error)
	Size() int64
	String() string
}

// Fingerprinter can be implemented by RemoteObject to invalidate stale cache entries.
type Fingerprinter interface {
	Fingerprint() string
}

// ModTimer can be implemented by RemoteObject to preserve upstream modification time.
type ModTimer interface {
	ModTime(ctx context.Context) time.Time
}

// Source can open an arbitrary byte range from an upstream object.
// End is inclusive. If end is negative, the range continues to EOF.
type Source interface {
	OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error)
}

// Object describes a cacheable object and its custom cache key.
type Object struct {
	Key         string
	Size        int64
	Source      Source
	Fingerprint string
	ModTime     time.Time
}

// ObjectOption configures Object helpers like OpenReadSeeker.
type ObjectOption func(*Object)

// WithKey sets the cache key used on disk.
func WithKey(key string) ObjectOption {
	return func(obj *Object) { obj.Key = key }
}

// WithSize sets the object size. This is required for generic seekable sources.
func WithSize(size int64) ObjectOption {
	return func(obj *Object) { obj.Size = size }
}

// WithFingerprint sets a stable content fingerprint such as an ETag or hash.
func WithFingerprint(fingerprint string) ObjectOption {
	return func(obj *Object) { obj.Fingerprint = fingerprint }
}

// WithModTime sets the object's modification time.
func WithModTime(modTime time.Time) ObjectOption {
	return func(obj *Object) { obj.ModTime = modTime }
}

// Reader is a cache-backed reader. It can be wrapped with standard io decorators.
type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Size() int64
}

// Options configures Cache.
type Options struct {
	CacheDir          string
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
	Logger            Logger
}

// DefaultOptions returns production-safe defaults for a read-through cache.
func DefaultOptions() Options {
	return Options{
		CacheDir:          filepath.Join(os.TempDir(), "varc_cache"),
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

// Cache is an importable read-through range cache.
type Cache struct {
	engine *internal.Engine
}

// New creates a Cache. Zero-valued options are filled from DefaultOptions.
func New(ctx context.Context, opt Options) (*Cache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := mergeOptions(DefaultOptions(), opt)
	engOpt := types.Options{
		ChunkSize:         merged.ChunkSize,
		ChunkSizeLimit:    merged.ChunkSizeLimit,
		ChunkStreams:      merged.ChunkStreams,
		CacheMaxAge:       merged.CacheMaxAge,
		CacheMaxSize:      merged.CacheMaxSize,
		CacheMinFreeSpace: merged.CacheMinFreeSpace,
		CachePollInterval: merged.CachePollInterval,
		ReadAhead:         merged.ReadAhead,
		FastFingerprint:   merged.FastFingerprint,
		HandleCaching:     merged.HandleCaching,
		CacheDir:          merged.CacheDir,
		Logger:            merged.Logger,
	}
	engOpt.Init()

	engine, err := internal.New(ctx, &engOpt)
	if err != nil {
		return nil, err
	}
	return &Cache{engine: engine}, nil
}

// Open opens key from the local cache, filling cache misses from obj.
// Pass obj=nil for cache-only reads.
func (c *Cache) Open(ctx context.Context, key string, obj RemoteObject) (Reader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	h, err := c.engine.OpenCached(key, adaptRemote(obj))
	if err != nil {
		return nil, err
	}
	sizer, _ := h.(interface{ Size() int64 })
	return &cacheReader{handle: h, sizer: sizer}, nil
}

// OpenObject opens obj using obj.Key as the cache key.
func (c *Cache) OpenObject(ctx context.Context, obj Object) (Reader, error) {
	if obj.Key == "" {
		return nil, errors.New("varc: object key is required")
	}
	if obj.Source == nil {
		return nil, errors.New("varc: object source is required")
	}
	if obj.Size < 0 {
		return nil, errors.New("varc: object size must be known")
	}
	return c.Open(ctx, obj.Key, adaptObject(obj))
}

// OpenReadSeeker opens a cache-backed reader from any io.ReadSeeker.
// Use WithKey and WithSize unless src also implements Stat() (for example *os.File).
func (c *Cache) OpenReadSeeker(ctx context.Context, src io.ReadSeeker, opts ...ObjectOption) (Reader, error) {
	if src == nil {
		return nil, errors.New("varc: read seeker source is required")
	}
	obj := Object{Size: -1, Source: NewReadSeekerSource(src)}
	applyObjectOptions(&obj, opts)
	if obj.Size < 0 {
		if statter, ok := src.(interface{ Stat() (os.FileInfo, error) }); ok {
			fi, err := statter.Stat()
			if err != nil {
				return nil, fmt.Errorf("varc: stat read seeker: %w", err)
			}
			obj.Size = fi.Size()
		}
	}
	return c.OpenObject(ctx, obj)
}

// OpenReaderAt opens a cache-backed reader from any io.ReaderAt.
func (c *Cache) OpenReaderAt(ctx context.Context, src io.ReaderAt, opts ...ObjectOption) (Reader, error) {
	if src == nil {
		return nil, errors.New("varc: reader-at source is required")
	}
	obj := Object{Size: -1}
	applyObjectOptions(&obj, opts)
	if obj.Size < 0 {
		return nil, errors.New("varc: WithSize is required for ReaderAt sources")
	}
	obj.Source = NewReaderAtSource(src, obj.Size)
	return c.OpenObject(ctx, obj)
}

// Remove evicts key from the cache.
func (c *Cache) Remove(key string) error {
	return c.engine.Remove(key)
}

// Stats returns cache statistics.
func (c *Cache) Stats() map[string]interface{} {
	return c.engine.Stats()
}

// Close shuts down the cache and background cleaner.
func (c *Cache) Close() error {
	return c.engine.Close()
}

type cacheReader struct {
	handle internal.Handle
	sizer  interface{ Size() int64 }
}

type objectRemote struct {
	object Object
}

func (o objectRemote) Open(ctx context.Context, options ...OpenOption) (io.ReadCloser, error) {
	start, end := int64(0), int64(-1)
	for _, option := range options {
		key, value := option.Header()
		if strings.EqualFold(key, "Range") {
			parsedStart, parsedEnd, err := parseRangeHeader(value)
			if err != nil {
				return nil, err
			}
			start, end = parsedStart, parsedEnd
		}
	}
	return o.object.Source.OpenRange(ctx, start, end)
}

func (o objectRemote) Size() int64 { return o.object.Size }

func (o objectRemote) String() string { return o.object.Key }

type objectRemoteFingerprint struct{ objectRemote }

func (o objectRemoteFingerprint) Fingerprint() string { return o.object.Fingerprint }

type objectRemoteModTime struct{ objectRemote }

func (o objectRemoteModTime) ModTime(ctx context.Context) time.Time { return o.object.ModTime }

type objectRemoteFingerprintModTime struct{ objectRemote }

func (o objectRemoteFingerprintModTime) Fingerprint() string { return o.object.Fingerprint }

func (o objectRemoteFingerprintModTime) ModTime(ctx context.Context) time.Time {
	return o.object.ModTime
}

func adaptObject(obj Object) RemoteObject {
	base := objectRemote{object: obj}
	hasFingerprint := obj.Fingerprint != ""
	hasModTime := !obj.ModTime.IsZero()
	switch {
	case hasFingerprint && hasModTime:
		return objectRemoteFingerprintModTime{objectRemote: base}
	case hasFingerprint:
		return objectRemoteFingerprint{objectRemote: base}
	case hasModTime:
		return objectRemoteModTime{objectRemote: base}
	default:
		return base
	}
}

type readSeekerSource struct {
	mu  sync.Mutex
	src io.ReadSeeker
}

// NewReadSeekerSource adapts any io.ReadSeeker into a Source.
// Range opens are serialized because io.ReadSeeker has a shared cursor.
func NewReadSeekerSource(src io.ReadSeeker) Source {
	return &readSeekerSource{src: src}
}

func (s *readSeekerSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if start < 0 {
		return nil, fmt.Errorf("varc: negative range start %d", start)
	}
	s.mu.Lock()
	if _, err := s.src.Seek(start, io.SeekStart); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var reader io.Reader = s.src
	if end >= start {
		reader = io.LimitReader(s.src, end-start+1)
	}
	return &lockedReadCloser{reader: reader, unlock: s.mu.Unlock}, nil
}

type lockedReadCloser struct {
	reader io.Reader
	once   sync.Once
	unlock func()
}

func (r *lockedReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		r.Close()
	}
	return n, err
}

func (r *lockedReadCloser) Close() error {
	r.once.Do(r.unlock)
	return nil
}

type readerAtSource struct {
	src  io.ReaderAt
	size int64
}

// NewReaderAtSource adapts any io.ReaderAt into a Source.
func NewReaderAtSource(src io.ReaderAt, size int64) Source {
	return readerAtSource{src: src, size: size}
}

func (s readerAtSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if start < 0 {
		return nil, fmt.Errorf("varc: negative range start %d", start)
	}
	if start > s.size {
		start = s.size
	}
	if end < 0 || end >= s.size {
		end = s.size - 1
	}
	if end < start {
		return io.NopCloser(strings.NewReader("")), nil
	}
	return io.NopCloser(io.NewSectionReader(s.src, start, end-start+1)), nil
}

func applyObjectOptions(obj *Object, opts []ObjectOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(obj)
		}
	}
}

func parseRangeHeader(value string) (start, end int64, err error) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, -1, fmt.Errorf("varc: unsupported range header %q", value)
	}
	rangeValue := strings.TrimPrefix(value, "bytes=")
	if strings.HasSuffix(rangeValue, "-") {
		_, err = fmt.Sscanf(rangeValue, "%d-", &start)
		return start, -1, err
	}
	_, err = fmt.Sscanf(rangeValue, "%d-%d", &start, &end)
	return start, end, err
}

func (r *cacheReader) Read(p []byte) (int, error)              { return r.handle.Read(p) }
func (r *cacheReader) ReadAt(p []byte, off int64) (int, error) { return r.handle.ReadAt(p, off) }
func (r *cacheReader) Seek(offset int64, whence int) (int64, error) {
	return r.handle.Seek(offset, whence)
}
func (r *cacheReader) Close() error { return r.handle.Close() }
func (r *cacheReader) Size() int64 {
	if r.sizer == nil {
		return -1
	}
	return r.sizer.Size()
}

type remoteAdapter struct {
	remote RemoteObject
}

func (a remoteAdapter) Open(ctx context.Context, options ...types.OpenOption) (io.ReadCloser, error) {
	converted := make([]OpenOption, len(options))
	for i, option := range options {
		converted[i] = internalOpenOption{option: option}
	}
	return a.remote.Open(ctx, converted...)
}

func (a remoteAdapter) Size() int64    { return a.remote.Size() }
func (a remoteAdapter) String() string { return a.remote.String() }

type remoteAdapterFingerprint struct{ remoteAdapter }

func (a remoteAdapterFingerprint) Fingerprint() string {
	return a.remote.(Fingerprinter).Fingerprint()
}

type remoteAdapterModTime struct{ remoteAdapter }

func (a remoteAdapterModTime) ModTime(ctx context.Context) time.Time {
	return a.remote.(ModTimer).ModTime(ctx)
}

type remoteAdapterFingerprintModTime struct{ remoteAdapter }

func (a remoteAdapterFingerprintModTime) Fingerprint() string {
	return a.remote.(Fingerprinter).Fingerprint()
}

func (a remoteAdapterFingerprintModTime) ModTime(ctx context.Context) time.Time {
	return a.remote.(ModTimer).ModTime(ctx)
}

type internalOpenOption struct {
	option types.OpenOption
}

func (o internalOpenOption) Header() (key, value string) {
	return o.option.Header()
}

func adaptRemote(obj RemoteObject) types.RemoteObject {
	if obj == nil {
		return nil
	}
	base := remoteAdapter{remote: obj}
	_, hasFingerprint := obj.(Fingerprinter)
	_, hasModTime := obj.(ModTimer)
	switch {
	case hasFingerprint && hasModTime:
		return remoteAdapterFingerprintModTime{remoteAdapter: base}
	case hasFingerprint:
		return remoteAdapterFingerprint{remoteAdapter: base}
	case hasModTime:
		return remoteAdapterModTime{remoteAdapter: base}
	default:
		return base
	}
}

func mergeOptions(defaults, override Options) Options {
	if override.CacheDir != "" {
		defaults.CacheDir = override.CacheDir
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
	if override.HandleCaching != 0 {
		defaults.HandleCaching = override.HandleCaching
	}
	if override.Logger != nil {
		defaults.Logger = override.Logger
	}
	return defaults
}
