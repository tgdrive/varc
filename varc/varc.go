// Package varc implements a production-oriented read-through range cache for
// immutable or content-addressable byte streams.
//
// The cache is designed for media/range workloads, HTTP range servers,
// object-storage gateways, and any consumer that reads byte ranges from a
// slower io.ReaderAt source.  A caller opens a key with a known size and an
// upstream reader; varc stores fetched byte ranges in a sparse local file and
// persists a compact metadata sidecar describing which ranges are present.
// Repeated reads are served from disk and cache misses are filled from the
// source.
//
// The implementation emphasizes operational safety:
//   - persistent sparse range metadata
//   - atomic metadata updates
//   - duplicate download coalescing
//   - bounded concurrent fetches
//   - optional read-ahead
//   - cache-only reads
//   - fingerprint/modtime invalidation
//   - LRU/age/free-space cleanup
//   - metrics and entry inspection
//   - context-aware reads
//
// A cache entry is assumed to be immutable for a given fingerprint.  When the
// fingerprint changes, stale cached data for the same key is discarded.  If no
// fingerprint is supplied, a size change invalidates the entry.
package varc

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	kibi int64 = 1024
	mebi int64 = 1024 * kibi
	gibi int64 = 1024 * mebi

	defaultFileMode os.FileMode = 0o600
	defaultDirMode  os.FileMode = 0o700

	metaVersion = 2
)

var (
	// ErrClosed is returned when the cache or reader has been closed.
	ErrClosed = errors.New("varc: closed")

	// ErrCacheMiss is returned for cache-only readers when a requested range is
	// absent from the local sparse file.
	ErrCacheMiss = errors.New("varc: cache miss")

	// ErrSourceRequired is returned when a missing range must be filled but no
	// source was supplied.
	ErrSourceRequired = errors.New("varc: source required")

	// ErrInvalidRange is returned for negative offsets or malformed ranges.
	ErrInvalidRange = errors.New("varc: invalid range")

	// ErrCorruptMeta is returned when metadata exists but cannot be decoded or
	// fails basic invariants.
	ErrCorruptMeta = errors.New("varc: corrupt metadata")
)

// Logger is the logging interface used by varc.  It intentionally matches the
// common printf-style subset provided by zap.SugaredLogger, logrus, zerolog
// wrappers, and many in-house loggers.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

// Source describes a readable byte source with an optional close method.
// io.ReaderAt is enough for varc; Close is detected dynamically when present.
type Source interface {
	io.ReaderAt
}

// OpenOption configures one cache entry opened through Cache.Open.
type OpenOption func(*openOptions)

type openOptions struct {
	fingerprint string
	modTime     time.Time
	cacheOnly   bool
	strict      bool
	attrs       map[string]string
}

// WithFingerprint sets a stable content fingerprint such as an ETag, object
// generation, content hash, or database version.  Changing the fingerprint
// invalidates stale cached data for the same key.
func WithFingerprint(fingerprint string) OpenOption {
	return func(o *openOptions) { o.fingerprint = strings.TrimSpace(fingerprint) }
}

// WithModTime preserves the upstream modification time on both metadata and
// the cache data file.
func WithModTime(modTime time.Time) OpenOption {
	return func(o *openOptions) { o.modTime = modTime }
}

// WithCacheOnly makes the reader fail on missing local ranges even if a source
// was accidentally provided.  It is useful for offline mode and for tests that
// assert exact cache coverage.
func WithCacheOnly() OpenOption {
	return func(o *openOptions) { o.cacheOnly = true }
}

// WithStrictFingerprint rejects an existing entry if the requested fingerprint
// is empty while the cached entry has a fingerprint, or vice versa.  Without
// this option, an empty requested fingerprint means "size-based validation".
func WithStrictFingerprint() OpenOption {
	return func(o *openOptions) { o.strict = true }
}

// WithAttr stores a string attribute in metadata.  Attributes are not used by
// the cache core, but they are helpful for consumers that want to persist MIME
// type, backend id, remote path, or opaque generation numbers on disk.
func WithAttr(key, value string) OpenOption {
	return func(o *openOptions) {
		if key == "" {
			return
		}
		if o.attrs == nil {
			o.attrs = make(map[string]string)
		}
		o.attrs[key] = value
	}
}

// Options configures Cache.  Zero values are filled from DefaultOptions.
type Options struct {
	// CacheDir is the root directory containing sparse data files and .meta
	// sidecars.
	CacheDir string

	// BlockSize is the smallest persisted range granularity.  Downloads are
	// committed block-by-block so readers waiting on the first block of a large
	// chunk can resume quickly.
	BlockSize int64

	// ChunkSize is the preferred downloader window.  A read miss starts a
	// downloader for at least one chunk aligned to BlockSize.  ChunkSize is capped
	// by ChunkSizeLimit when the latter is positive.
	ChunkSize int64

	// ChunkSizeLimit is a compatibility option.  When positive, ChunkSize is
	// clamped to this limit.
	ChunkSizeLimit int64

	// ChunkStreams limits the number of concurrent download goroutines across the
	// whole cache.  Values <= 0 use a safe default.
	ChunkStreams int

	// MaxInflightBytes is a soft limit on bytes currently being downloaded.  The
	// implementation enforces it before starting each block fetch.
	MaxInflightBytes int64

	// CacheMaxAge evicts entries whose AccessedAt is older than this duration
	// when Prune runs.  Non-positive disables age based eviction.
	CacheMaxAge time.Duration

	// CacheMaxSize keeps total cache data bytes under this value after Prune.
	// Non-positive disables size based eviction.
	CacheMaxSize int64

	// CacheMinFreeSpace asks Prune to evict least-recently-used entries until at
	// least this many bytes are free on the filesystem.  Non-positive disables
	// free-space based eviction.
	CacheMinFreeSpace int64

	// CachePollInterval controls the background janitor.  Non-positive disables
	// the janitor unless CleanOnStart is true.
	CachePollInterval time.Duration

	// ReadAhead specifies how many bytes after the caller's requested range should
	// be scheduled opportunistically.  It is capped to ChunkSize internally.
	ReadAhead int64

	// FastFingerprint is retained for compatibility with older configs.  The core
	// cache does not compute full-file fingerprints by default because upstream
	// sources are often remote and expensive.
	FastFingerprint bool

	// HandleCaching is retained for compatibility.  It does not affect cache
	// correctness.  Consumers can use it to decide how long to keep Reader
	// handles open.
	HandleCaching time.Duration

	// ShardLevel controls path sharding.  A level of 2 creates aa/bb/hash.  Shards
	// avoid huge directories on long-running media servers.
	ShardLevel int

	// Logger receives operational messages.  Nil uses a no-op logger.
	Logger Logger

	// FileMode and DirMode control permissions for new files/directories.  Zero
	// values use 0600/0700.
	FileMode os.FileMode
	DirMode  os.FileMode

	// SyncWrites fsyncs data and metadata before making a range visible.  This is
	// safer after power loss but slower on spinning disks and network filesystems.
	SyncWrites bool

	// NoBackground disables the janitor goroutine even if CachePollInterval is
	// positive.
	NoBackground bool

	// CleanOnStart runs Prune once from New before returning the cache.
	CleanOnStart bool

	// ReadRetryCount retries short-lived source errors.  EOF with a short read is
	// treated as an error and is also retried.
	ReadRetryCount int

	// ReadRetryDelay is the base delay between retries.  Retries use linear
	// backoff: delay, 2*delay, ...
	ReadRetryDelay time.Duration

	// VerifyChecksum computes a CRC32 checksum for each downloaded block and
	// stores the value in metadata.  Reads can verify complete requested ranges
	// only when all blocks have checksums.  This option costs CPU and metadata
	// churn, so it is off by default.
	VerifyChecksum bool

	// TouchInterval limits AccessedAt metadata writes for hot files.  A value of
	// zero uses a default; a negative value updates metadata on every read.
	TouchInterval time.Duration
}

// DefaultOptions returns production-safe defaults for a read-through range cache.
func DefaultOptions() Options {
	return Options{
		CacheDir:          filepath.Join(os.TempDir(), "varc_cache"),
		BlockSize:         mebi,
		ChunkSize:         128 * mebi,
		ChunkSizeLimit:    -1,
		ChunkStreams:      4,
		MaxInflightBytes:  512 * mebi,
		CacheMaxAge:       0,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		CachePollInterval: time.Minute,
		ReadAhead:         16 * mebi,
		HandleCaching:     5 * time.Second,
		ShardLevel:        2,
		FileMode:          defaultFileMode,
		DirMode:           defaultDirMode,
		ReadRetryCount:    2,
		ReadRetryDelay:    100 * time.Millisecond,
		TouchInterval:     10 * time.Second,
	}
}

// Cache is a sparse, read-through, range-addressed cache.
type Cache struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir               string
	blockSize         int64
	chunkSize         int64
	chunkStreams      int
	maxInflightBytes  int64
	cacheMaxAge       time.Duration
	cacheMaxSize      int64
	cacheMinFreeSpace int64
	pollInterval      time.Duration
	readAhead         int64
	fastFingerprint   bool
	handleCaching     time.Duration
	shardLevel        int
	logger            Logger
	fileMode          os.FileMode
	dirMode           os.FileMode
	syncWrites        bool
	readRetryCount    int
	readRetryDelay    time.Duration
	verifyChecksum    bool
	touchInterval     time.Duration

	sem chan struct{}

	mu     sync.Mutex
	states map[string]*entryState

	closed       atomic.Bool
	inflightByte atomic.Int64

	metricOpens            atomic.Int64
	metricOpenErrors       atomic.Int64
	metricReads            atomic.Int64
	metricReadBytes        atomic.Int64
	metricHits             atomic.Int64
	metricHitBytes         atomic.Int64
	metricMisses           atomic.Int64
	metricMissBytes        atomic.Int64
	metricSourceReads      atomic.Int64
	metricSourceReadBytes  atomic.Int64
	metricDownloadErrors   atomic.Int64
	metricEvictions        atomic.Int64
	metricEvictedBytes     atomic.Int64
	metricMetaWrites       atomic.Int64
	metricBackgroundPrunes atomic.Int64
}

// Reader is a cache-backed reader returned by Cache.Open.  It implements
// io.Reader, io.ReaderAt, io.Seeker, io.Closer, and io.ReaderAt-style context
// methods through ReadAtContext.
type Reader struct {
	cache  *Cache
	state  *entryState
	key    string
	path   string
	meta   cacheMeta
	src    io.ReaderAt
	ctx    context.Context
	cancel context.CancelFunc

	cacheOnly bool
	closed    bool
	pos       int64
	readMu    sync.Mutex
}

type entryState struct {
	path     string
	metaPath string

	mu   sync.Mutex
	cond *sync.Cond

	meta      cacheMeta
	loaded    bool
	tasks     map[string]*downloadTask
	lastError error
	readers   int
	refs      int
	lastTouch time.Time
	removed   bool
}

type downloadTask struct {
	state  *entryState
	cache  *Cache
	src    io.ReaderAt
	start  int64
	end    int64
	offset int64
	key    string

	ctx    context.Context
	cancel context.CancelFunc
	done   bool
	err    error
}

type cacheMeta struct {
	Version     int               `json:"version"`
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	Fingerprint string            `json:"fingerprint,omitempty"`
	ModTime     time.Time         `json:"mod_time,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	AccessedAt  time.Time         `json:"accessed_at"`
	BlockSize   int64             `json:"block_size"`
	ChunkSize   int64             `json:"chunk_size"`
	Ranges      []byteRange       `json:"ranges,omitempty"`
	Attrs       map[string]string `json:"attrs,omitempty"`
	Checksums   []blockChecksum   `json:"checksums,omitempty"`
}

type byteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type blockChecksum struct {
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	CRC32 uint32 `json:"crc32"`
}

// EntryInfo describes one cached entry.
type EntryInfo struct {
	Key           string
	Path          string
	MetaPath      string
	Size          int64
	DataBytes     int64
	CachedBytes   int64
	Percent       float64
	Fingerprint   string
	ModTime       time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	AccessedAt    time.Time
	Ranges        []byteRange
	Attrs         map[string]string
	Pinned        bool
	Complete      bool
	OnDisk        bool
	MetadataOK    bool
	MetadataErr   error
	OpenReaders   int
	ActiveFetches int
}

// Metrics is a point-in-time snapshot of cache counters.
type Metrics struct {
	Opens              int64 `json:"opens"`
	OpenErrors         int64 `json:"open_errors"`
	Reads              int64 `json:"reads"`
	ReadBytes          int64 `json:"read_bytes"`
	Hits               int64 `json:"hits"`
	HitBytes           int64 `json:"hit_bytes"`
	Misses             int64 `json:"misses"`
	MissBytes          int64 `json:"miss_bytes"`
	SourceReads        int64 `json:"source_reads"`
	SourceReadBytes    int64 `json:"source_read_bytes"`
	DownloadErrors     int64 `json:"download_errors"`
	Evictions          int64 `json:"evictions"`
	EvictedBytes       int64 `json:"evicted_bytes"`
	MetaWrites         int64 `json:"meta_writes"`
	BackgroundPrunes   int64 `json:"background_prunes"`
	InflightBytes      int64 `json:"inflight_bytes"`
	OpenTrackedEntries int   `json:"open_tracked_entries"`
}

// PruneStats reports work performed by Prune.
type PruneStats struct {
	Scanned       int
	Removed       int
	RemovedBytes  int64
	Errors        []error
	BytesBefore   int64
	BytesAfter    int64
	FreeBefore    int64
	FreeAfter     int64
	ReasonAge     int
	ReasonSize    int
	ReasonFree    int
	ReasonInvalid int
}

// VerifyStats reports consistency checks performed by Verify.
type VerifyStats struct {
	Entries        int
	Complete       int
	Incomplete     int
	MissingData    int
	CorruptMeta    int
	BadRanges      int
	ChecksumErrors int
	Errors         []error
}

// ShardKey hashes key with MD5 and optionally applies directory sharding.
func ShardKey(key string, level int) string {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	if level <= 0 {
		return hash
	}
	var b strings.Builder
	for i := 0; i < level && i*2+2 <= len(hash); i++ {
		if i > 0 {
			// path separator already appended by previous iteration
		}
		b.WriteString(hash[i*2 : i*2+2])
		b.WriteByte(os.PathSeparator)
	}
	b.WriteString(hash)
	return b.String()
}

// New creates a Cache.  The returned cache should be closed to stop background
// cleanup and cancel active downloads.
func New(ctx context.Context, opt Options) (*Cache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := mergeOptions(DefaultOptions(), opt)
	if err := validateOptions(&merged); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(merged.CacheDir, merged.DirMode); err != nil {
		return nil, fmt.Errorf("varc: create cache dir: %w", err)
	}
	cacheCtx, cancel := context.WithCancel(ctx)
	logger := merged.Logger
	if logger == nil {
		logger = noopLogger{}
	}
	c := &Cache{
		ctx:               cacheCtx,
		cancel:            cancel,
		dir:               merged.CacheDir,
		blockSize:         merged.BlockSize,
		chunkSize:         merged.ChunkSize,
		chunkStreams:      merged.ChunkStreams,
		maxInflightBytes:  merged.MaxInflightBytes,
		cacheMaxAge:       merged.CacheMaxAge,
		cacheMaxSize:      merged.CacheMaxSize,
		cacheMinFreeSpace: merged.CacheMinFreeSpace,
		pollInterval:      merged.CachePollInterval,
		readAhead:         merged.ReadAhead,
		fastFingerprint:   merged.FastFingerprint,
		handleCaching:     merged.HandleCaching,
		shardLevel:        merged.ShardLevel,
		logger:            logger,
		fileMode:          merged.FileMode,
		dirMode:           merged.DirMode,
		syncWrites:        merged.SyncWrites,
		readRetryCount:    merged.ReadRetryCount,
		readRetryDelay:    merged.ReadRetryDelay,
		verifyChecksum:    merged.VerifyChecksum,
		touchInterval:     merged.TouchInterval,
		sem:               make(chan struct{}, merged.ChunkStreams),
		states:            make(map[string]*entryState),
	}
	if merged.CleanOnStart {
		if _, err := c.Prune(ctx); err != nil {
			cancel()
			return nil, err
		}
	}
	if !merged.NoBackground && merged.CachePollInterval > 0 {
		c.wg.Add(1)
		go c.janitor()
	}
	return c, nil
}

func validateOptions(opt *Options) error {
	if opt.CacheDir == "" {
		return errors.New("varc: CacheDir is required")
	}
	if opt.BlockSize <= 0 {
		opt.BlockSize = mebi
	}
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 128 * mebi
	}
	if opt.ChunkSizeLimit > 0 && opt.ChunkSize > opt.ChunkSizeLimit {
		opt.ChunkSize = opt.ChunkSizeLimit
	}
	if opt.BlockSize > opt.ChunkSize {
		opt.BlockSize = opt.ChunkSize
	}
	if opt.ChunkSize%opt.BlockSize != 0 {
		opt.ChunkSize = roundUp(opt.ChunkSize, opt.BlockSize)
	}
	if opt.ChunkStreams <= 0 {
		opt.ChunkStreams = 4
	}
	if opt.ChunkStreams > 1024 {
		return fmt.Errorf("varc: ChunkStreams too high: %d", opt.ChunkStreams)
	}
	if opt.MaxInflightBytes <= 0 {
		opt.MaxInflightBytes = int64(opt.ChunkStreams) * opt.ChunkSize
	}
	if opt.ReadAhead < 0 {
		opt.ReadAhead = 0
	}
	if opt.ReadAhead > opt.ChunkSize {
		opt.ReadAhead = opt.ChunkSize
	}
	if opt.ShardLevel < 0 {
		opt.ShardLevel = 0
	}
	if opt.ShardLevel > 8 {
		return fmt.Errorf("varc: ShardLevel too high: %d", opt.ShardLevel)
	}
	if opt.FileMode == 0 {
		opt.FileMode = defaultFileMode
	}
	if opt.DirMode == 0 {
		opt.DirMode = defaultDirMode
	}
	if opt.ReadRetryCount < 0 {
		opt.ReadRetryCount = 0
	}
	if opt.ReadRetryDelay < 0 {
		opt.ReadRetryDelay = 0
	}
	if opt.TouchInterval == 0 {
		opt.TouchInterval = 10 * time.Second
	}
	return nil
}

// Open opens key from the local cache, filling cache misses from src.
//
// If src is nil, Open is cache-only: already cached ranges may be read, but
// missing ranges return ErrCacheMiss.  When src is non-nil, size must be known
// and non-negative.
func (c *Cache) Open(ctx context.Context, key string, size int64, src io.ReaderAt, opts ...OpenOption) (*Reader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	if key == "" {
		c.metricOpenErrors.Add(1)
		return nil, errors.New("varc: key is required")
	}
	var openOpt openOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&openOpt)
		}
	}
	if openOpt.cacheOnly {
		src = nil
	}
	if src != nil && size < 0 {
		c.metricOpenErrors.Add(1)
		return nil, errors.New("varc: size must be known when source is provided")
	}
	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	state := c.acquireState(path)
	state.mu.Lock()
	keepState := false
	defer func() {
		state.mu.Unlock()
		if !keepState {
			c.releaseState(state)
		}
	}()

	if err := os.MkdirAll(filepath.Dir(path), c.dirMode); err != nil {
		c.metricOpenErrors.Add(1)
		return nil, fmt.Errorf("varc: create cache key dir: %w", err)
	}
	if err := c.loadStateLocked(state); err != nil {
		c.metricOpenErrors.Add(1)
		return nil, err
	}

	metaExists := state.loaded && state.meta.Size >= 0
	dataExists := fileExists(path)
	now := time.Now()
	if src == nil {
		if !metaExists || !dataExists {
			c.metricOpenErrors.Add(1)
			return nil, fmt.Errorf("%w for %q", ErrCacheMiss, key)
		}
		size = state.meta.Size
	} else {
		stale := !metaExists || !dataExists
		if !stale {
			stale = shouldInvalidate(state.meta, size, openOpt)
		}
		if stale {
			c.cancelTasksLocked(state)
			_ = os.Remove(path)
			_ = os.Remove(path + ".meta")
			state.meta = cacheMeta{
				Version:     metaVersion,
				Key:         key,
				Size:        size,
				Fingerprint: openOpt.fingerprint,
				ModTime:     openOpt.modTime,
				CreatedAt:   now,
				UpdatedAt:   now,
				AccessedAt:  now,
				BlockSize:   c.blockSize,
				ChunkSize:   c.chunkSize,
				Attrs:       cloneStringMap(openOpt.attrs),
			}
			state.loaded = true
		} else {
			state.meta.Key = key
			state.meta.Size = size
			state.meta.Version = metaVersion
			state.meta.BlockSize = c.blockSize
			state.meta.ChunkSize = c.chunkSize
			state.meta.UpdatedAt = now
			state.meta.AccessedAt = now
			if openOpt.fingerprint != "" {
				state.meta.Fingerprint = openOpt.fingerprint
			}
			if !openOpt.modTime.IsZero() {
				state.meta.ModTime = openOpt.modTime
			}
			if len(openOpt.attrs) > 0 {
				if state.meta.Attrs == nil {
					state.meta.Attrs = make(map[string]string)
				}
				for k, v := range openOpt.attrs {
					state.meta.Attrs[k] = v
				}
			}
			state.meta.Ranges = normalizeRanges(state.meta.Ranges, state.meta.Size)
		}
	}
	if state.meta.Size < 0 {
		c.metricOpenErrors.Add(1)
		return nil, fmt.Errorf("varc: unknown size for %q", key)
	}
	if err := c.saveMetaLocked(state); err != nil {
		c.metricOpenErrors.Add(1)
		return nil, err
	}
	if !state.meta.ModTime.IsZero() && fileExists(path) {
		_ = os.Chtimes(path, state.meta.ModTime, state.meta.ModTime)
	}
	state.readers++
	readerCtx, cancel := context.WithCancel(ctx)
	r := &Reader{
		cache:     c,
		state:     state,
		key:       key,
		path:      path,
		meta:      state.meta,
		src:       src,
		ctx:       readerCtx,
		cancel:    cancel,
		cacheOnly: src == nil,
	}
	c.metricOpens.Add(1)
	keepState = true
	return r, nil
}

func shouldInvalidate(meta cacheMeta, size int64, opt openOptions) bool {
	if meta.Size != size {
		return true
	}
	if opt.strict && meta.Fingerprint != opt.fingerprint {
		return true
	}
	if opt.fingerprint != "" && meta.Fingerprint != opt.fingerprint {
		return true
	}
	if opt.fingerprint == "" && meta.Fingerprint == "" && meta.Size != size {
		return true
	}
	return false
}

func (c *Cache) acquireState(path string) *entryState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[path]
	if st == nil {
		st = &entryState{
			path:     path,
			metaPath: path + ".meta",
			tasks:    make(map[string]*downloadTask),
		}
		st.cond = sync.NewCond(&st.mu)
		c.states[path] = st
	}
	st.refs++
	return st
}

func (c *Cache) releaseState(st *entryState) {
	if st == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states[st.path] != st {
		return
	}
	if st.refs > 0 {
		st.refs--
	}
	c.removeIdleStateLocked(st)
}

func (c *Cache) maybeForgetState(st *entryState) {
	if st == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states[st.path] != st {
		return
	}
	c.removeIdleStateLocked(st)
}

func (c *Cache) removeIdleStateLocked(st *entryState) {
	if st.refs != 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	c.pruneTasksLocked(st)
	if st.readers == 0 && activeTasks(st.tasks) == 0 {
		delete(c.states, st.path)
	}
}

func (c *Cache) forgetState(path string) {
	c.mu.Lock()
	delete(c.states, path)
	c.mu.Unlock()
}

func (c *Cache) forgetStateIfMatch(st *entryState) {
	c.mu.Lock()
	if c.states[st.path] == st {
		delete(c.states, st.path)
	}
	c.mu.Unlock()
}

func (c *Cache) loadStateLocked(st *entryState) error {
	if st.loaded {
		return nil
	}
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

// Exists checks whether key has both a cache data file and a metadata file.
func (c *Cache) Exists(key string) bool {
	if c == nil || c.closed.Load() || key == "" {
		return false
	}
	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	return fileExists(path) && fileExists(path+".meta")
}

// Remove evicts one key from the cache and cancels active downloads for it.
func (c *Cache) Remove(key string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}
	if key == "" {
		return errors.New("varc: key is required")
	}
	path := filepath.Join(c.dir, ShardKey(key, c.shardLevel))
	return c.removePath(path)
}

func (c *Cache) removePath(path string) error {
	st := c.acquireState(path)
	defer c.releaseState(st)
	st.mu.Lock()
	c.cancelTasksLocked(st)
	bytes := dataFileSize(path)
	st.removed = true
	st.meta.Ranges = nil
	st.cond.Broadcast()
	st.mu.Unlock()
	var errs []error
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(path + ".meta"); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	c.metricEvictions.Add(1)
	c.metricEvictedBytes.Add(bytes)
	c.forgetStateIfMatch(st)
	return joinErrors(errs...)
}

// Close shuts down the cache, cancels active downloads, and waits for the
// background janitor/downloader goroutines to finish.
func (c *Cache) Close() error {
	if c == nil {
		return nil
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()
	c.mu.Lock()
	states := make([]*entryState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()
	for _, st := range states {
		st.mu.Lock()
		c.cancelTasksLocked(st)
		st.cond.Broadcast()
		st.mu.Unlock()
	}
	c.wg.Wait()
	return nil
}

// Metrics returns a point-in-time snapshot of cache counters.
func (c *Cache) Metrics() Metrics {
	if c == nil {
		return Metrics{}
	}
	c.mu.Lock()
	tracked := len(c.states)
	c.mu.Unlock()
	return Metrics{
		Opens:              c.metricOpens.Load(),
		OpenErrors:         c.metricOpenErrors.Load(),
		Reads:              c.metricReads.Load(),
		ReadBytes:          c.metricReadBytes.Load(),
		Hits:               c.metricHits.Load(),
		HitBytes:           c.metricHitBytes.Load(),
		Misses:             c.metricMisses.Load(),
		MissBytes:          c.metricMissBytes.Load(),
		SourceReads:        c.metricSourceReads.Load(),
		SourceReadBytes:    c.metricSourceReadBytes.Load(),
		DownloadErrors:     c.metricDownloadErrors.Load(),
		Evictions:          c.metricEvictions.Load(),
		EvictedBytes:       c.metricEvictedBytes.Load(),
		MetaWrites:         c.metricMetaWrites.Load(),
		BackgroundPrunes:   c.metricBackgroundPrunes.Load(),
		InflightBytes:      c.inflightByte.Load(),
		OpenTrackedEntries: tracked,
	}
}

// Stats returns a map compatible with older versions of this package.
func (c *Cache) Stats() map[string]interface{} {
	entries, bytesUsed := c.scanDataUsage()
	m := c.Metrics()
	return map[string]interface{}{
		"files":              entries,
		"bytesUsed":          bytesUsed,
		"opens":              m.Opens,
		"reads":              m.Reads,
		"readBytes":          m.ReadBytes,
		"hits":               m.Hits,
		"hitBytes":           m.HitBytes,
		"misses":             m.Misses,
		"missBytes":          m.MissBytes,
		"sourceReads":        m.SourceReads,
		"sourceReadBytes":    m.SourceReadBytes,
		"downloadErrors":     m.DownloadErrors,
		"evictions":          m.Evictions,
		"evictedBytes":       m.EvictedBytes,
		"metaWrites":         m.MetaWrites,
		"backgroundPrunes":   m.BackgroundPrunes,
		"inflightBytes":      m.InflightBytes,
		"openTrackedEntries": m.OpenTrackedEntries,
	}
}

func (c *Cache) scanDataUsage() (int, int64) {
	var files int
	var bytesUsed int64
	_ = filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(path, ".meta") || isTempFile(path) {
			return nil
		}
		files++
		if info, statErr := d.Info(); statErr == nil {
			bytesUsed += info.Size()
		}
		return nil
	})
	return files, bytesUsed
}

// Read implements io.Reader using the reader's current position.
func (r *Reader) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	n, err := r.readAtContextLocked(r.ctx, p, r.pos)
	r.pos += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	return r.readAtContextLocked(r.ctx, p, off)
}

// ReadAtContext reads at offset using ctx for waiting on cache misses and
// source downloads.  It is the preferred method for request-scoped servers.
func (r *Reader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	return r.readAtContextLocked(ctx, p, off)
}

func (r *Reader) readAtContextLocked(ctx context.Context, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", ErrInvalidRange, off)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	meta := r.currentMeta()
	if off >= meta.Size {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	finalErr := error(nil)
	if end > meta.Size {
		end = meta.Size
		finalErr = io.EOF
	}
	if err := r.ensureRange(ctx, off, end); err != nil {
		return 0, err
	}
	if r.cache.readAhead > 0 && end < meta.Size && r.src != nil {
		r.scheduleReadAhead(end, min64(meta.Size, end+r.cache.readAhead))
	}
	f, err := os.Open(r.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, readErr := readFullAt(f, p[:end-off], off)
	r.cache.metricReads.Add(1)
	r.cache.metricReadBytes.Add(int64(n))
	r.cache.metricHits.Add(1)
	r.cache.metricHitBytes.Add(int64(n))
	if n > 0 {
		r.touch(false)
	}
	if readErr != nil && readErr != io.EOF {
		return n, readErr
	}
	if finalErr != nil {
		return n, finalErr
	}
	return n, readErr
}

func (r *Reader) currentMeta() cacheMeta {
	r.state.mu.Lock()
	meta := r.state.meta
	r.state.mu.Unlock()
	return meta
}

func readFullAt(f *os.File, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var total int
	for total < len(p) {
		n, err := f.ReadAt(p[total:], off+int64(total))
		total += n
		if err != nil {
			if err == io.EOF && total == len(p) {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

// Seek implements io.Seeker.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	meta := r.currentMeta()
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		next = meta.Size + offset
	default:
		return r.pos, fmt.Errorf("varc: invalid whence %d", whence)
	}
	if next < 0 {
		return r.pos, fmt.Errorf("%w: negative seek position %d", ErrInvalidRange, next)
	}
	r.pos = next
	return r.pos, nil
}

// Close closes the reader and cancels downloaders that were only useful for it.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.readMu.Lock()
	if r.closed {
		r.readMu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel()
	r.readMu.Unlock()
	st := r.state
	st.mu.Lock()
	if st.readers > 0 {
		st.readers--
	}
	st.cond.Broadcast()
	st.mu.Unlock()
	r.cache.releaseState(st)
	return nil
}

// Size returns the logical size of the opened object.
func (r *Reader) Size() int64 { return r.currentMeta().Size }

// ModTime returns the upstream modification time when provided.
func (r *Reader) ModTime() time.Time { return r.currentMeta().ModTime }

// Fingerprint returns the fingerprint supplied at open time, if any.
func (r *Reader) Fingerprint() string { return r.currentMeta().Fingerprint }

// CachedRanges returns a snapshot of currently cached byte ranges.
func (r *Reader) CachedRanges() []byteRange { return cloneRanges(r.currentMeta().Ranges) }

// CachedBytes returns the number of logical bytes currently cached for this
// reader's entry.
func (r *Reader) CachedBytes() int64 { return rangesLen(r.currentMeta().Ranges) }

// Complete reports whether the whole object is cached.
func (r *Reader) Complete() bool {
	meta := r.currentMeta()
	return containsRange(meta.Ranges, 0, meta.Size)
}

func (r *Reader) ensureRange(ctx context.Context, start, end int64) error {
	if start < 0 || end < start {
		return ErrInvalidRange
	}
	if start == end {
		return nil
	}
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.cache.closed.Load() || st.removed {
			return ErrClosed
		}
		if err := r.cache.reloadMetaLocked(st); err != nil {
			return err
		}
		if !fileExists(st.path) {
			st.meta.Ranges = nil
		}
		if containsRange(st.meta.Ranges, start, end) {
			r.meta = st.meta
			return nil
		}
		missingStart, missingEnd, ok := firstMissingRange(st.meta.Ranges, start, end)
		if !ok {
			r.meta = st.meta
			return nil
		}
		r.cache.metricMisses.Add(1)
		r.cache.metricMissBytes.Add(missingEnd - missingStart)
		if r.src == nil || r.cacheOnly {
			return fmt.Errorf("%w for %q at %d-%d", ErrCacheMiss, r.key, missingStart, missingEnd-1)
		}
		r.cache.ensureTaskLocked(st, r.src, missingStart, missingEnd)
		if err := waitCond(ctx, st.cond); err != nil {
			return err
		}
		if st.lastError != nil && !containsRange(st.meta.Ranges, start, end) {
			err := st.lastError
			st.lastError = nil
			return err
		}
	}
}

func waitCond(ctx context.Context, cond *sync.Cond) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		case <-done:
		}
	}()
	cond.Wait()
	close(done)
	return ctx.Err()
}

func (r *Reader) scheduleReadAhead(start, end int64) {
	if end <= start {
		return
	}
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if containsRange(st.meta.Ranges, start, end) {
		return
	}
	if missingStart, missingEnd, ok := firstMissingRange(st.meta.Ranges, start, end); ok {
		r.cache.ensureTaskLocked(st, r.src, missingStart, missingEnd)
	}
}

func (r *Reader) touch(force bool) {
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	if !force && r.cache.touchInterval >= 0 && !st.lastTouch.IsZero() && now.Sub(st.lastTouch) < r.cache.touchInterval {
		return
	}
	st.lastTouch = now
	st.meta.AccessedAt = now
	st.meta.UpdatedAt = now
	_ = r.cache.saveMetaLocked(st)
}

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

func (c *Cache) ensureTaskLocked(st *entryState, src io.ReaderAt, start, end int64) {
	c.pruneTasksLocked(st)
	for _, t := range st.tasks {
		if !t.done && t.err == nil && t.start <= start && t.end >= end {
			return
		}
	}
	chunkStart := alignDown(start, c.blockSize)
	chunkEnd := chunkStart + c.chunkSize
	if chunkEnd < end {
		chunkEnd = roundUp(end, c.blockSize)
	}
	if chunkEnd > st.meta.Size {
		chunkEnd = st.meta.Size
	}
	key := rangeKey(chunkStart, chunkEnd)
	if existing := st.tasks[key]; existing != nil && !existing.done {
		return
	}
	taskCtx, cancel := context.WithCancel(c.ctx)
	t := &downloadTask{
		state:  st,
		cache:  c,
		src:    src,
		start:  chunkStart,
		end:    chunkEnd,
		offset: chunkStart,
		key:    key,
		ctx:    taskCtx,
		cancel: cancel,
	}
	st.tasks[key] = t
	c.wg.Add(1)
	go c.runDownloadTask(t)
}

func (c *Cache) runDownloadTask(t *downloadTask) {
	defer c.wg.Done()
	defer c.maybeForgetState(t.state)
	if err := c.acquire(t.ctx); err != nil {
		c.finishTask(t, err)
		return
	}
	defer c.release()
	for {
		if err := t.ctx.Err(); err != nil {
			c.finishTask(t, err)
			return
		}
		t.state.mu.Lock()
		if t.offset >= t.end || t.state.removed {
			t.done = true
			t.state.cond.Broadcast()
			t.state.mu.Unlock()
			return
		}
		start := t.offset
		end := min64(t.end, start+c.blockSize)
		// Skip ranges that another downloader completed while this task waited.
		if containsRange(t.state.meta.Ranges, start, end) {
			t.offset = end
			t.state.cond.Broadcast()
			t.state.mu.Unlock()
			continue
		}
		t.state.mu.Unlock()

		buf, err := c.readSourceBlock(t.ctx, t.src, start, end)
		if err != nil {
			c.finishTask(t, err)
			return
		}
		checksum := uint32(0)
		if c.verifyChecksum {
			checksum = crc32.ChecksumIEEE(buf)
		}
		t.state.mu.Lock()
		if t.state.removed {
			t.done = true
			t.state.cond.Broadcast()
			t.state.mu.Unlock()
			return
		}
		if err := c.writeCacheBlockLocked(t.state, buf, start); err != nil {
			t.state.mu.Unlock()
			c.finishTask(t, err)
			return
		}
		t.state.meta.Ranges = addRange(t.state.meta.Ranges, start, end)
		if c.verifyChecksum {
			t.state.meta.Checksums = addChecksum(t.state.meta.Checksums, blockChecksum{Start: start, End: end, CRC32: checksum})
		}
		t.state.meta.UpdatedAt = time.Now()
		if err := c.saveMetaLocked(t.state); err != nil {
			t.state.mu.Unlock()
			c.finishTask(t, err)
			return
		}
		t.offset = end
		t.state.cond.Broadcast()
		t.state.mu.Unlock()
	}
}

func (c *Cache) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	}
}

func (c *Cache) release() {
	select {
	case <-c.sem:
	default:
	}
}

func (c *Cache) readSourceBlock(ctx context.Context, src io.ReaderAt, start, end int64) ([]byte, error) {
	size := end - start
	if size < 0 || size > math.MaxInt32 {
		return nil, fmt.Errorf("%w: bad source block %d-%d", ErrInvalidRange, start, end)
	}
	if err := c.reserveInflight(ctx, size); err != nil {
		return nil, err
	}
	defer c.releaseInflight(size)
	buf := make([]byte, size)
	var lastErr error
	attempts := c.readRetryCount + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := src.ReadAt(buf, start)
		c.metricSourceReads.Add(1)
		c.metricSourceReadBytes.Add(int64(maxInt(n, 0)))
		if err == nil && n == len(buf) {
			return buf, nil
		}
		if err == io.EOF && n == len(buf) {
			return buf, nil
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		if err == io.EOF && n != len(buf) {
			err = io.ErrUnexpectedEOF
		}
		lastErr = err
		if attempt+1 < attempts && c.readRetryDelay > 0 {
			delay := time.Duration(attempt+1) * c.readRetryDelay
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return nil, fmt.Errorf("varc: source read %d-%d failed: %w", start, end-1, lastErr)
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

func (c *Cache) writeCacheBlockLocked(st *entryState, buf []byte, off int64) error {
	if err := os.MkdirAll(filepath.Dir(st.path), c.dirMode); err != nil {
		return err
	}
	f, err := os.OpenFile(st.path, os.O_CREATE|os.O_RDWR, c.fileMode)
	if err != nil {
		return err
	}
	if _, err = f.WriteAt(buf, off); err != nil {
		_ = f.Close()
		return err
	}
	if c.syncWrites {
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	if !st.meta.ModTime.IsZero() {
		_ = os.Chtimes(st.path, st.meta.ModTime, st.meta.ModTime)
	}
	return nil
}

func (c *Cache) finishTask(t *downloadTask, err error) {
	if err != nil {
		c.metricDownloadErrors.Add(1)
	}
	t.state.mu.Lock()
	t.err = err
	t.done = true
	if err != nil {
		t.state.lastError = err
	}
	t.state.cond.Broadcast()
	t.state.mu.Unlock()
}

func (c *Cache) pruneTasksLocked(st *entryState) {
	for k, t := range st.tasks {
		if t.done {
			delete(st.tasks, k)
		}
	}
}

func (c *Cache) cancelTasksLocked(st *entryState) {
	for _, t := range st.tasks {
		if !t.done {
			t.cancel()
		}
	}
	st.cond.Broadcast()
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
	if st.meta.BlockSize <= 0 {
		st.meta.BlockSize = c.blockSize
	}
	if st.meta.ChunkSize <= 0 {
		st.meta.ChunkSize = c.chunkSize
	}
	if err := saveMeta(st.metaPath, st.meta, c.dirMode, c.syncWrites); err != nil {
		return err
	}
	c.metricMetaWrites.Add(1)
	return nil
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
		// v1 compatibility: older metadata did not carry Version, BlockSize, or
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
		return info
	}
	info.MetadataOK = ok
	if ok {
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

// Prune performs age, size, invalid-entry, and free-space cleanup.  It is safe
// to call while readers are active; active entries are skipped unless their
// metadata is invalid and no data file exists.
func (c *Cache) Prune(ctx context.Context) (PruneStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return PruneStats{}, ErrClosed
	}
	var stats PruneStats
	entries, err := c.ListEntries(ctx)
	if err != nil {
		stats.Errors = append(stats.Errors, err)
	}
	stats.Scanned = len(entries)
	stats.BytesBefore = sumEntryBytes(entries)
	stats.FreeBefore = diskFree(c.dir)
	now := time.Now()
	var survivors []EntryInfo
	for _, e := range entries {
		if ctx.Err() != nil {
			stats.Errors = append(stats.Errors, ctx.Err())
			break
		}
		if e.MetadataErr != nil {
			stats.ReasonInvalid++
			c.removeEntryInfo(e, &stats)
			continue
		}
		if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
			survivors = append(survivors, e)
			continue
		}
		if c.cacheMaxAge > 0 && !e.AccessedAt.IsZero() && now.Sub(e.AccessedAt) > c.cacheMaxAge {
			stats.ReasonAge++
			c.removeEntryInfo(e, &stats)
			continue
		}
		survivors = append(survivors, e)
	}
	bytes := sumEntryBytes(survivors)
	if c.cacheMaxSize > 0 && bytes > c.cacheMaxSize {
		sort.Slice(survivors, func(i, j int) bool { return survivors[i].AccessedAt.Before(survivors[j].AccessedAt) })
		kept := survivors[:0]
		for _, e := range survivors {
			if bytes <= c.cacheMaxSize {
				kept = append(kept, e)
				continue
			}
			if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
				kept = append(kept, e)
				continue
			}
			stats.ReasonSize++
			c.removeEntryInfo(e, &stats)
			bytes -= e.DataBytes
		}
		survivors = kept
	}
	if c.cacheMinFreeSpace > 0 {
		free := diskFree(c.dir)
		if free >= 0 && free < c.cacheMinFreeSpace {
			sort.Slice(survivors, func(i, j int) bool { return survivors[i].AccessedAt.Before(survivors[j].AccessedAt) })
			for _, e := range survivors {
				if free >= c.cacheMinFreeSpace {
					break
				}
				if e.OpenReaders > 0 || e.ActiveFetches > 0 || e.Pinned {
					continue
				}
				stats.ReasonFree++
				c.removeEntryInfo(e, &stats)
				free = diskFree(c.dir)
			}
		}
	}
	_, after := c.scanDataUsage()
	stats.BytesAfter = after
	stats.FreeAfter = diskFree(c.dir)
	return stats, joinErrors(stats.Errors...)
}

func (c *Cache) removeEntryInfo(e EntryInfo, stats *PruneStats) {
	if e.Path == "" && e.MetaPath != "" {
		e.Path = strings.TrimSuffix(e.MetaPath, ".meta")
	}
	bytes := e.DataBytes
	if bytes == 0 {
		bytes = dataFileSize(e.Path)
	}
	if err := c.removePath(e.Path); err != nil {
		stats.Errors = append(stats.Errors, err)
		return
	}
	stats.Removed++
	stats.RemovedBytes += bytes
}

func (c *Cache) janitor() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, c.pollInterval)
			_, err := c.Prune(ctx)
			cancel()
			c.metricBackgroundPrunes.Add(1)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
				c.logger.Warnf("varc: background prune failed: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Verify checks metadata/data consistency and optionally block checksums.
func (c *Cache) Verify(ctx context.Context) (VerifyStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := c.ListEntries(ctx)
	stats := VerifyStats{Entries: len(entries)}
	if err != nil {
		stats.Errors = append(stats.Errors, err)
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			stats.Errors = append(stats.Errors, ctx.Err())
			break
		}
		if e.MetadataErr != nil {
			stats.CorruptMeta++
			stats.Errors = append(stats.Errors, e.MetadataErr)
			continue
		}
		if !e.OnDisk {
			stats.MissingData++
			continue
		}
		if !rangesValid(e.Ranges, e.Size) {
			stats.BadRanges++
			continue
		}
		if e.Complete {
			stats.Complete++
		} else {
			stats.Incomplete++
		}
		if c.verifyChecksum {
			meta, ok, err := loadMeta(e.MetaPath)
			if err != nil || !ok {
				stats.ChecksumErrors++
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				}
				continue
			}
			if err := verifyChecksums(e.Path, meta.Checksums); err != nil {
				stats.ChecksumErrors++
				stats.Errors = append(stats.Errors, err)
			}
		}
	}
	return stats, joinErrors(stats.Errors...)
}

func verifyChecksums(path string, sums []blockChecksum) error {
	if len(sums) == 0 {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, mebi)
	for _, s := range sums {
		if s.End < s.Start {
			return fmt.Errorf("varc: bad checksum range %d-%d", s.Start, s.End)
		}
		need := s.End - s.Start
		if int64(len(buf)) < need {
			buf = make([]byte, need)
		}
		n, err := readFullAt(f, buf[:need], s.Start)
		if err != nil {
			return err
		}
		if int64(n) != need {
			return io.ErrUnexpectedEOF
		}
		got := crc32.ChecksumIEEE(buf[:need])
		if got != s.CRC32 {
			return fmt.Errorf("varc: checksum mismatch %s %d-%d", path, s.Start, s.End)
		}
	}
	return nil
}

// WarmRange schedules and waits for a range to be cached.  It is useful for
// mounting layers that want to pre-buffer file headers, media indexes, or small
// sidecar objects before serving a request.
func (r *Reader) WarmRange(ctx context.Context, start, end int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if start < 0 || end < start {
		return ErrInvalidRange
	}
	meta := r.currentMeta()
	if start > meta.Size {
		return io.EOF
	}
	if end > meta.Size {
		end = meta.Size
	}
	return r.ensureRange(ctx, start, end)
}

// WarmAll downloads the entire object into the cache.
func (r *Reader) WarmAll(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return r.WarmRange(ctx, 0, r.Size())
}

// CopyTo writes the object to dst using the cache.  Missing ranges are fetched
// and persisted along the way.
func (r *Reader) CopyTo(ctx context.Context, dst io.Writer) (int64, error) {
	if dst == nil {
		return 0, errors.New("varc: nil writer")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	buf := make([]byte, min64(4*mebi, max64(r.cache.blockSize, 64*kibi)))
	var off int64
	var total int64
	for off < r.Size() {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		need := min64(int64(len(buf)), r.Size()-off)
		n, err := r.ReadAtContext(ctx, buf[:need], off)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
			if wn != n {
				return total, io.ErrShortWrite
			}
			off += int64(n)
		}
		if err != nil {
			if err == io.EOF && off >= r.Size() {
				break
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

// Attr returns a metadata attribute stored at Open time.
func (r *Reader) Attr(key string) (string, bool) {
	meta := r.currentMeta()
	v, ok := meta.Attrs[key]
	return v, ok
}

// SetAttr sets or updates a metadata attribute.  It does not affect cached data.
func (r *Reader) SetAttr(key, value string) error {
	if key == "" {
		return errors.New("varc: empty attribute key")
	}
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.meta.Attrs == nil {
		st.meta.Attrs = make(map[string]string)
	}
	st.meta.Attrs[key] = value
	st.meta.UpdatedAt = time.Now()
	return r.cache.saveMetaLocked(st)
}

// RemoveAttr removes a metadata attribute.
func (r *Reader) RemoveAttr(key string) error {
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.meta.Attrs != nil {
		delete(st.meta.Attrs, key)
	}
	st.meta.UpdatedAt = time.Now()
	return r.cache.saveMetaLocked(st)
}

func normalizeRanges(ranges []byteRange, size int64) []byteRange {
	if len(ranges) == 0 || size < 0 {
		return nil
	}
	clean := make([]byteRange, 0, len(ranges))
	for _, r := range ranges {
		if r.End <= r.Start || r.End <= 0 || r.Start >= size {
			continue
		}
		if r.Start < 0 {
			r.Start = 0
		}
		if r.End > size {
			r.End = size
		}
		clean = append(clean, r)
	}
	if len(clean) == 0 {
		return nil
	}
	sort.Slice(clean, func(i, j int) bool {
		if clean[i].Start == clean[j].Start {
			return clean[i].End < clean[j].End
		}
		return clean[i].Start < clean[j].Start
	})
	merged := clean[:0]
	for _, r := range clean {
		if len(merged) == 0 || r.Start > merged[len(merged)-1].End {
			merged = append(merged, r)
			continue
		}
		if r.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = r.End
		}
	}
	return append([]byteRange(nil), merged...)
}

func addRange(ranges []byteRange, start, end int64) []byteRange {
	if end <= start {
		return normalizeRanges(ranges, math.MaxInt64)
	}
	ranges = append(ranges, byteRange{Start: start, End: end})
	return normalizeRanges(ranges, math.MaxInt64)
}

func containsRange(ranges []byteRange, start, end int64) bool {
	if end <= start {
		return true
	}
	for _, r := range ranges {
		if r.Start <= start && r.End >= end {
			return true
		}
		if r.Start > start {
			return false
		}
	}
	return false
}

func firstMissingRange(ranges []byteRange, start, end int64) (int64, int64, bool) {
	if end <= start {
		return 0, 0, false
	}
	pos := start
	sorted := normalizeRanges(ranges, math.MaxInt64)
	for _, r := range sorted {
		if r.End <= pos {
			continue
		}
		if r.Start > pos {
			return pos, min64(r.Start, end), true
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

func missingRanges(ranges []byteRange, start, end int64) []byteRange {
	var out []byteRange
	pos := start
	sorted := normalizeRanges(ranges, math.MaxInt64)
	for _, r := range sorted {
		if r.End <= pos {
			continue
		}
		if r.Start > pos {
			out = append(out, byteRange{Start: pos, End: min64(r.Start, end)})
		}
		if r.End > pos {
			pos = r.End
		}
		if pos >= end {
			break
		}
	}
	if pos < end {
		out = append(out, byteRange{Start: pos, End: end})
	}
	return out
}

func rangesLen(ranges []byteRange) int64 {
	var n int64
	for _, r := range normalizeRanges(ranges, math.MaxInt64) {
		if r.End > r.Start {
			n += r.End - r.Start
		}
	}
	return n
}

func rangesValid(ranges []byteRange, size int64) bool {
	prev := int64(0)
	first := true
	for _, r := range ranges {
		if r.Start < 0 || r.End < r.Start || r.End > size {
			return false
		}
		if !first && r.Start < prev {
			return false
		}
		prev = r.End
		first = false
	}
	return true
}

func cloneRanges(ranges []byteRange) []byteRange {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]byteRange, len(ranges))
	copy(out, ranges)
	return out
}

func addChecksum(sums []blockChecksum, next blockChecksum) []blockChecksum {
	out := sums[:0]
	for _, s := range sums {
		if s.Start == next.Start && s.End == next.End {
			continue
		}
		out = append(out, s)
	}
	out = append(out, next)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func rangeKey(start, end int64) string { return fmt.Sprintf("%d-%d", start, end) }

func alignDown(n, block int64) int64 {
	if block <= 0 {
		return n
	}
	return n - n%block
}

func roundUp(n, block int64) int64 {
	if block <= 0 || n%block == 0 {
		return n
	}
	return n + block - n%block
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dataFileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

func isTempFile(path string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, ".tmp") || strings.HasSuffix(base, "~")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sumEntryBytes(entries []EntryInfo) int64 {
	var n int64
	for _, e := range entries {
		n += e.DataBytes
	}
	return n
}

func diskFree(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

func joinErrors(errs ...error) error {
	var parts []string
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return errors.New(strings.Join(parts, "; "))
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
	if override.MaxInflightBytes != 0 {
		defaults.MaxInflightBytes = override.MaxInflightBytes
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
	if override.ShardLevel != 0 {
		defaults.ShardLevel = override.ShardLevel
	}
	if override.Logger != nil {
		defaults.Logger = override.Logger
	}
	if override.FileMode != 0 {
		defaults.FileMode = override.FileMode
	}
	if override.DirMode != 0 {
		defaults.DirMode = override.DirMode
	}
	if override.SyncWrites {
		defaults.SyncWrites = true
	}
	if override.NoBackground {
		defaults.NoBackground = true
	}
	if override.CleanOnStart {
		defaults.CleanOnStart = true
	}
	if override.ReadRetryCount != 0 {
		defaults.ReadRetryCount = override.ReadRetryCount
	}
	if override.ReadRetryDelay != 0 {
		defaults.ReadRetryDelay = override.ReadRetryDelay
	}
	if override.VerifyChecksum {
		defaults.VerifyChecksum = true
	}
	if override.TouchInterval != 0 {
		defaults.TouchInterval = override.TouchInterval
	}
	return defaults
}

// CacheDir returns the cache root directory.
func (c *Cache) CacheDir() string { return c.dir }

// BlockSize returns the normalized block size.
func (c *Cache) BlockSize() int64 { return c.blockSize }

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
			if activeTasks(st.tasks) == 0 {
				return ErrCacheMiss
			}
		} else if containsRange(st.meta.Ranges, 0, st.meta.Size) {
			return nil
		}
		if activeTasks(st.tasks) == 0 {
			return ErrCacheMiss
		}
		if err := waitCond(ctx, st.cond); err != nil {
			return err
		}
	}
}
