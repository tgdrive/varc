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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	kibi int64 = 1024
	mebi int64 = 1024 * kibi
	gibi int64 = 1024 * mebi

	defaultFileMode os.FileMode = 0o600
	defaultDirMode  os.FileMode = 0o700

	metaVersion            = 2
	maxWriteBufferSize     = mebi
	initialWriteBufferSize = 4 * kibi
)

// RangeSource can stream an inclusive byte range in one backend request.
// Implement it in addition to io.ReaderAt to let varc fetch each cache chunk
// without buffering the complete chunk or issuing one request per write.
type RangeSource interface {
	OpenRange(context.Context, int64, int64) (io.ReadCloser, error)
}

type readerAtRangeSource struct{ io.ReaderAt }

func (s readerAtRangeSource) OpenRange(_ context.Context, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(s.ReaderAt, start, end-start+1)), nil
}

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

	// ChunkSize is the initial downloader window. Each window maps to one range
	// request and may grow during sequential reads.
	ChunkSize int64

	// ChunkSizeLimit caps per-reader sequential growth. Zero uses the default.
	// Set it equal to ChunkSize to keep requests fixed-size.
	ChunkSizeLimit int64

	// PreloadChunks is the number of adaptive chunks prepared after the active
	// read window. One is a balanced media-streaming default.
	PreloadChunks int

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

	// ReadIdleTimeout aborts and retries a source range that produces no bytes
	// for this duration. It is an idle timeout, not a total request timeout.
	ReadIdleTimeout time.Duration

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
		ChunkSize:         32 * mebi,
		ChunkSizeLimit:    128 * mebi,
		PreloadChunks:     1,
		CacheMaxAge:       0,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		CachePollInterval: time.Minute,
		HandleCaching:     5 * time.Second,
		ShardLevel:        2,
		FileMode:          defaultFileMode,
		DirMode:           defaultDirMode,
		ReadRetryCount:    2,
		ReadRetryDelay:    100 * time.Millisecond,
		ReadIdleTimeout:   30 * time.Second,
		TouchInterval:     10 * time.Second,
	}
}

// Cache is a sparse, read-through, range-addressed cache.
type Cache struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir               string
	chunkSize         int64
	chunkSizeLimit    int64
	preloadChunks     int
	maxInflightBytes  int64
	cacheMaxAge       time.Duration
	cacheMaxSize      int64
	cacheMinFreeSpace int64
	pollInterval      time.Duration
	fastFingerprint   bool
	handleCaching     time.Duration
	shardLevel        int
	logger            Logger
	fileMode          os.FileMode
	dirMode           os.FileMode
	syncWrites        bool
	readRetryCount    int
	readRetryDelay    time.Duration
	readIdleTimeout   time.Duration
	verifyChecksum    bool
	touchInterval     time.Duration

	schedulerMu   sync.Mutex
	schedulerCond *sync.Cond
	waitingTasks  []*downloadTask
	activeTasks   map[*entryState]*downloadTask
	nextTaskSeq   uint64

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
	cache      *Cache
	state      *entryState
	key        string
	path       string
	meta       cacheMeta
	src        io.ReaderAt
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64

	cacheOnly bool
	closed    atomic.Bool
	closeOnce sync.Once

	cursorMu  sync.Mutex
	pos       int64
	cursorGen uint64

	adaptiveMu sync.Mutex
	adaptive   adaptiveReadState
}

type entryState struct {
	path     string
	metaPath string

	mu      sync.Mutex
	cond    *sync.Cond
	changed chan struct{}

	meta       cacheMeta
	volatile   []byteRange
	loaded     bool
	tasks      map[string]*downloadTask
	lastError  error
	failureSeq uint64
	generation uint64
	readers    int
	refs       int
	lastTouch  time.Time
	removed    bool
}

type downloadTask struct {
	state *entryState
	cache *Cache
	src   io.ReaderAt
	start int64
	end   int64
	key   string

	ctx           context.Context
	cancel        context.CancelFunc
	priority      taskPriority
	sequence      uint64
	started       bool
	preloadOwners map[*Reader]struct{}
	file          *os.File // guarded by state.mu; lazily opened on the first cache write
	done          bool
	err           error
	generation    uint64
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
		chunkSize:         merged.ChunkSize,
		chunkSizeLimit:    merged.ChunkSizeLimit,
		preloadChunks:     merged.PreloadChunks,
		maxInflightBytes:  merged.ChunkSizeLimit,
		cacheMaxAge:       merged.CacheMaxAge,
		cacheMaxSize:      merged.CacheMaxSize,
		cacheMinFreeSpace: merged.CacheMinFreeSpace,
		pollInterval:      merged.CachePollInterval,
		fastFingerprint:   merged.FastFingerprint,
		handleCaching:     merged.HandleCaching,
		shardLevel:        merged.ShardLevel,
		logger:            logger,
		fileMode:          merged.FileMode,
		dirMode:           merged.DirMode,
		syncWrites:        merged.SyncWrites,
		readRetryCount:    merged.ReadRetryCount,
		readRetryDelay:    merged.ReadRetryDelay,
		readIdleTimeout:   merged.ReadIdleTimeout,
		verifyChecksum:    merged.VerifyChecksum,
		touchInterval:     merged.TouchInterval,
		states:            make(map[string]*entryState),
		activeTasks:       make(map[*entryState]*downloadTask),
	}
	c.schedulerCond = sync.NewCond(&c.schedulerMu)
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
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 32 * mebi
	}
	if opt.ChunkSizeLimit <= 0 {
		opt.ChunkSizeLimit = opt.ChunkSize
	}
	if opt.ChunkSizeLimit < opt.ChunkSize {
		opt.ChunkSizeLimit = opt.ChunkSize
	}
	if opt.PreloadChunks < 0 {
		opt.PreloadChunks = 0
	}
	if opt.PreloadChunks > 16 {
		return fmt.Errorf("varc: PreloadChunks too high: %d", opt.PreloadChunks)
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
	if opt.ReadIdleTimeout < 0 {
		opt.ReadIdleTimeout = 0
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
		stale := !metaExists || shouldInvalidate(state.meta, size, openOpt)
		if !stale && !dataExists && state.readers == 0 && activeTasks(state.tasks) == 0 {
			stale = true
		}
		if stale {
			c.cancelTasksLocked(state)
			state.generation++
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
				ChunkSize:   c.chunkSize,
				Attrs:       cloneStringMap(openOpt.attrs),
			}
			state.loaded = true
		} else {
			state.meta.Key = key
			state.meta.Size = size
			state.meta.Version = metaVersion
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
		cache:      c,
		state:      state,
		key:        key,
		path:       path,
		meta:       state.meta,
		src:        src,
		ctx:        readerCtx,
		cancel:     cancel,
		generation: state.generation,
		cacheOnly:  src == nil,
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
			changed:  make(chan struct{}),
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
	st.generation++
	bytes := dataFileSize(path)
	st.removed = true
	st.meta.Ranges = nil
	st.notifyLocked()
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
	c.schedulerMu.Lock()
	c.schedulerCond.Broadcast()
	c.schedulerMu.Unlock()
	c.mu.Lock()
	states := make([]*entryState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()
	for _, st := range states {
		st.mu.Lock()
		c.cancelTasksLocked(st)
		st.notifyLocked()
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

// Read implements io.Reader using the reader's current position. Blocking cache
// fills happen without holding cursorMu so Seek and Close remain responsive.
