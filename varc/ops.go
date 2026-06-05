package varc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Range is an exported alias for varc byte ranges. End is exclusive.
// It lets external callers build WarmJob ranges while preserving backwards
// compatibility with existing metadata fields.
type Range = byteRange

const (
	attrPinned       = "varc.pinned"
	attrPinnedAt     = "varc.pinned_at"
	attrLastRepair   = "varc.last_repair_at"
	attrImportedAt   = "varc.imported_at"
	attrWarmClass    = "varc.warm_class"
	manifestVersion  = 1
	defaultWarmQueue = 64
)

// RangeSegment is one contiguous cached or missing segment inside a requested
// range.  End is exclusive.
type RangeSegment struct {
	Start  int64 `json:"start"`
	End    int64 `json:"end"`
	Cached bool  `json:"cached"`
}

// Length returns the segment length in bytes.
func (s RangeSegment) Length() int64 {
	if s.End <= s.Start {
		return 0
	}
	return s.End - s.Start
}

// RangePlan describes exactly what varc can serve locally and what would need
// to be fetched from the source for a byte range.  It is a pure metadata/data
// inspection and never contacts the upstream source.
type RangePlan struct {
	Key          string         `json:"key"`
	Size         int64          `json:"size"`
	Start        int64          `json:"start"`
	End          int64          `json:"end"`
	CachedBytes  int64          `json:"cached_bytes"`
	MissingBytes int64          `json:"missing_bytes"`
	CachedRanges []byteRange    `json:"cached_ranges,omitempty"`
	Missing      []byteRange    `json:"missing,omitempty"`
	Segments     []RangeSegment `json:"segments,omitempty"`
	Complete     bool           `json:"complete"`
	OnDisk       bool           `json:"on_disk"`
	Pinned       bool           `json:"pinned"`
	Fingerprint  string         `json:"fingerprint,omitempty"`
	ModTime      time.Time      `json:"mod_time,omitempty"`
	PlannedAt    time.Time      `json:"planned_at"`
}

// NeedFetch reports whether the planned range has any holes.
func (p RangePlan) NeedFetch() bool { return p.MissingBytes > 0 }

// RangeLength returns End-Start, clamped at zero.
func (p RangePlan) RangeLength() int64 {
	if p.End <= p.Start {
		return 0
	}
	return p.End - p.Start
}

// CoveragePercent returns local coverage for the planned byte range.
func (p RangePlan) CoveragePercent() float64 {
	length := p.RangeLength()
	if length <= 0 {
		return 100
	}
	return float64(p.CachedBytes) * 100 / float64(length)
}

// Plan returns a cache-only range plan for key.  The end offset is exclusive;
// use end < 0 to plan through the known object size.  It is safe to call from
// request hot paths because it does not open the remote source.
func (c *Cache) Plan(ctx context.Context, key string, start, end int64, opts ...OpenOption) (RangePlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return RangePlan{}, ErrClosed
	}
	if key == "" {
		return RangePlan{}, errors.New("varc: key is required")
	}
	if err := ctx.Err(); err != nil {
		return RangePlan{}, err
	}
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return RangePlan{}, err
	}
	if !ok {
		return RangePlan{}, ErrCacheMiss
	}
	var openOpt openOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&openOpt)
		}
	}
	if shouldInvalidate(meta, meta.Size, openOpt) {
		return RangePlan{}, ErrCacheMiss
	}
	return planFromMeta(key, c.KeyPath(key), meta, start, end)
}

// PlanRange returns a range plan for this open reader without touching the
// upstream source.
func (r *Reader) PlanRange(ctx context.Context, start, end int64) (RangePlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return RangePlan{}, ErrClosed
	}
	r.readMu.Lock()
	closed := r.closed
	r.readMu.Unlock()
	if closed {
		return RangePlan{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return RangePlan{}, err
	}
	return planFromMeta(r.key, r.path, r.currentMeta(), start, end)
}

func planFromMeta(key, path string, meta cacheMeta, start, end int64) (RangePlan, error) {
	if start < 0 {
		return RangePlan{}, ErrInvalidRange
	}
	if end < 0 || end > meta.Size {
		end = meta.Size
	}
	if end < start {
		return RangePlan{}, ErrInvalidRange
	}
	onDisk := fileExists(path)
	ranges := normalizeRanges(meta.Ranges, meta.Size)
	if !onDisk {
		// Metadata without a data file is useful for inspection/import flows, but
		// it must never be reported as locally readable coverage.
		ranges = nil
	}
	plan := RangePlan{
		Key:          key,
		Size:         meta.Size,
		Start:        start,
		End:          end,
		CachedRanges: intersectRanges(ranges, start, end),
		Missing:      missingRanges(ranges, start, end),
		Complete:     onDisk && containsRange(ranges, 0, meta.Size),
		OnDisk:       onDisk,
		Pinned:       isPinnedAttrs(meta.Attrs),
		Fingerprint:  meta.Fingerprint,
		ModTime:      meta.ModTime,
		PlannedAt:    time.Now(),
	}
	plan.CachedBytes = rangesLen(plan.CachedRanges)
	plan.MissingBytes = rangesLen(plan.Missing)
	plan.Segments = buildSegments(plan.CachedRanges, plan.Missing, start, end)
	return plan, nil
}

func intersectRanges(ranges []byteRange, start, end int64) []byteRange {
	if end <= start {
		return nil
	}
	out := make([]byteRange, 0, len(ranges))
	for _, r := range normalizeRanges(ranges, end) {
		lo := max64(start, r.Start)
		hi := min64(end, r.End)
		if hi > lo {
			out = append(out, byteRange{Start: lo, End: hi})
		}
		if r.Start >= end {
			break
		}
	}
	return out
}

func buildSegments(cached, missing []byteRange, start, end int64) []RangeSegment {
	if end <= start {
		return nil
	}
	items := make([]RangeSegment, 0, len(cached)+len(missing))
	for _, r := range cached {
		items = append(items, RangeSegment{Start: r.Start, End: r.End, Cached: true})
	}
	for _, r := range missing {
		items = append(items, RangeSegment{Start: r.Start, End: r.End, Cached: false})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Start == items[j].Start {
			if items[i].End == items[j].End {
				return items[i].Cached && !items[j].Cached
			}
			return items[i].End < items[j].End
		}
		return items[i].Start < items[j].Start
	})
	return items
}

// Pin marks an entry as protected from age/size/free-space pruning.  Explicit
// Remove still evicts it.  The marker is stored in metadata, so it survives
// restarts and works across processes that respect varc metadata.
func (c *Cache) Pin(ctx context.Context, key string) error {
	return c.updateAttrs(ctx, key, func(attrs map[string]string) error {
		attrs[attrPinned] = "true"
		attrs[attrPinnedAt] = time.Now().UTC().Format(time.RFC3339Nano)
		return nil
	})
}

// Unpin removes eviction protection from an entry.
func (c *Cache) Unpin(ctx context.Context, key string) error {
	return c.updateAttrs(ctx, key, func(attrs map[string]string) error {
		delete(attrs, attrPinned)
		delete(attrs, attrPinnedAt)
		return nil
	})
}

// IsPinned reports whether key is protected from Prune.
func (c *Cache) IsPinned(key string) (bool, error) {
	if key == "" {
		return false, errors.New("varc: key is required")
	}
	meta, ok, err := loadMeta(c.MetaPath(key))
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrCacheMiss
	}
	return isPinnedAttrs(meta.Attrs), nil
}

func (c *Cache) updateAttrs(ctx context.Context, key string, fn func(map[string]string) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return ErrClosed
	}
	if key == "" {
		return errors.New("varc: key is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := c.KeyPath(key)
	st := c.getState(path)
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := c.reloadMetaLocked(st); err != nil {
		return err
	}
	if !st.loaded || !fileExists(path) {
		return ErrCacheMiss
	}
	if st.meta.Attrs == nil {
		st.meta.Attrs = make(map[string]string)
	}
	if err := fn(st.meta.Attrs); err != nil {
		return err
	}
	if len(st.meta.Attrs) == 0 {
		st.meta.Attrs = nil
	}
	st.meta.UpdatedAt = time.Now()
	return c.saveMetaLocked(st)
}

func isPinnedAttrs(attrs map[string]string) bool {
	if len(attrs) == 0 {
		return false
	}
	v := strings.TrimSpace(strings.ToLower(attrs[attrPinned]))
	return v == "1" || v == "true" || v == "yes" || v == "on" || v == "pinned"
}

// WarmJob describes one object/range set to prefetch.  If Ranges is empty, the
// whole object is warmed.  OpenOptions can include WithFingerprint, WithModTime,
// and WithAttr just like Cache.Open.
type WarmJob struct {
	Key         string
	Size        int64
	Source      io.ReaderAt
	Ranges      []byteRange
	OpenOptions []OpenOption
}

// WarmOptions controls batch warming.
type WarmOptions struct {
	Concurrency int
	Queue       int
	Class       string
	StopOnError bool
}

// WarmResult is returned for every attempted WarmJob.
type WarmResult struct {
	Key          string
	Ranges       []byteRange
	WarmedBytes  int64
	SkippedBytes int64
	StartedAt    time.Time
	FinishedAt   time.Time
	Err          error
}

// WarmBatch warms many objects with bounded concurrency.  It is intended for
// mount startup, media index prebuffering, and admin-triggered cache promotion.
// It never creates unbounded goroutines and exits when ctx is canceled.
func (c *Cache) WarmBatch(ctx context.Context, jobs []WarmJob, opt WarmOptions) ([]WarmResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	if opt.Concurrency <= 0 {
		opt.Concurrency = c.chunkStreams
	}
	if opt.Concurrency <= 0 {
		opt.Concurrency = 1
	}
	if opt.Queue <= 0 {
		opt.Queue = defaultWarmQueue
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	in := make(chan int, opt.Queue)
	results := make([]WarmResult, len(jobs))
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	worker := func() {
		defer wg.Done()
		for idx := range in {
			res := c.runWarmJob(ctx, jobs[idx], opt)
			results[idx] = res
			if res.Err != nil && opt.StopOnError {
				once.Do(func() {
					firstErr = res.Err
					cancel()
				})
			}
		}
	}
	for i := 0; i < opt.Concurrency; i++ {
		wg.Add(1)
		go worker()
	}
sendLoop:
	for i := range jobs {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		select {
		case in <- i:
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			break sendLoop
		}
	}
	close(in)
	wg.Wait()
	if firstErr != nil {
		return results, firstErr
	}
	var errs []error
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Key, r.Err))
		}
	}
	return results, joinErrors(errs...)
}

func (c *Cache) runWarmJob(ctx context.Context, job WarmJob, opt WarmOptions) WarmResult {
	res := WarmResult{Key: job.Key, StartedAt: time.Now()}
	defer func() { res.FinishedAt = time.Now() }()
	if job.Key == "" {
		res.Err = errors.New("varc: warm job key is required")
		return res
	}
	if job.Source == nil {
		res.Err = ErrSourceRequired
		return res
	}
	if job.Size < 0 {
		res.Err = errors.New("varc: warm job size must be known")
		return res
	}
	opts := append([]OpenOption(nil), job.OpenOptions...)
	if opt.Class != "" {
		opts = append(opts, WithAttr(attrWarmClass, opt.Class))
	}
	r, err := c.Open(ctx, job.Key, job.Size, job.Source, opts...)
	if err != nil {
		res.Err = err
		return res
	}
	defer r.Close()
	ranges := normalizeWarmRanges(job.Ranges, job.Size)
	if len(ranges) == 0 {
		ranges = []byteRange{{Start: 0, End: job.Size}}
	}
	res.Ranges = cloneRanges(ranges)
	for _, rr := range ranges {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res
		}
		plan, err := r.PlanRange(ctx, rr.Start, rr.End)
		if err != nil {
			res.Err = err
			return res
		}
		res.SkippedBytes += plan.CachedBytes
		if plan.MissingBytes == 0 {
			continue
		}
		if err := r.WarmRange(ctx, rr.Start, rr.End); err != nil {
			res.Err = err
			return res
		}
		res.WarmedBytes += plan.MissingBytes
	}
	return res
}

func normalizeWarmRanges(in []byteRange, size int64) []byteRange {
	if size < 0 {
		return nil
	}
	if len(in) == 0 {
		return nil
	}
	return normalizeRanges(in, size)
}

// Manifest is a portable metadata snapshot.  It intentionally excludes cache
// data bytes so it stays small and safe for admin APIs.  ImportManifest can
// restore sidecars when data files are already present or will be populated
// later.
type Manifest struct {
	Version   int             `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	CacheDir  string          `json:"cache_dir,omitempty"`
	Entries   []ManifestEntry `json:"entries"`
}

// ManifestEntry is the serializable form of cache metadata.
type ManifestEntry struct {
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

// ExportManifest writes a JSON metadata manifest for all readable entries.
func (c *Cache) ExportManifest(ctx context.Context, w io.Writer) error {
	if w == nil {
		return errors.New("varc: nil manifest writer")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := c.ListEntries(ctx)
	if err != nil {
		return err
	}
	m := Manifest{Version: manifestVersion, CreatedAt: time.Now().UTC(), CacheDir: c.dir}
	for _, e := range entries {
		if e.MetadataErr != nil || !e.MetadataOK {
			continue
		}
		meta, ok, err := loadMeta(e.MetaPath)
		if err != nil || !ok {
			continue
		}
		m.Entries = append(m.Entries, manifestEntryFromMeta(meta))
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

func manifestEntryFromMeta(meta cacheMeta) ManifestEntry {
	return ManifestEntry{
		Key:         meta.Key,
		Size:        meta.Size,
		Fingerprint: meta.Fingerprint,
		ModTime:     meta.ModTime,
		CreatedAt:   meta.CreatedAt,
		UpdatedAt:   meta.UpdatedAt,
		AccessedAt:  meta.AccessedAt,
		BlockSize:   meta.BlockSize,
		ChunkSize:   meta.ChunkSize,
		Ranges:      cloneRanges(meta.Ranges),
		Attrs:       cloneStringMap(meta.Attrs),
		Checksums:   cloneChecksums(meta.Checksums),
	}
}

func (e ManifestEntry) toMeta() cacheMeta {
	return cacheMeta{
		Version:     metaVersion,
		Key:         e.Key,
		Size:        e.Size,
		Fingerprint: e.Fingerprint,
		ModTime:     e.ModTime,
		CreatedAt:   e.CreatedAt,
		UpdatedAt:   e.UpdatedAt,
		AccessedAt:  e.AccessedAt,
		BlockSize:   e.BlockSize,
		ChunkSize:   e.ChunkSize,
		Ranges:      cloneRanges(e.Ranges),
		Attrs:       cloneStringMap(e.Attrs),
		Checksums:   cloneChecksums(e.Checksums),
	}
}

// ImportOptions controls manifest restoration.
type ImportOptions struct {
	Overwrite        bool
	RequireDataFiles bool
	MarkImported     bool
}

// ImportStats reports manifest import results.
type ImportStats struct {
	Entries  int
	Imported int
	Skipped  int
	Errors   []error
}

// ImportManifest restores metadata sidecars from a manifest.  It is useful when
// moving a cache directory, pre-seeding metadata before warming, or recovering
// sidecars after accidental deletion.  By default existing sidecars are left in
// place and missing data files are allowed.
func (c *Cache) ImportManifest(ctx context.Context, r io.Reader, opt ImportOptions) (ImportStats, error) {
	if r == nil {
		return ImportStats{}, errors.New("varc: nil manifest reader")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return ImportStats{}, ErrClosed
	}
	var m Manifest
	dec := json.NewDecoder(r)
	if err := dec.Decode(&m); err != nil {
		return ImportStats{}, err
	}
	if m.Version > manifestVersion {
		return ImportStats{}, fmt.Errorf("varc: unsupported manifest version %d", m.Version)
	}
	stats := ImportStats{Entries: len(m.Entries)}
	for _, e := range m.Entries {
		if err := ctx.Err(); err != nil {
			stats.Errors = append(stats.Errors, err)
			break
		}
		if e.Key == "" || e.Size < 0 {
			stats.Skipped++
			stats.Errors = append(stats.Errors, fmt.Errorf("varc: bad manifest entry key=%q size=%d", e.Key, e.Size))
			continue
		}
		path := c.KeyPath(e.Key)
		if opt.RequireDataFiles && !fileExists(path) {
			stats.Skipped++
			continue
		}
		if !opt.Overwrite && fileExists(path+".meta") {
			stats.Skipped++
			continue
		}
		meta := e.toMeta()
		if meta.BlockSize <= 0 {
			meta.BlockSize = c.blockSize
		}
		if meta.ChunkSize <= 0 {
			meta.ChunkSize = c.chunkSize
		}
		meta.Ranges = normalizeRanges(meta.Ranges, meta.Size)
		if opt.MarkImported {
			if meta.Attrs == nil {
				meta.Attrs = make(map[string]string)
			}
			meta.Attrs[attrImportedAt] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if err := validateMeta(meta); err != nil {
			stats.Skipped++
			stats.Errors = append(stats.Errors, err)
			continue
		}
		if err := saveMeta(path+".meta", meta, c.dirMode, c.syncWrites); err != nil {
			stats.Skipped++
			stats.Errors = append(stats.Errors, err)
			continue
		}
		c.forgetState(path)
		stats.Imported++
	}
	return stats, joinErrors(stats.Errors...)
}

// RepairOptions controls cache scrubbing.  DryRun reports what would be done
// without modifying files.
type RepairOptions struct {
	DryRun            bool
	RemoveCorruptMeta bool
	RemoveMissingData bool
	DropBadRanges     bool
	DropBadChecksums  bool
	TouchRepaired     bool
}

// RepairStats reports repair/scrub work.
type RepairStats struct {
	Scanned          int
	Repaired         int
	Removed          int
	CorruptMeta      int
	MissingData      int
	BadRanges        int
	ChecksumFailures int
	DryRun           bool
	Errors           []error
}

// Repair scans metadata/data pairs and optionally fixes common cache damage:
// corrupt sidecars, missing data files, ranges beyond file size, and checksum
// records that no longer match.  It does not contact the upstream source.
func (c *Cache) Repair(ctx context.Context, opt RepairOptions) (RepairStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.closed.Load() {
		return RepairStats{}, ErrClosed
	}
	stats := RepairStats{DryRun: opt.DryRun}
	var metaPaths []string
	walkErr := filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !d.IsDir() && strings.HasSuffix(path, ".meta") && !isTempFile(path) {
			metaPaths = append(metaPaths, path)
		}
		return nil
	})
	if walkErr != nil {
		stats.Errors = append(stats.Errors, walkErr)
	}
	stats.Scanned = len(metaPaths)
	for _, metaPath := range metaPaths {
		if err := ctx.Err(); err != nil {
			stats.Errors = append(stats.Errors, err)
			break
		}
		c.repairOne(metaPath, opt, &stats)
	}
	return stats, joinErrors(stats.Errors...)
}

func (c *Cache) repairOne(metaPath string, opt RepairOptions, stats *RepairStats) {
	dataPath := strings.TrimSuffix(metaPath, ".meta")
	meta, ok, err := loadMeta(metaPath)
	if err != nil || !ok {
		stats.CorruptMeta++
		if opt.RemoveCorruptMeta && !opt.DryRun {
			if rmErr := os.Remove(metaPath); rmErr != nil && !os.IsNotExist(rmErr) {
				stats.Errors = append(stats.Errors, rmErr)
			} else {
				stats.Removed++
			}
		}
		if err != nil {
			stats.Errors = append(stats.Errors, err)
		}
		return
	}
	if !fileExists(dataPath) {
		stats.MissingData++
		if opt.RemoveMissingData && !opt.DryRun {
			if rmErr := os.Remove(metaPath); rmErr != nil && !os.IsNotExist(rmErr) {
				stats.Errors = append(stats.Errors, rmErr)
			} else {
				stats.Removed++
				c.forgetState(dataPath)
			}
		}
		return
	}
	changed := false
	if !rangesValid(meta.Ranges, meta.Size) {
		stats.BadRanges++
		if opt.DropBadRanges {
			meta.Ranges = normalizeRanges(meta.Ranges, meta.Size)
			changed = true
		}
	}
	if opt.DropBadRanges {
		st, err := os.Stat(dataPath)
		if err == nil {
			trimmed := trimRangesToFile(meta.Ranges, st.Size())
			if !sameRanges(meta.Ranges, trimmed) {
				meta.Ranges = trimmed
				changed = true
			}
		}
	}
	if opt.DropBadChecksums && len(meta.Checksums) > 0 {
		good, dropped := filterGoodChecksums(dataPath, meta.Checksums)
		if dropped > 0 {
			stats.ChecksumFailures += dropped
			meta.Checksums = good
			changed = true
		}
	}
	if changed {
		stats.Repaired++
		if !opt.DryRun {
			if meta.Attrs == nil {
				meta.Attrs = make(map[string]string)
			}
			if opt.TouchRepaired {
				meta.Attrs[attrLastRepair] = time.Now().UTC().Format(time.RFC3339Nano)
			}
			meta.UpdatedAt = time.Now()
			meta.Ranges = normalizeRanges(meta.Ranges, meta.Size)
			if err := saveMeta(metaPath, meta, c.dirMode, c.syncWrites); err != nil {
				stats.Errors = append(stats.Errors, err)
			}
			c.forgetState(dataPath)
		}
	}
}

func trimRangesToFile(ranges []byteRange, fileSize int64) []byteRange {
	if fileSize <= 0 {
		return nil
	}
	out := make([]byteRange, 0, len(ranges))
	for _, r := range ranges {
		if r.Start >= fileSize {
			continue
		}
		if r.End > fileSize {
			r.End = fileSize
		}
		if r.End > r.Start {
			out = append(out, r)
		}
	}
	return normalizeRanges(out, fileSize)
}

func sameRanges(a, b []byteRange) bool {
	a = normalizeRanges(a, int64(^uint64(0)>>1))
	b = normalizeRanges(b, int64(^uint64(0)>>1))
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func filterGoodChecksums(path string, sums []blockChecksum) ([]blockChecksum, int) {
	if len(sums) == 0 {
		return nil, 0
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, len(sums)
	}
	defer f.Close()
	out := make([]blockChecksum, 0, len(sums))
	dropped := 0
	for _, s := range sums {
		if s.End <= s.Start || s.Start < 0 {
			dropped++
			continue
		}
		need := s.End - s.Start
		if need > 64*mebi {
			// Avoid accidental huge allocation from corrupt metadata.
			dropped++
			continue
		}
		buf := make([]byte, need)
		n, err := readFullAt(f, buf, s.Start)
		if err != nil || int64(n) != need {
			dropped++
			continue
		}
		if crc := checksumIEEE(buf); crc != s.CRC32 {
			dropped++
			continue
		}
		out = append(out, s)
	}
	return out, dropped
}

// CacheHealth is a cheap operational snapshot suitable for admin endpoints.
type CacheHealth struct {
	CacheDir      string    `json:"cache_dir"`
	Writable      bool      `json:"writable"`
	DiskFreeBytes int64     `json:"disk_free_bytes"`
	Entries       int       `json:"entries"`
	Complete      int       `json:"complete"`
	Incomplete    int       `json:"incomplete"`
	Pinned        int       `json:"pinned"`
	BytesUsed     int64     `json:"bytes_used"`
	OpenReaders   int       `json:"open_readers"`
	ActiveFetches int       `json:"active_fetches"`
	InflightBytes int64     `json:"inflight_bytes"`
	LastChecked   time.Time `json:"last_checked"`
	CheckError    string    `json:"check_error,omitempty"`
}

// Health returns a cache status summary without contacting upstream sources.
func (c *Cache) Health(ctx context.Context) CacheHealth {
	if ctx == nil {
		ctx = context.Background()
	}
	h := CacheHealth{CacheDir: c.dir, DiskFreeBytes: diskFree(c.dir), LastChecked: time.Now(), InflightBytes: c.inflightByte.Load()}
	test := filepath.Join(c.dir, ".varc-health-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	if err := os.WriteFile(test, []byte("ok"), 0o600); err == nil {
		h.Writable = true
		_ = os.Remove(test)
	} else {
		h.CheckError = err.Error()
	}
	entries, err := c.ListEntries(ctx)
	if err != nil && h.CheckError == "" {
		h.CheckError = err.Error()
	}
	h.Entries = len(entries)
	for _, e := range entries {
		h.BytesUsed += e.DataBytes
		h.OpenReaders += e.OpenReaders
		h.ActiveFetches += e.ActiveFetches
		if e.Pinned {
			h.Pinned++
		}
		if e.Complete {
			h.Complete++
		} else {
			h.Incomplete++
		}
	}
	return h
}

func cloneChecksums(in []blockChecksum) []blockChecksum {
	if len(in) == 0 {
		return nil
	}
	out := make([]blockChecksum, len(in))
	copy(out, in)
	return out
}

func checksumIEEE(buf []byte) uint32 {
	// Small wrapper kept here so repair code remains decoupled from the exact
	// checksum implementation used by the downloader metadata.
	return crc32.ChecksumIEEE(buf)
}
