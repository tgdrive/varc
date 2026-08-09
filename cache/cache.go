// Package cache implements a read-only sparse-file cache.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	diskspace "varc/internal/engine/diskspace"
	"varc/internal/engine/objectio"
	"varc/ranges"
	"varc/source"
)

// Cache owns cached objects and the background eviction loop.
type Cache struct {
	ctx    context.Context
	cancel context.CancelFunc
	src    source.Source
	opt    Options
	root   string
	data   string
	meta   string

	mu        sync.Mutex
	cond      *sync.Cond
	items     map[string]*Item
	wg        sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeErr  error

	kickerMu      sync.Mutex
	kick          chan struct{}
	cleanerKicked bool
	outOfSpace    bool
}

// Stats is a point-in-time view of cache usage.
type Stats struct {
	Items       int
	OpenItems   int
	CachedBytes int64
}

// New creates a standalone read cache rooted at dir.
func New(ctx context.Context, dir string, src source.Source, opt Options) (*Cache, error) {
	if ctx == nil {
		return nil, errors.New("cache: nil context")
	}
	if src == nil {
		return nil, errors.New("cache: nil source")
	}
	if dir == "" {
		return nil, errors.New("cache: empty cache directory")
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}

	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: resolve root: %w", err)
	}
	data := filepath.Join(root, "data")
	meta := filepath.Join(root, "meta")
	if err := os.MkdirAll(data, 0o700); err != nil {
		return nil, fmt.Errorf("cache: create data root: %w", err)
	}
	if err := os.MkdirAll(meta, 0o700); err != nil {
		return nil, fmt.Errorf("cache: create metadata root: %w", err)
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	cacheCtx = objectio.WithConfig(cacheCtx, objectio.Config{BufferSize: opt.BufferSize})
	c := &Cache{
		ctx:    cacheCtx,
		cancel: cancel,
		src:    src,
		opt:    opt,
		root:   root,
		data:   data,
		meta:   meta,
		items:  make(map[string]*Item),
	}
	c.cond = sync.NewCond(&c.mu)
	c.kick = make(chan struct{})
	if err := c.loadExisting(); err != nil {
		cancel()
		return nil, fmt.Errorf("cache: load existing cache: %w", err)
	}
	if opt.CachePollInterval > 0 {
		c.Clean()
		c.wg.Add(1)
		go c.cleaner()
	}
	return c, nil
}

func (c *Cache) loadExisting() error {
	var metaPaths []string
	if err := filepath.WalkDir(c.meta, func(metaPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(metaPath) != ".json" {
			return os.Remove(metaPath)
		}
		metaPaths = append(metaPaths, metaPath)
		return nil
	}); err != nil {
		return err
	}

	knownData := make(map[string]struct{}, len(metaPaths))
	for _, metaPath := range metaPaths {
		dataPath, err := c.loadExistingMeta(metaPath)
		if err != nil {
			return err
		}
		if dataPath != "" {
			knownData[dataPath] = struct{}{}
		}
	}

	return filepath.WalkDir(c.data, func(dataPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if _, ok := knownData[dataPath]; ok {
			return nil
		}
		return os.Remove(dataPath)
	})
}

func (c *Cache) loadExistingMeta(metaPath string) (string, error) {
	encoded, err := os.ReadFile(metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	var info itemInfo
	if err := json.Unmarshal(encoded, &info); err != nil || info.Key == "" || info.Size < 0 {
		return "", os.Remove(metaPath)
	}

	relMeta, err := filepath.Rel(c.meta, metaPath)
	if err != nil || relMeta == "." || filepath.Ext(relMeta) != ".json" {
		return "", fmt.Errorf("cache: invalid metadata path %q", metaPath)
	}
	actualDataPath := filepath.Join(c.data, relMeta[:len(relMeta)-len(".json")])
	stat, err := os.Stat(actualDataPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", os.Remove(metaPath)
	}
	if err != nil {
		return "", err
	}
	if !stat.Mode().IsRegular() || stat.Size() != info.Size {
		if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if err := os.Remove(actualDataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		return "", nil
	}

	expectedDataPath, expectedMetaPath := c.pathsForKey(info.Key)
	if filepath.Clean(actualDataPath) != filepath.Clean(expectedDataPath) || filepath.Clean(metaPath) != filepath.Clean(expectedMetaPath) {
		if _, err := os.Stat(expectedMetaPath); err == nil {
			_ = os.Remove(metaPath)
			_ = os.Remove(actualDataPath)
			return "", nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if _, err := os.Stat(expectedDataPath); err == nil {
			_ = os.Remove(metaPath)
			_ = os.Remove(actualDataPath)
			return "", nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(expectedDataPath), 0o700); err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(expectedMetaPath), 0o700); err != nil {
			return "", err
		}
		if err := os.Rename(actualDataPath, expectedDataPath); err != nil {
			return "", err
		}
		if err := os.Rename(metaPath, expectedMetaPath); err != nil {
			_ = os.Rename(expectedDataPath, actualDataPath)
			return "", err
		}
		actualDataPath = expectedDataPath
		metaPath = expectedMetaPath
	}

	info.Rs = info.Rs.Intersection(ranges.Range{Pos: 0, Size: info.Size})
	if info.ATime.IsZero() {
		info.ATime = stat.ModTime()
	}
	item := newItem(c, info.Key)
	item.info = info
	item.loaded = true
	c.items[info.Key] = item
	return actualDataPath, nil
}

// Open validates key against the source and returns a reader backed by the
// sparse local cache.
func (c *Cache) Open(ctx context.Context, key string) (*Reader, error) {
	if ctx == nil {
		return nil, errors.New("cache: nil open context")
	}
	if key == "" {
		return nil, errors.New("cache: empty key")
	}
	meta, err := c.src.Stat(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("cache: stat %q: %w", key, err)
	}
	if meta.Size < 0 {
		return nil, fmt.Errorf("cache: source %q has unknown size", key)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("cache: closed")
	}
	item := c.items[key]
	if item == nil {
		item = newItem(c, key)
		c.items[key] = item
	}
	item.mu.Lock()
	c.mu.Unlock()

	if err := item.openLocked(ctx, meta); err != nil {
		item.mu.Unlock()
		return nil, err
	}
	item.mu.Unlock()
	return &Reader{ctx: ctx, item: item, meta: meta}, nil
}

// Clean applies age and quota eviction immediately.
func (c *Cache) Clean() { c.clean(false) }

type cacheCandidate struct {
	item  *Item
	atime time.Time
	size  int64
	opens int
	grace bool
}

func (c *Cache) snapshotCandidates() (items []cacheCandidate, used int64) {
	c.mu.Lock()
	objects := make([]*Item, 0, len(c.items))
	for _, item := range c.items {
		objects = append(objects, item)
	}
	c.mu.Unlock()

	items = make([]cacheCandidate, 0, len(objects))
	for _, item := range objects {
		item.mu.Lock()
		candidate := cacheCandidate{
			item:  item,
			atime: item.info.ATime,
			size:  item.info.Rs.Size(),
			opens: item.opens,
			grace: item.graceTimer != nil,
		}
		item.mu.Unlock()
		used += candidate.size
		items = append(items, candidate)
	}
	return items, used
}

func (c *Cache) minFreeSpaceQuotaOK() bool {
	if c.opt.CacheMinFreeSpace <= 0 {
		return true
	}
	du, err := diskspace.New(c.root)
	if errors.Is(err, diskspace.ErrUnsupported) {
		return true
	}
	if err != nil {
		return true
	}
	return du.Available >= uint64(c.opt.CacheMinFreeSpace)
}

func (c *Cache) quotasOK(used int64) bool {
	maxSizeOK := c.opt.CacheMaxSize <= 0 || used <= c.opt.CacheMaxSize
	return maxSizeOK && c.minFreeSpaceQuotaOK()
}

func (c *Cache) removeUnused(item *Item) (bool, int64) {
	removed, freed, err := item.resetForSpace()
	if err != nil || !removed {
		return false, 0
	}
	c.mu.Lock()
	if c.items[item.key] == item {
		delete(c.items, item.key)
	}
	c.mu.Unlock()
	return true, freed
}

func (c *Cache) clean(kicked bool) {
	items, used := c.snapshotCandidates()
	now := time.Now()

	if c.opt.CacheMaxAge > 0 {
		cutoff := now.Add(-c.opt.CacheMaxAge)
		for i := range items {
			candidate := &items[i]
			if candidate.opens == 0 && !candidate.grace && candidate.atime.Before(cutoff) {
				if removed, freed := c.removeUnused(candidate.item); removed {
					used -= freed
					candidate.size = 0
				}
			}
		}
	}

	if !c.quotasOK(used) {
		sort.Slice(items, func(i, j int) bool { return items[i].atime.Before(items[j].atime) })

		// First remove items that are not in use, oldest first.
		for i := range items {
			if c.quotasOK(used) {
				break
			}
			candidate := &items[i]
			if candidate.size == 0 || candidate.opens != 0 || candidate.grace {
				continue
			}
			if removed, freed := c.removeUnused(candidate.item); removed {
				used -= freed
				candidate.size = 0
			}
		}

		// If quota is still exceeded, reset clean cache contents even for open
		// items. resetForSpace skips grace-period and actively accessed items,
		// preserving reset coordination for active readers.
		for i := range items {
			if c.quotasOK(used) {
				break
			}
			candidate := &items[i]
			if candidate.size == 0 {
				continue
			}
			removed, freed, err := candidate.item.resetForSpace()
			if err != nil {
				continue
			}
			used -= freed
			candidate.size = 0
			if removed {
				c.mu.Lock()
				if c.items[candidate.item.key] == candidate.item {
					delete(c.items, candidate.item.key)
				}
				c.mu.Unlock()
			}
		}
	}

	if kicked {
		c.kickerMu.Lock()
		c.cleanerKicked = false
		c.kickerMu.Unlock()
	}
	c.mu.Lock()
	c.outOfSpace = false
	c.cond.Broadcast()
	c.mu.Unlock()
}

// KickCleaner wakes the cleaner after an ENOSPC error and waits until that
// cleaning pass releases blocked I/O.
func (c *Cache) KickCleaner() {
	if c.opt.CachePollInterval <= 0 {
		return
	}
	c.kickerMu.Lock()
	if !c.cleanerKicked {
		c.cleanerKicked = true
		c.mu.Lock()
		c.outOfSpace = true
		c.kick <- struct{}{}
		c.mu.Unlock()
	}
	c.kickerMu.Unlock()

	c.mu.Lock()
	for c.outOfSpace {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

func (c *Cache) cleaner() {
	defer c.wg.Done()
	if c.opt.CachePollInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.opt.CachePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.kick:
			c.clean(true)
		case <-ticker.C:
			c.clean(false)
		case <-c.ctx.Done():
			return
		}
	}
}

// Stats returns current in-memory cache usage. CachedBytes counts known
// present ranges rather than sparse file apparent size.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	items := make([]*Item, 0, len(c.items))
	for _, item := range c.items {
		items = append(items, item)
	}
	c.mu.Unlock()

	stats := Stats{Items: len(items)}
	for _, item := range items {
		item.mu.Lock()
		stats.CachedBytes += item.info.Rs.Size()
		if item.opens != 0 {
			stats.OpenItems++
		}
		item.mu.Unlock()
	}
	return stats
}

// Close stops the cleaner and closes all local file handles.
func (c *Cache) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		c.cancel()
		c.wg.Wait()

		c.mu.Lock()
		defer c.mu.Unlock()
		for _, item := range c.items {
			item.mu.Lock()
			if item.graceTimer != nil {
				item.graceTimer.Stop()
				item.graceTimer = nil
			}
			if err := item.actualCloseLocked(); err != nil && c.closeErr == nil {
				c.closeErr = err
			}
			item.mu.Unlock()
		}
	})
	return c.closeErr
}

func (c *Cache) pathsForKey(key string) (dataPath, metaPath string) {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	return shardedPath(c.data, name, c.opt.CacheShardDepth), shardedPath(c.meta, name+".json", c.opt.CacheShardDepth)
}

func shardedPath(root, name string, depth int) string {
	parts := make([]string, 0, depth+2)
	parts = append(parts, root)
	for level := 0; level < depth; level++ {
		start := level * 2
		parts = append(parts, name[start:start+2])
	}
	parts = append(parts, name)
	return filepath.Join(parts...)
}
