package httpcache

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tgdrive/varc/internal"
	"github.com/tgdrive/varc/internal/types"
)

// Metrics tracks cache proxy performance counters.
type Metrics struct {
	mu                sync.Mutex
	Requests          int64 `json:"requests"`
	Hits              int64 `json:"hits"`
	Misses            int64 `json:"misses"`
	BytesServed       int64 `json:"bytes_served"`
	BytesFromUpstream int64 `json:"bytes_from_upstream"`
	Purges            int64 `json:"purges"`
}

// Snapshot returns a copy of the current metrics as a map.
func (m *Metrics) Snapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]int64{
		"requests":            m.Requests,
		"hits":                m.Hits,
		"misses":              m.Misses,
		"bytes_served":        m.BytesServed,
		"bytes_from_upstream": m.BytesFromUpstream,
		"purges":              m.Purges,
	}
}

// inc atomically increments a counter.
func (m *Metrics) inc(field *int64) {
	m.mu.Lock()
	*field++
	m.mu.Unlock()
}

// add atomically adds to a counter.
func (m *Metrics) add(field *int64, n int64) {
	m.mu.Lock()
	*field += n
	m.mu.Unlock()
}

// Options holds configuration for the cache proxy handler
type Options struct {
	CacheDir          string       `caddy:"cache_dir"`
	CacheMaxAge       string       `caddy:"max_age"`
	CacheMaxSize      string       `caddy:"max_size"`
	CacheChunkSize    string       `caddy:"chunk_size"`
	CacheChunkStreams int          `caddy:"chunk_streams"`
	StripQuery        bool         `caddy:"strip_query"`
	StripDomain       bool         `caddy:"strip_domain"`
	ShardLevel        int          `caddy:"shard_level"`
	Passthrough       bool         `caddy:"passthrough"`
	Logger            types.Logger `caddy:"-"`
}

// DefaultOptions returns Options with sensible defaults
func DefaultOptions() Options {
	return Options{
		ShardLevel:        1,
		CacheChunkStreams: 2,
	}
}

// mapping tracks URL-to-cache-path mappings so the cache engine knows
// which upstream URL and headers to use for each cache path
type mapping struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	url     string
	headers http.Header
}

func newMapping() *mapping {
	return &mapping{entries: make(map[string]cacheEntry)}
}

func (m *mapping) put(url, cachePath string, headers http.Header) {
	m.mu.Lock()
	m.entries[cachePath] = cacheEntry{url: url, headers: headers.Clone()}
	m.mu.Unlock()
}

func (m *mapping) get(cachePath string) (cacheEntry, bool) {
	m.mu.RLock()
	e, ok := m.entries[cachePath]
	m.mu.RUnlock()
	return e, ok
}

// Handler is the cache proxy HTTP handler
type Handler struct {
	Engine  *internal.Engine
	mapping *mapping
	client  *http.Client
	metrics *Metrics

	stripQuery  bool
	stripDomain bool
	shardLevel  int
	passthrough bool
}

// NewHandler creates a new Handler
func NewHandler(opt Options) (*Handler, error) {
	ctx := context.Background()

	cacheDir := opt.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "varc_cache")
	}

	// Build engine options
	engOpt := &types.Options{
		CacheDir: cacheDir,
	}

	if opt.Logger != nil {
		engOpt.Logger = opt.Logger
	}

	if opt.CacheMaxAge != "" {
		d, err := time.ParseDuration(opt.CacheMaxAge)
		if err == nil {
			engOpt.CacheMaxAge = d
		} else {
			return nil, fmt.Errorf("invalid cache-max-age: %w", err)
		}
	}
	if opt.CacheMaxSize != "" {
		s, err := parseSize(opt.CacheMaxSize)
		if err == nil {
			engOpt.CacheMaxSize = s
		}
	}
	if opt.CacheChunkSize != "" {
		s, err := parseSize(opt.CacheChunkSize)
		if err == nil {
			engOpt.ChunkSize = s
		}
	}
	engOpt.ChunkStreams = opt.CacheChunkStreams

	engOpt.Init()

	engInstance, err := internal.New(ctx, engOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	return &Handler{
		Engine:      engInstance,
		mapping:     newMapping(),
		client:      &http.Client{Timeout: 30 * time.Second},
		metrics:     &Metrics{},
		stripQuery:  opt.StripQuery,
		stripDomain: opt.StripDomain,
		shardLevel:  opt.ShardLevel,
		passthrough: opt.Passthrough,
	}, nil
}

// Shutdown shuts down the handler
func (h *Handler) Shutdown() {
	h.Engine.Close()
}

// Metrics returns a reference to the handler's metrics collector
func (h *Handler) Metrics() *Metrics {
	return h.metrics
}

// ServeMetrics writes a JSON snapshot of the current metrics to w
func (h *Handler) ServeMetrics(w http.ResponseWriter) {
	stats := h.metrics.Snapshot()
	engineStats := h.Engine.Stats()
	for k, v := range engineStats {
		if vi, ok := v.(int64); ok {
			stats[k] = vi
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// hashCachePath computes a cache path from a URL
func (h *Handler) hashCachePath(targetURL string) string {
	keyURL := targetURL
	if h.stripQuery {
		if idx := strings.Index(keyURL, "?"); idx >= 0 {
			keyURL = keyURL[:idx]
		}
	}
	if h.stripDomain {
		if idx := strings.Index(keyURL, "://"); idx >= 0 {
			rest := keyURL[idx+3:]
			if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
				rest = rest[slashIdx:]
			} else {
				rest = "/"
			}
			keyURL = rest
		}
	}

	hash := fmt.Sprintf("%x", md5.Sum([]byte(keyURL)))

	if h.shardLevel > 0 {
		sharded := ""
		for i := 0; i < h.shardLevel && i*2 < len(hash); i++ {
			sharded += string(hash[i*2]) + string(hash[i*2+1]) + "/"
		}
		return sharded + hash
	}
	return hash
}

// shouldPassthrough returns true if the request should bypass the cache
func (h *Handler) shouldPassthrough(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	if r.Header.Get("Authorization") != "" {
		return true
	}
	if r.Header.Get("Cookie") != "" {
		return true
	}
	return false
}

// proxyDirect proxies a request directly to the upstream without caching
func (h *Handler) proxyDirect(w http.ResponseWriter, r *http.Request, targetURL string) {
	req, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Copy original headers
	for k, vv := range r.Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	// Copy response headers
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handlePurge handles PURGE requests to remove items from cache
func (h *Handler) handlePurge(w http.ResponseWriter, r *http.Request, targetURL string) {
	cachePath := h.hashCachePath(targetURL)

	// Remove from mapping
	h.mapping.mu.Lock()
	delete(h.mapping.entries, cachePath)
	h.mapping.mu.Unlock()

	// Remove from cache
	err := h.Engine.Remove(cachePath)
	if err != nil {
		http.Error(w, "Purge failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	h.metrics.mu.Lock()
	h.metrics.Purges++
	h.metrics.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Purged"))
}

// tryStaleServe attempts to serve stale data from cache when upstream is unavailable.
// Returns true if stale data was served.
func (h *Handler) tryStaleServe(w http.ResponseWriter, r *http.Request, cachePath string) bool {
	item := h.Engine.CacheItem(cachePath)
	if item == nil || !item.Exists() {
		return false
	}

	// Open cached file handle (no upstream fetch)
	fh, err := h.Engine.OpenCached(cachePath, nil)
	if err != nil {
		return false
	}
	defer fh.Close()

	info, _ := fh.Stat()
	size := info.Size()
	modTime := info.ModTime()

	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Cache", "STALE")
	if !modTime.IsZero() {
		w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
	}

	if size >= 0 {
		http.ServeContent(w, r, cachePath, modTime, fh)
	} else {
		io.Copy(w, fh)
	}
	return true
}

// accessLog logs an HTTP request to the engine's logger if available
func (h *Handler) accessLog(r *http.Request, status int, size int64, duration time.Duration) {
	if h.Engine != nil && h.Engine.Opt.Logger != nil {
		h.Engine.Opt.Logger.Infof("[proxy] %s %s %d %d %v", r.Method, r.URL.String(), status, size, duration)
	}
}

// Serve handles an HTTP request for the given targetURL.
//
// It opens the file through the disk cache, associating it with the
// upstream URL so that the cache engine can fetch the file on cache misses.
// Supports PURGE, conditional requests (If-Modified-Since, If-None-Match),
// passthrough for non-GET methods, and stale-serve on upstream errors.
func (h *Handler) Serve(w http.ResponseWriter, r *http.Request, targetURL string) {
	if targetURL == "" {
		http.Error(w, "Target URL is required", http.StatusBadRequest)
		return
	}

	cachePath := h.hashCachePath(targetURL)
	start := time.Now()

	// Handle PURGE requests
	if r.Method == "PURGE" {
		h.handlePurge(w, r, targetURL)
		h.accessLog(r, http.StatusOK, 0, time.Since(start))
		return
	}

	// Check if request should bypass cache
	if h.passthrough || h.shouldPassthrough(r) {
		h.proxyDirect(w, r, targetURL)
		h.accessLog(r, http.StatusOK, 0, time.Since(start))
		return
	}

	// Build upstream headers by combining request headers (minus per-request ones)
	upstreamHeaders := make(http.Header)
	for k, vv := range r.Header {
		switch k {
		case "Range", "If-Range", "If-Modified-Since", "If-Unmodified-Since", "If-None-Match", "If-Match":
			continue
		}
		for _, v := range vv {
			upstreamHeaders.Add(k, v)
		}
	}

	h.mapping.put(targetURL, cachePath, upstreamHeaders)

	// Create an httpFile to associate with this cache path
	httpFile := h.newHTTPFile(cachePath)

	// Track cache hit/miss
	cachedItem := h.Engine.CacheItem(cachePath)
	isCached := cachedItem.Exists()

	h.metrics.mu.Lock()
	h.metrics.Requests++
	if isCached {
		h.metrics.Hits++
	} else {
		h.metrics.Misses++
	}
	h.metrics.mu.Unlock()

	// Open through disk cache with the httpFile
	fh, err := h.Engine.OpenCached(cachePath, httpFile)
	if err != nil {
		// Try stale-serve if upstream is unavailable
		if h.tryStaleServe(w, r, cachePath) {
			h.metrics.mu.Lock()
			h.metrics.Hits++
			h.metrics.mu.Unlock()
			h.accessLog(r, http.StatusOK, 0, time.Since(start))
			return
		}
		http.Error(w, "Failed to open file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer fh.Close()

	// Get file info
	info, err := fh.Stat()
	if err != nil {
		http.Error(w, "Failed to stat file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	size := info.Size()
	modTime := info.ModTime()

	// Handle conditional requests
	if !modTime.IsZero() {
		if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil && !modTime.After(t) {
			w.WriteHeader(http.StatusNotModified)
			h.accessLog(r, http.StatusNotModified, 0, time.Since(start))
			return
		}
	}
	if !modTime.IsZero() && size >= 0 {
		etag := fmt.Sprintf(`"%s-%x"`, cachePath, size)
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			h.accessLog(r, http.StatusNotModified, 0, time.Since(start))
			return
		}
	}

	// Set response headers
	if size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	mimeType := mime.TypeByExtension(path.Ext(cachePath))
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	if !modTime.IsZero() {
		w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
	}

	// Serve content (handles Range requests via http.ServeContent)
	if size >= 0 {
		http.ServeContent(w, r, cachePath, modTime, fh)
	} else {
		if r.Header.Get("Range") != "" {
			http.Error(w, "Cannot use Range on files of unknown length", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		io.Copy(w, fh)
	}

	// Access log and metrics
	h.metrics.mu.Lock()
	h.metrics.BytesServed += size
	h.metrics.mu.Unlock()
	h.accessLog(r, http.StatusOK, size, time.Since(start))
}

// newHTTPFile creates an httpFile for the given cache path, looking up
// the upstream URL and headers from the mapping.
func (h *Handler) newHTTPFile(cachePath string) *remoteFile {
	entry, ok := h.mapping.get(cachePath)
	if !ok {
		return &remoteFile{size: -1}
	}

	// First do a HEAD request to get metadata
	size := int64(-1)
	modTime := time.Time{}
	etag := ""

	req, err := http.NewRequest("HEAD", entry.url, nil)
	if err == nil {
		for k, vv := range entry.headers {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
		resp, err := h.client.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				etag = resp.Header.Get("ETag")
				if cl := resp.Header.Get("Content-Length"); cl != "" {
					if parsed, err := strconv.ParseInt(cl, 10, 64); err == nil {
						size = parsed
					}
				}
				if lm := resp.Header.Get("Last-Modified"); lm != "" {
					if parsed, err := http.ParseTime(lm); err == nil {
						modTime = parsed
					}
				}
			}
			resp.Body.Close()
		}
	}

	return newHTTPFile(entry.url, entry.headers, size, modTime, etag, h.client)
}

// parseSize parses a size string like "100M", "1G", etc.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	multiplier := int64(1)
	switch s[len(s)-1] {
	case 'k', 'K':
		multiplier = 1 << 10
		s = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1 << 20
		s = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1 << 30
		s = s[:len(s)-1]
	case 'T', 't':
		multiplier = 1 << 40
		s = s[:len(s)-1]
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %w", err)
	}
	return v * multiplier, nil
}
