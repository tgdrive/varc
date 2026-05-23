package httpcache

import (
	"context"
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

	"github.com/tgdrive/varc"
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
	CacheDir          string      `caddy:"cache_dir"`
	CacheMaxAge       string      `caddy:"max_age"`
	CacheMaxSize      string      `caddy:"max_size"`
	CacheChunkSize    string      `caddy:"chunk_size"`
	CacheChunkStreams int         `caddy:"chunk_streams"`
	StripQuery        bool        `caddy:"strip_query"`
	StripDomain       bool        `caddy:"strip_domain"`
	ShardLevel        int         `caddy:"shard_level"`
	Passthrough       bool        `caddy:"passthrough"`
	Logger            varc.Logger `caddy:"-"`
}

// DefaultOptions returns Options with sensible defaults
func DefaultOptions() Options {
	return Options{
		ShardLevel:        1,
		CacheChunkStreams: 2,
	}
}

// Handler is the cache proxy HTTP handler
type Handler struct {
	cache   *varc.Cache
	client  *http.Client
	metrics *Metrics
	logger  varc.Logger

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

	varcOpt := varc.Options{
		CacheDir:     cacheDir,
		ChunkStreams: opt.CacheChunkStreams,
		ShardLevel:   opt.ShardLevel,
		Logger:       opt.Logger,
	}

	if opt.CacheMaxAge != "" {
		d, err := time.ParseDuration(opt.CacheMaxAge)
		if err == nil {
			varcOpt.CacheMaxAge = d
		} else {
			return nil, fmt.Errorf("invalid cache-max-age: %w", err)
		}
	}
	if opt.CacheMaxSize != "" {
		s, err := parseSize(opt.CacheMaxSize)
		if err == nil {
			varcOpt.CacheMaxSize = s
		}
	}
	if opt.CacheChunkSize != "" {
		s, err := parseSize(opt.CacheChunkSize)
		if err == nil {
			varcOpt.ChunkSize = s
		}
	}

	cache, err := varc.New(ctx, varcOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &Handler{
		cache:       cache,
		client:      &http.Client{Timeout: 30 * time.Second},
		metrics:     &Metrics{},
		logger:      opt.Logger,
		stripQuery:  opt.StripQuery,
		stripDomain: opt.StripDomain,
		shardLevel:  opt.ShardLevel,
		passthrough: opt.Passthrough,
	}, nil
}

// Shutdown shuts down the cache
func (h *Handler) Shutdown() {
	h.cache.Close()
}

// Metrics returns a reference to the handler's metrics collector
func (h *Handler) Metrics() *Metrics {
	return h.metrics
}

// ServeMetrics writes a JSON snapshot of the current metrics to w
func (h *Handler) ServeMetrics(w http.ResponseWriter) {
	stats := h.metrics.Snapshot()
	cacheStats := h.cache.Stats()
	for k, v := range cacheStats {
		if vi, ok := v.(int64); ok {
			stats[k] = vi
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// cacheKeyURL strips query and/or domain from a URL to produce a logical cache key.
func (h *Handler) cacheKeyURL(targetURL string) string {
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
	return keyURL
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
	keyURL := h.cacheKeyURL(targetURL)

	err := h.cache.Remove(keyURL)
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

// accessLog logs an HTTP request to the configured logger
func (h *Handler) accessLog(r *http.Request, status int, size int64, duration time.Duration) {
	if h.logger != nil {
		h.logger.Infof("[proxy] %s %s %d %d %v", r.Method, r.URL.String(), status, size, duration)
	}
}

// Serve handles an HTTP request for the given targetURL.
//
// It opens the file through the disk cache. Supports PURGE, conditional
// requests (If-Modified-Since, If-None-Match), and passthrough for
// non-GET methods or requests with Authorization/Cookie headers.
func (h *Handler) Serve(w http.ResponseWriter, r *http.Request, targetURL string) {
	if targetURL == "" {
		http.Error(w, "Target URL is required", http.StatusBadRequest)
		return
	}

	keyURL := h.cacheKeyURL(targetURL)
	// Compute the cache path for ETag/Content-Type (varc.Cache.Open()
	// applies the same ShardKey internally, so the result matches).
	cachePath := varc.ShardKey(keyURL, h.shardLevel)
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

	h.metrics.inc(&h.metrics.Requests)

	var reader *varc.Reader
	var openErr error

	// Check if the file is already cached. If so, we can serve it without
	// any upstream HEAD request — the cache engine reads the size from the
	// file stat on disk.
	if h.cache.Exists(keyURL) {
		h.metrics.inc(&h.metrics.Hits)
		reader, openErr = h.cache.Open(context.Background(), keyURL, -1, nil)
	} else {
		h.metrics.inc(&h.metrics.Misses)
		// Discover file metadata via HEAD (the cache engine requires known sizes).
		httpFile := newHTTPFile(targetURL, upstreamHeaders, h.client)
		if err := httpFile.Discover(); err != nil {
			h.accessLog(r, http.StatusBadGateway, 0, time.Since(start))
			http.Error(w, "Failed to discover file: "+err.Error(), http.StatusBadGateway)
			return
		}
		reader, openErr = h.cache.Open(context.Background(), keyURL, httpFile.Size(), httpFile,
			varc.WithFingerprint(httpFile.Fingerprint()),
			varc.WithModTime(httpFile.ModTime(context.Background())),
		)
	}
	if openErr != nil {
		http.Error(w, "Failed to open file: "+openErr.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	size := reader.Size()
	modTime := reader.ModTime()

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
		http.ServeContent(w, r, cachePath, modTime, reader)
	} else {
		if r.Header.Get("Range") != "" {
			http.Error(w, "Cannot use Range on files of unknown length", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		io.Copy(w, reader)
	}

	// Access log and metrics
	h.metrics.mu.Lock()
	h.metrics.BytesServed += size
	h.metrics.mu.Unlock()
	h.accessLog(r, http.StatusOK, size, time.Since(start))
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
