package httpcache

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testUpstream creates a test HTTP server that serves a file with Range support
func testUpstream(t *testing.T, data []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle HEAD requests
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}

		// Handle GET with optional Range
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		// Parse Range header
		var start, end int64
		if strings.HasPrefix(rangeHeader, "bytes=") {
			rangeStr := rangeHeader[6:]
			if parts := strings.SplitN(rangeStr, "-", 2); len(parts) == 2 {
				var err error
				start, _ = strconv.ParseInt(parts[0], 10, 64)
				if parts[1] != "" {
					end, err = strconv.ParseInt(parts[1], 10, 64)
					if err != nil {
						end = int64(len(data)) - 1
					}
				} else {
					end = int64(len(data)) - 1
				}
			}
		}

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		if start > end {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
}

func TestProxyFullFileServe(t *testing.T) {
	data := []byte("hello world this is a test file for the cache proxy")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// Make a request to the proxy
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, strconv.Itoa(len(data)), resp.Header.Get("Content-Length"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, body)
}

func TestProxyRangeRequest(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	tests := []struct {
		name      string
		offset    int64
		length    int64
		expectLen int
		expect    string
	}{
		{"first 10 bytes", 0, 10, 10, "0123456789"},
		{"bytes 10-19", 10, 10, 10, "abcdefghij"},
		{"last 5 bytes", 31, 5, 5, "vwxyz"},
		{"middle range", 5, 10, 10, "56789abcde"},
		{"single byte", 15, 1, 1, "f"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", proxy.URL, nil)
			require.NoError(t, err)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", tt.offset, tt.offset+tt.length-1))

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
			assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", tt.offset, tt.offset+int64(tt.length)-1, len(data)), resp.Header.Get("Content-Range"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.expectLen, len(body))
			assert.Equal(t, tt.expect, string(body))
		})
	}
}

func TestProxyMultipleRanges(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 2,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// First request gets the beginning
	req1, _ := http.NewRequest("GET", proxy.URL, nil)
	req1.Header.Set("Range", "bytes=0-9")
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, "The quick ", string(body1))

	// Second request gets a different part (should use cache)
	req2, _ := http.NewRequest("GET", proxy.URL, nil)
	req2.Header.Set("Range", "bytes=10-19")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, "brown fox ", string(body2))
}

func TestProxyCacheReuse(t *testing.T) {
	data := make([]byte, 50000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 4,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// Read full file once (populates cache)
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, data, body)

	// Now make a Range request that should be served from cache
	req, _ := http.NewRequest("GET", proxy.URL, nil)
	req.Header.Set("Range", "bytes=1000-1999")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, data[1000:2000], body)
}

func TestProxyFileNotFound(t *testing.T) {
	opt := Options{
		CacheDir: t.TempDir(),

		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, "")
	}))
	defer proxy.Close()

	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestProxyHashCachePath(t *testing.T) {
	handler := &Handler{
		stripQuery:  false,
		stripDomain: false,
		shardLevel:  0,
	}

	// Same URL should produce same hash
	h1 := handler.hashCachePath("https://example.com/file.txt")
	h2 := handler.hashCachePath("https://example.com/file.txt")
	assert.Equal(t, h1, h2)

	// Different URLs should produce different hashes
	h3 := handler.hashCachePath("https://example.com/other.txt")
	assert.NotEqual(t, h1, h3)
}

func TestProxyStripQuery(t *testing.T) {
	handler := &Handler{
		stripQuery:  true,
		stripDomain: false,
		shardLevel:  0,
	}

	// URLs with different query params should have same hash with stripQuery
	h1 := handler.hashCachePath("https://example.com/file.txt?token=abc")
	h2 := handler.hashCachePath("https://example.com/file.txt?token=xyz")
	assert.Equal(t, h1, h2)
}

func TestCacheCleanup(t *testing.T) {
	data := []byte("test data for cache cleanup")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// Serve a file
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Check cache directory was created
	entries, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "cache directory should contain files")

	// Shutdown handler
	handler.Shutdown()
}

func TestOptionsDefaults(t *testing.T) {
	opt := DefaultOptions()

	assert.Equal(t, 1, opt.ShardLevel)
	assert.Equal(t, 2, opt.CacheChunkStreams)
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"100", 100},
		{"1k", 1024},
		{"1M", 1 << 20},
		{"1G", 1 << 30},
		{"2M", 2 << 20},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOptionsNewHandler(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "test-cache")
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkSize:    "4M",
		CacheChunkStreams: 4,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	assert.NotNil(t, handler.Engine)
	assert.NotNil(t, handler.client)
}

func TestPurgeEndpoint(t *testing.T) {
	data := []byte("purge test data")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// First cache the file
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, data, body)

	// Verify it's cached
	cachePath := handler.hashCachePath(upstream.URL)
	item := handler.Engine.CacheItem(cachePath)
	require.NotNil(t, item)
	require.True(t, item.Exists())

	// Purge it via PURGE method
	purgeReq, err := http.NewRequest("PURGE", proxy.URL, nil)
	require.NoError(t, err)
	purgeResp, err := http.DefaultClient.Do(purgeReq)
	require.NoError(t, err)
	purgeBody, err := io.ReadAll(purgeResp.Body)
	purgeResp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, purgeResp.StatusCode)
	assert.Equal(t, "Purged", string(purgeBody))

	// Verify it's gone from cache
	item2 := handler.Engine.CacheItem(cachePath)
	require.NotNil(t, item2)
	require.False(t, item2.Exists())
}

func TestPassthroughNonGet(t *testing.T) {
	data := []byte("passthrough test")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	// POST request should bypass cache
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/", strings.NewReader("body"))
	handler.Serve(w, req, upstream.URL)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, data, w.Body.Bytes())

	// Also test that a PUT request bypasses cache
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("PUT", "/", strings.NewReader("body"))
	handler.Serve(w2, req2, upstream.URL)
	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, data, w2.Body.Bytes())
}

func TestPassthroughHeader(t *testing.T) {
	data := []byte("passthrough header test")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	// Request with Authorization header should bypass cache
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	handler.Serve(w, req, upstream.URL)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, data, w.Body.Bytes())
}

func TestPassthroughOption(t *testing.T) {
	data := []byte("passthrough option test")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
		Passthrough:       true,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	// Even GET should bypass cache when Passthrough option is set
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	handler.Serve(w, req, upstream.URL)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, data, w.Body.Bytes())
}

func TestMetrics(t *testing.T) {
	data := []byte("metrics test data")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// Make a request
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Verify metrics
	snap := handler.Metrics().Snapshot()
	assert.Equal(t, int64(1), snap["requests"])
	assert.Equal(t, int64(len(data)), snap["bytes_served"])
}

func TestMetricsEndpoint(t *testing.T) {
	data := []byte("metrics endpoint test")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	// Serve a file first
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	handler.Serve(w, req, upstream.URL)
	assert.Equal(t, http.StatusOK, w.Code)

	// ServeMetrics should return JSON
	mw := httptest.NewRecorder()
	handler.ServeMetrics(mw)
	assert.Equal(t, "application/json", mw.Header().Get("Content-Type"))
	assert.Contains(t, mw.Body.String(), `"requests":1`)
}

func TestConditionalRequest(t *testing.T) {
	data := []byte("conditional request test")
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir:          cacheDir,
		CacheChunkStreams: 1,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// First request to cache the file
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	resp.Body.Close()

	// Get Last-Modified from the first response
	lastMod := resp.Header.Get("Last-Modified")
	require.NotEmpty(t, lastMod)
	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag)

	// Request with If-Modified-Since (future date) → should get 304
	req, _ := http.NewRequest("GET", proxy.URL, nil)
	req.Header.Set("If-Modified-Since", time.Now().UTC().Add(time.Hour).Format(http.TimeFormat))
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusNotModified, resp2.StatusCode)

	// Request with If-None-Match (matching ETag) → should get 304
	req3, _ := http.NewRequest("GET", proxy.URL, nil)
	req3.Header.Set("If-None-Match", etag)
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	resp3.Body.Close()
	assert.Equal(t, http.StatusNotModified, resp3.StatusCode)
}

func TestConcurrentRangeRequests(t *testing.T) {
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	upstream := testUpstream(t, data)
	defer upstream.Close()

	cacheDir := t.TempDir()
	opt := Options{
		CacheDir: cacheDir,

		CacheChunkStreams: 4,
		ShardLevel:        0,
	}

	handler, err := NewHandler(opt)
	require.NoError(t, err)
	defer handler.Shutdown()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Serve(w, r, upstream.URL)
	}))
	defer proxy.Close()

	// Fire 10 concurrent range requests
	errChan := make(chan error, 10)
	for i := 0; i < 10; i++ {
		start := i * 1000
		end := start + 999
		go func(s, e int) {
			req, _ := http.NewRequest("GET", proxy.URL, nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", s, e))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				errChan <- err
				return
			}
			if string(body) != string(data[s:e+1]) {
				errChan <- fmt.Errorf("data mismatch at range %d-%d", s, e)
				return
			}
			errChan <- nil
		}(start, end)
	}

	for i := 0; i < 10; i++ {
		err := <-errChan
		require.NoError(t, err)
	}
}
