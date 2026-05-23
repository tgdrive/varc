package httpcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackedUpstream returns a test server that counts HEAD vs GET requests.
func trackedUpstream(t *testing.T, data []byte) (*httptest.Server, *upstreamStats) {
	t.Helper()
	stats := &upstreamStats{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isRange := r.Header.Get("Range") != ""
		if r.Method == "HEAD" {
			stats.headCount.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			return
		}
		// GET
		stats.getTotal.Add(1)
		if isRange {
			stats.rangeCount.Add(1)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" || !strings.HasPrefix(rangeHeader, "bytes=") {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		// Parse Range header
		rangeVal := rangeHeader[6:]
		parts := strings.SplitN(rangeVal, "-", 2)
		if len(parts) != 2 {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		var start, end int64
		fmt.Sscanf(parts[0], "%d", &start)
		if parts[1] != "" {
			fmt.Sscanf(parts[1], "%d", &end)
		} else {
			end = int64(len(data)) - 1
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		if start > end {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	return srv, stats
}

type upstreamStats struct {
	headCount  atomic.Int32
	getTotal   atomic.Int32
	rangeCount atomic.Int32
}

func (s *upstreamStats) String() string {
	return fmt.Sprintf("HEAD:%d GET:%d (Range:%d)", s.headCount.Load(), s.getTotal.Load(), s.rangeCount.Load())
}

func hashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// TestFullFileCachedOnDisk verifies a full GET results in the entire file
// being cached on disk with matching content hash.
func TestFullFileCachedOnDisk(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedHash := hashBytes(data)

	upstream, stats := trackedUpstream(t, data)
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

	// Full GET — should cache the entire file
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, data, body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	t.Logf("Upstream stats after first read: %s", stats)

	// Should have 1 HEAD (metadata check) + 1 GET (data fetch)
	assert.Equal(t, int32(1), stats.headCount.Load(), "should do 1 HEAD for metadata check")
	assert.Equal(t, int32(1), stats.getTotal.Load(), "should do 1 GET for data fetch")

	// Inspect cache directories
	dataDir := filepath.Join(cacheDir, "data")
	metaDir := filepath.Join(cacheDir, "meta")
	dataEntries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	metaEntries, err := os.ReadDir(metaDir)
	require.NoError(t, err)

	// Find data and meta files (they're stored under subdirectories by hash sharding)
	var dataFile, metaFile string
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataFile = path
		}
		return nil
	})
	filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			metaFile = path
		}
		return nil
	})

	require.NotEmpty(t, dataFile, "should have a cache data file in %s", dataDir)
	require.NotEmpty(t, metaFile, "should have a metadata file in %s", metaDir)

	t.Logf("Cache data file: %s", dataFile)
	t.Logf("Cache meta file: %s", metaFile)

	// Verify data file content hash
	cachedBytes, err := os.ReadFile(dataFile)
	require.NoError(t, err)
	cachedHash := hashBytes(cachedBytes)
	assert.Equal(t, expectedHash, cachedHash, "cached file hash should match original")
	assert.Equal(t, len(data), len(cachedBytes), "cached file size should match original")

	// Verify metadata JSON
	metaBytes, err := os.ReadFile(metaFile)
	require.NoError(t, err)
	var meta map[string]interface{}
	err = json.Unmarshal(metaBytes, &meta)
	require.NoError(t, err, "metadata must be valid JSON: %s", string(metaBytes))
	assert.Equal(t, float64(len(data)), meta["Size"], "metadata should record correct file size")

	// Verify meta / data dir structure
	assert.NotEmpty(t, dataEntries, "data/ dir should have entries")
	assert.NotEmpty(t, metaEntries, "meta/ dir should have entries")
}

// TestRangeCachedPartiallyOnDisk verifies a Range GET only fetches the requested
// range from upstream and stores it in the cache file (sparse for rest).
func TestRangeCachedPartiallyOnDisk(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEF")
	// Length: 46
	// Let's request bytes 20-29: "klmnopqrst"
	rangeStart, rangeLen := int64(20), int64(10)
	expectedRange := data[rangeStart : rangeStart+rangeLen]

	upstream, stats := trackedUpstream(t, data)
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

	// Range request for bytes 20-29
	req, _ := http.NewRequest("GET", proxy.URL, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rangeStart, rangeStart+rangeLen-1))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, expectedRange, body)
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)

	t.Logf("Upstream stats after range read: %s", stats)

	// Should have 1 HEAD + at least 1 GET (depends on chunk size if full file fetched)
	assert.Equal(t, int32(1), stats.headCount.Load(), "should do 1 HEAD")

	// The chunk size is big (128MiB default), so the GET should fetch a single chunk
	// that covers the whole file
	assert.Equal(t, int32(1), stats.getTotal.Load(), "should do 1 GET")

	// Find cache data file
	dataDir := filepath.Join(cacheDir, "data")
	var dataFile string
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataFile = path
		}
		return nil
	})
	require.NotEmpty(t, dataFile)

	cachedBytes, err := os.ReadFile(dataFile)
	require.NoError(t, err)

	// With 128MiB chunk size, the entire 46-byte file is fetched in one chunk
	// from upstream. So the cache should contain the full file.
	assert.Equal(t, len(data), len(cachedBytes),
		"cached file should be full size (chunkedreader fetches full file in one chunk)")
	cachedHash := hashBytes(cachedBytes)
	expectedHash := hashBytes(data)
	assert.Equal(t, expectedHash, cachedHash,
		"with large chunk size, entire file should be cached")
}

// TestCacheHashMatches verifies that after requesting multiple ranges,
// the assembled cache file has the correct hash.
func TestCacheHashMatches(t *testing.T) {
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 251) // Prime to avoid patterns
	}
	expectedHash := hashBytes(data)

	upstream, _ := trackedUpstream(t, data)
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

	// Request 3 disjoint ranges at different positions
	rangeRequests := []struct {
		start, length int64
	}{
		{0, 1000},
		{50000, 1000},
		{99000, 1000},
	}

	for _, rr := range rangeRequests {
		req, _ := http.NewRequest("GET", proxy.URL, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rr.start, rr.start+rr.length-1))
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	}

	// Read the full file from cache - this should trigger downloading
	// any uncached parts and assemble the complete file
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	fullBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)

	fullHash := hashBytes(fullBody)
	assert.Equal(t, expectedHash, fullHash,
		"full cache read should match original hash")

	// Verify on-disk file hash
	dataDir := filepath.Join(cacheDir, "data")
	var dataFile string
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataFile = path
		}
		return nil
	})
	if dataFile != "" {
		diskBytes, err := os.ReadFile(dataFile)
		require.NoError(t, err)
		diskHash := hashBytes(diskBytes)
		assert.Equal(t, expectedHash, diskHash,
			"on-disk cached file hash should match original after full read")
		assert.Equal(t, len(data), len(diskBytes),
			"on-disk file should be full size")
	}
}

// TestCacheReuseDataNoExtraGets verifies that once data is cached,
// subsequent requests don't do extra GET requests (only HEAD for metadata check).
func TestCacheReuseDataNoExtraGets(t *testing.T) {
	data := []byte("data for cache reuse verification test")
	upstream, stats := trackedUpstream(t, data)
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

	// First request: full file — this caches it
	resp, err := http.Get(proxy.URL)
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, data, body1)
	t.Logf("After 1st full read: %s", stats)

	// At this point: 1 HEAD (metadata check) + 1 GET (data) = 2 upstream calls
	assert.Equal(t, int32(1), stats.headCount.Load())
	assert.Equal(t, int32(1), stats.getTotal.Load())

	// Second request: full file again — data should come from cache
	resp, err = http.Get(proxy.URL)
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, data, body2)
	t.Logf("After 2nd full read: %s", stats)

	// HEAD counter goes up (metadata stale check), but no new GET
	assert.Equal(t, int32(2), stats.headCount.Load(), "HEAD for metadata check on each open")
	assert.Equal(t, int32(1), stats.getTotal.Load(), "data should be served from cache, no additional GET")

	// Third request: range — data should also come from cache
	req, _ := http.NewRequest("GET", proxy.URL, nil)
	req.Header.Set("Range", "bytes=5-14")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body3, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, data[5:15], body3)
	t.Logf("After range read: %s", stats)

	assert.Equal(t, int32(3), stats.headCount.Load(), "HEAD for metadata check on each open")
	assert.Equal(t, int32(1), stats.getTotal.Load(), "range data should also come from cache")
}

// TestCacheDirStructure verifies the cache directory layout.
func TestCacheDirStructure(t *testing.T) {
	data := []byte("test cache dir structure")
	upstream, _ := trackedUpstream(t, data)
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

	// Serve a file to populate cache
	http.Get(proxy.URL)

	// Check the two top-level directories
	dataDir := filepath.Join(cacheDir, "data")
	metaDir := filepath.Join(cacheDir, "meta")

	_, err = os.Stat(dataDir)
	assert.NoError(t, err, "data/ data dir should exist")

	_, err = os.Stat(metaDir)
	assert.NoError(t, err, "meta/ metadata dir should exist")

	// Data directory should have subdirectories (sharding)
	dataEntries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	assert.NotEmpty(t, dataEntries, "data/ should have entries")

	// Metadata directory should have subdirectories
	metaEntries, err := os.ReadDir(metaDir)
	require.NoError(t, err)
	assert.NotEmpty(t, metaEntries, "meta/ should have entries")

	// Walk to find and log all files
	t.Log("Cache data files:")
	var dataCount, metaCount int
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataCount++
		}
		return nil
	})
	filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			metaCount++
			// Verify metadata is valid JSON
			metaBytes, err := os.ReadFile(path)
			if err == nil {
				var meta map[string]interface{}
				if json.Unmarshal(metaBytes, &meta) == nil {
					t.Logf("  %s: Size=%v, ModTime=%v", path, meta["Size"], meta["ModTime"])
				}
			}
		}
		return nil
	})

	t.Logf("Data files: %d, Meta files: %d", dataCount, metaCount)
	assert.Equal(t, 1, dataCount, "should have exactly 1 data file")
	assert.Equal(t, 1, metaCount, "should have exactly 1 metadata file")
}
