package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"vfs-cache/internal/cachecore/diskusage"
	"vfs-cache/source"
)

type rangeCall struct {
	key        string
	start, end int64
}

type memorySource struct {
	mu       sync.Mutex
	objects  map[string][]byte
	metadata map[string]source.Metadata
	calls    []rangeCall
	failures []error
	gate     <-chan struct{}
	started  chan struct{}
	startOne sync.Once
}

func newMemorySource() *memorySource {
	return &memorySource{
		objects:  make(map[string][]byte),
		metadata: make(map[string]source.Metadata),
	}
}

func (s *memorySource) set(key string, data string, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = []byte(data)
	s.metadata[key] = source.Metadata{Size: int64(len(data)), ETag: etag, ContentType: "application/octet-stream"}
}

func (s *memorySource) Stat(_ context.Context, key string) (source.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.metadata[key]
	if !ok {
		return source.Metadata{}, source.ErrNotFound
	}
	return meta, nil
}

func (s *memorySource) OpenRange(ctx context.Context, key string, start, end int64, expected source.Metadata) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls = append(s.calls, rangeCall{key: key, start: start, end: end})
	if len(s.failures) != 0 {
		err := s.failures[0]
		s.failures = s.failures[1:]
		s.mu.Unlock()
		return nil, err
	}
	data, ok := s.objects[key]
	meta := s.metadata[key]
	gate := s.gate
	started := s.started
	s.mu.Unlock()

	if !ok {
		return nil, source.ErrNotFound
	}
	if expected.Size != meta.Size || (expected.ETag != "" && expected.ETag != meta.ETag) {
		return nil, source.ErrObjectChanged
	}
	if start < 0 || end > int64(len(data)) || end <= start {
		return nil, source.ErrRangeNotSatisfiable
	}
	if started != nil {
		s.startOne.Do(func() { close(started) })
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return io.NopCloser(bytes.NewReader(data[start:end])), nil
}

func (s *memorySource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *memorySource) lastCall() rangeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

func testOptions() Options {
	opt := DefaultOptions()
	opt.CachePollInterval = 0
	opt.HandleCaching = 0
	return opt
}

func openTestCache(t *testing.T, src source.Source, opt Options) *Cache {
	t.Helper()
	c, err := New(context.Background(), t.TempDir(), src, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func TestReadCachesSparseRange(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	c := openTestCache(t, src, testOptions())

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 4)
	if n, err := r.ReadAt(buf, 2); err != nil || n != 4 || string(buf) != "2345" {
		t.Fatalf("first ReadAt = %d, %v, %q", n, err, buf)
	}
	if n, err := r.ReadAt(buf, 2); err != nil || n != 4 || string(buf) != "2345" {
		t.Fatalf("second ReadAt = %d, %v, %q", n, err, buf)
	}
	if got := src.callCount(); got != 1 {
		t.Fatalf("source calls = %d, want 1", got)
	}
	if got := c.Stats().CachedBytes; got != 8 {
		t.Fatalf("cached bytes = %d, want 8 (chunk downloader continued to EOF)", got)
	}
}

func TestReadAheadIsCached(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	opt := testOptions()
	opt.ReadAhead = 3
	c := openTestCache(t, src, opt)

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 2)
	if _, err := r.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if got := src.lastCall(); got.start != 2 || got.end != 10 {
		t.Fatalf("source range = [%d,%d), want [2,10) after chunk clipping to EOF", got.start, got.end)
	}
	buf = make([]byte, 3)
	if n, err := r.ReadAt(buf, 4); err != nil || n != 3 || string(buf) != "456" {
		t.Fatalf("read-ahead ReadAt = %d, %v, %q", n, err, buf)
	}
	if got := src.callCount(); got != 1 {
		t.Fatalf("source calls = %d, want 1", got)
	}
}

func TestConcurrentOverlappingReadsShareFetch(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	gate := make(chan struct{})
	src.gate = gate
	src.started = make(chan struct{})
	c := openTestCache(t, src, testOptions())

	r1, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	type result struct {
		body string
		err  error
	}
	results := make(chan result, 2)
	go func() {
		buf := make([]byte, 4)
		_, err := r1.ReadAt(buf, 2)
		results <- result{body: string(buf), err: err}
	}()
	<-src.started
	go func() {
		buf := make([]byte, 2)
		_, err := r2.ReadAt(buf, 3)
		results <- result{body: string(buf), err: err}
	}()

	// Give the second reader time to observe the active fetch before releasing it.
	time.Sleep(10 * time.Millisecond)
	close(gate)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("read errors: %v, %v", first.err, second.err)
	}
	if !((first.body == "2345" && second.body == "34") || (first.body == "34" && second.body == "2345")) {
		t.Fatalf("unexpected bodies %q and %q", first.body, second.body)
	}
	if got := src.callCount(); got != 1 {
		t.Fatalf("source calls = %d, want 1", got)
	}
}

func TestCachePersistsRangesAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	opt := testOptions()

	c1, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c1.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if got := c2.Stats(); got.Items != 1 || got.CachedBytes != 8 || got.OpenItems != 0 {
		t.Fatalf("stats immediately after restart = %+v, want one loaded 8-byte cached item", got)
	}
	r, err = c2.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if got := src.callCount(); got != 1 {
		t.Fatalf("source calls after restart = %d, want 1", got)
	}
}

func TestObjectChangeInvalidatesCachedRanges(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "abcdefghij", `"v1"`)
	c := openTestCache(t, src, testOptions())

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	src.set("movie", "ABCDEFGHIJ", `"v2"`)
	r, err = c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ABCD" {
		t.Fatalf("body = %q, want ABCD", buf)
	}
	if got := src.callCount(); got != 2 {
		t.Fatalf("source calls = %d, want 2", got)
	}
}

func TestCleanResetsOpenItemWhenOverQuota(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	opt := testOptions()
	opt.CacheMaxSize = 1
	c := openTestCache(t, src, opt)

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	c.Clean()
	if got := c.Stats(); got.Items != 1 || got.OpenItems != 1 || got.CachedBytes != 0 {
		t.Fatalf("stats after open-item reset = %+v", got)
	}
	if _, err := r.ReadAt(buf, 0); err != nil || string(buf) != "01" {
		t.Fatalf("ReadAt after reset = %v, %q", err, buf)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	c.Clean()
	if got := c.Stats(); got.Items != 0 || got.CachedBytes != 0 {
		t.Fatalf("stats after clean = %+v", got)
	}
}

func TestMinFreeSpaceQuotaEvictsCache(t *testing.T) {
	dir := t.TempDir()
	du, err := diskusage.New(dir)
	if errors.Is(err, diskusage.ErrUnsupported) {
		t.Skip("disk usage unsupported")
	}
	if err != nil {
		t.Fatal(err)
	}
	const maxInt64 = uint64(^uint64(0) >> 1)
	if du.Total >= maxInt64 {
		t.Skip("filesystem is too large for int64 minimum-free-space option")
	}

	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	opt := testOptions()
	opt.CacheMinFreeSpace = int64(du.Total + 1)
	c, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 2), 0); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	c.Clean()
	if got := c.Stats(); got.Items != 0 || got.CachedBytes != 0 {
		t.Fatalf("stats after minimum-free-space clean = %+v", got)
	}
}

func TestCorruptMetadataNeverTrustsExistingSparseFile(t *testing.T) {
	dir := t.TempDir()
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	opt := testOptions()

	c1, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c1.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	_, metaPath := c1.pathsForKey("movie")
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	c2, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	r, err = c2.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if got := src.callCount(); got != 2 {
		t.Fatalf("source calls = %d, want 2 after corrupt metadata", got)
	}
}

func TestReadAtClipsAtEOF(t *testing.T) {
	src := newMemorySource()
	src.set("short", "abcde", `"v1"`)
	c := openTestCache(t, src, testOptions())
	r, err := c.Open(context.Background(), "short")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 3)
	if n != 2 || !errors.Is(err, io.EOF) || string(buf[:n]) != "de" {
		t.Fatalf("ReadAt = %d, %v, %q; want 2, EOF, de", n, err, buf[:n])
	}
}

func TestDownloaderRestartsAfterRangeFailures(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	src.failures = []error{errors.New("temporary 1"), errors.New("temporary 2")}
	c := openTestCache(t, src, testOptions())
	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 2)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if got := src.callCount(); got != 3 {
		t.Fatalf("source calls = %d, want 3", got)
	}
}

func TestDownloaderRestartsAfterObjectChangedError(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	src.failures = []error{source.ErrObjectChanged}
	c := openTestCache(t, src, testOptions())
	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err = r.ReadAt(make([]byte, 2), 0); err != nil {
		t.Fatal(err)
	}
	if got := src.callCount(); got != 2 {
		t.Fatalf("source calls = %d, want 2", got)
	}
}

func TestCloseIsIdempotentAndRejectsNewOpens(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "0123456789", `"v1"`)
	c, err := New(context.Background(), t.TempDir(), src, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(context.Background(), "movie"); err == nil {
		t.Fatal("expected Open after Close to fail")
	}
}

func TestUnvalidatedMetadataIsNotReusedAcrossOpens(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "AAAA", "")
	c := openTestCache(t, src, testOptions())

	r, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	src.set("movie", "BBBB", "")
	r, err = c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "BB" {
		t.Fatalf("body = %q, want BB", buf)
	}
	if got := src.callCount(); got != 2 {
		t.Fatalf("source calls = %d, want 2 because unvalidated bytes must not persist across opens", got)
	}
}

func TestActiveReaderPreventsVersionSwap(t *testing.T) {
	src := newMemorySource()
	src.set("movie", "abcdefghij", `"v1"`)
	c := openTestCache(t, src, testOptions())

	first, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.ReadAt(make([]byte, 2), 0); err != nil {
		t.Fatal(err)
	}

	src.set("movie", "ABCDEFGHIJ", `"v2"`)
	if _, err := c.Open(context.Background(), "movie"); !errors.Is(err, source.ErrObjectChanged) {
		t.Fatalf("second open error = %v, want ErrObjectChanged while old reader is active", err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := c.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	buf := make([]byte, 2)
	if _, err := second.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "AB" {
		t.Fatalf("body = %q, want AB", buf)
	}
}

func TestShardedPath(t *testing.T) {
	for _, test := range []struct {
		depth int
		want  string
	}{
		{depth: 0, want: filepath.Join("root", "abcdef")},
		{depth: 1, want: filepath.Join("root", "ab", "abcdef")},
		{depth: 2, want: filepath.Join("root", "ab", "cd", "abcdef")},
	} {
		if got := shardedPath("root", "abcdef", test.depth); got != test.want {
			t.Fatalf("depth %d: got %q, want %q", test.depth, got, test.want)
		}
	}
}

func TestShardDepthMigrationPreservesCachedRanges(t *testing.T) {
	dir := t.TempDir()
	src := newMemorySource()
	src.set("movie", "abcdefghij", `"v1"`)

	opt := testOptions()
	opt.CacheShardDepth = 1
	c1, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := c1.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := r1.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	oldData, oldMeta := c1.pathsForKey("movie")
	calls := src.callCount()
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	opt.CacheShardDepth = 2
	c2, err := New(context.Background(), dir, src, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	newData, newMeta := c2.pathsForKey("movie")
	if oldData == newData || oldMeta == newMeta {
		t.Fatalf("shard depth did not change paths: data=%q meta=%q", newData, newMeta)
	}
	if _, err := os.Stat(oldData); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old data path still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(oldMeta); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old metadata path still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(newData); err != nil {
		t.Fatalf("new data path: %v", err)
	}
	if _, err := os.Stat(newMeta); err != nil {
		t.Fatalf("new metadata path: %v", err)
	}

	r2, err := c2.Open(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err := r2.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != "abcd" {
		t.Fatalf("body = %q, want abcd", got)
	}
	if got := src.callCount(); got != calls {
		t.Fatalf("source range calls = %d, want %d; migrated cached range should be reused", got, calls)
	}
}
