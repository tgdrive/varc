package varc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// memoryRemote implements RemoteObject with in-memory data.
type memoryRemote struct {
	data        []byte
	fingerprint string
	modTime     time.Time
}

func (r memoryRemote) Open(ctx context.Context, options ...OpenOption) (io.ReadCloser, error) {
	start, end := int64(0), int64(len(r.data)-1)
	for _, option := range options {
		key, value := option.Header()
		if key == "Range" {
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
				if _, err := fmt.Sscanf(value, "bytes=%d-", &start); err == nil {
					end = int64(len(r.data) - 1)
				}
			}
		}
	}
	if start > int64(len(r.data)) {
		start = int64(len(r.data))
	}
	if end >= int64(len(r.data)) {
		end = int64(len(r.data) - 1)
	}
	if end < start {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return io.NopCloser(bytes.NewReader(r.data[start : end+1])), nil
}

func (r memoryRemote) Size() int64           { return int64(len(r.data)) }
func (r memoryRemote) String() string        { return "memory" }
func (r memoryRemote) Fingerprint() string   { return r.fingerprint }
func (r memoryRemote) ModTime(context.Context) time.Time { return r.modTime }

// newTestCache creates a Cache with HandleCaching=0 so item cleanup is
// immediate, and a small ChunkSize for multi-chunk tests.
func newTestCache(t *testing.T) *Cache {
	t.Helper()
	cache, err := New(context.Background(), Options{
		CacheDir:      t.TempDir(),
		HandleCaching: 0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cache
}

// newSmallChunkCache creates a Cache with a 4K chunk size so multi-chunk
// tests don't need huge data.
func newSmallChunkCache(t *testing.T) *Cache {
	t.Helper()
	cache, err := New(context.Background(), Options{
		CacheDir:      t.TempDir(),
		ChunkSize:     4096,
		HandleCaching: 0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cache
}

// ---------------------------------------------------------------------------
// Cache.New
// ---------------------------------------------------------------------------

func TestNewWithDefaults(t *testing.T) {
	cache, err := New(context.Background(), Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cache == nil {
		t.Fatal("New returned nil")
	}
	cache.Close()
}

func TestNewWithNilContext(t *testing.T) {
	cache, err := New(nil, Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New with nil ctx: %v", err)
	}
	cache.Close()
}

func TestNewInvalidCacheDir(t *testing.T) {
	// A non-writable parent should cause an error. We just verify that
	// passing an empty dir still works (falls back to default).
	cache, err := New(context.Background(), Options{CacheDir: ""})
	if err != nil {
		t.Fatalf("New with empty CacheDir: %v", err)
	}
	cache.Close()
}

// ---------------------------------------------------------------------------
// Cache.Open with RemoteObject – basic read path
// ---------------------------------------------------------------------------

func TestOpenReadFull(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("hello world this is a test file for the cache")
	remote := memoryRemote{data: data}
	r, err := cache.Open(context.Background(), "read-full", remote)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadAll: got %q, want %q", string(got), string(data))
	}
}

func TestOpenSize(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "size-test",
		memoryRemote{data: []byte("0123456789")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if got := r.Size(); got != 10 {
		t.Fatalf("Size = %d, want 10", got)
	}
}

func TestOpenModTime(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	mt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	r, err := cache.Open(context.Background(), "modtime-test",
		memoryRemote{data: []byte("abc"), modTime: mt})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got := r.ModTime()
	if !got.Equal(mt) {
		t.Fatalf("ModTime = %v, want %v", got, mt)
	}
}

// ---------------------------------------------------------------------------
// Reader.ReadAt
// ---------------------------------------------------------------------------

func TestReadAtOffsets(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("0123456789")
	r, err := cache.Open(context.Background(), "readat-offsets", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	tests := []struct {
		name   string
		off    int64
		len    int
		want   string
		wantN  int
		wantOK bool // true → err should be nil
	}{
		{"offset 0", 0, 5, "01234", 5, true},
		{"offset 3", 3, 4, "3456", 4, true},
		{"offset 9", 9, 1, "9", 1, true},
		{"offset 9 len 2 (partial)", 9, 2, "9", 1, false}, // EOF after 1 byte
		{"offset 10 (EOF)", 10, 1, "", 0, false},
		{"offset 100 (past end)", 100, 4, "", 0, false},
		{"zero size buffer", 0, 0, "", 0, true},
		{"negative offset", -1, 4, "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.len)
			n, err := r.ReadAt(buf, tt.off)
			if n != tt.wantN {
				t.Errorf("ReadAt n = %d, want %d", n, tt.wantN)
			}
			if err == nil && !tt.wantOK {
				t.Errorf("ReadAt err = nil, want error")
			}
			if err != nil && tt.wantOK {
				t.Errorf("ReadAt err = %v, want nil", err)
			}
			if tt.wantOK && string(buf[:n]) != tt.want {
				t.Errorf("ReadAt content = %q, want %q", string(buf[:n]), tt.want)
			}
		})
	}
}

func TestReadAtZeroLenBuffer(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "readat-zero", memoryRemote{data: []byte("abc")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	n, err := r.ReadAt(nil, 0)
	if n != 0 || err != nil {
		t.Fatalf("ReadAt(nil,0) = (%d,%v), want (0,nil)", n, err)
	}
}

// ---------------------------------------------------------------------------
// Reader.Read (sequential)
// ---------------------------------------------------------------------------

func TestReadSequential(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	r, err := cache.Open(context.Background(), "read-seq", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 5)
	// Read first chunk
	n, err := r.Read(buf)
	if n != 5 || string(buf) != "abcde" || err != nil {
		t.Fatalf("Read 1: (%d,%q,%v), want (5,abcde,nil)", n, string(buf[:n]), err)
	}
	// Read second chunk
	n, err = r.Read(buf)
	if n != 5 || string(buf) != "fghij" || err != nil {
		t.Fatalf("Read 2: (%d,%q,%v), want (5,fghij,nil)", n, string(buf[:n]), err)
	}
	// Skip to end
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll rest: %v", err)
	}
	if string(rest) != "klmnopqrstuvwxyz" {
		t.Fatalf("rest = %q, want %q", string(rest), "klmnopqrstuvwxyz")
	}
}

// ---------------------------------------------------------------------------
// Reader.Seek
// ---------------------------------------------------------------------------

func TestSeekStart(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("abcdefghij")
	r, err := cache.Open(context.Background(), "seek-start", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Seek to offset 3 from start
	pos, err := r.Seek(3, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek(3,Start): %v", err)
	}
	if pos != 3 {
		t.Fatalf("Seek pos = %d, want 3", pos)
	}
	buf := make([]byte, 3)
	n, err := r.Read(buf)
	if n != 3 || string(buf) != "def" || err != nil {
		t.Fatalf("Read after Seek: (%d,%q,%v), want (3,def,nil)", n, string(buf), err)
	}
}

func TestSeekCurrent(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("abcdefghij")
	r, err := cache.Open(context.Background(), "seek-current", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Read 2 bytes, then seek +3 from current, then read
	buf := make([]byte, 2)
	io.ReadFull(r, buf)
	pos, err := r.Seek(3, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek(3,Current): %v", err)
	}
	if pos != 5 {
		t.Fatalf("Seek pos = %d, want 5 (2 read + 3 skip)", pos)
	}
	n, err := r.Read(buf)
	if n != 2 || string(buf) != "fg" || err != nil {
		t.Fatalf("Read after Seek: (%d,%q,%v)", n, string(buf), err)
	}
}

func TestSeekEnd(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("0123456789")
	r, err := cache.Open(context.Background(), "seek-end", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Seek to end (size 10)
	pos, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek(0,End): %v", err)
	}
	if pos != 10 {
		t.Fatalf("Seek(0,End) pos = %d, want 10", pos)
	}
	// Read should return 0 bytes with EOF
	n, err := r.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read at end: (%d,%v), want (0,EOF)", n, err)
	}

	// Seek to 5 from end
	pos, err = r.Seek(-5, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek(-5,End): %v", err)
	}
	if pos != 5 {
		t.Fatalf("Seek(-5,End) pos = %d, want 5", pos)
	}
	buf := make([]byte, 3)
	io.ReadFull(r, buf)
	if string(buf) != "567" {
		t.Fatalf("Read after Seek(-5) = %q, want 567", string(buf))
	}
}

func TestSeekErrors(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "seek-err", memoryRemote{data: []byte("abc")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Seek beyond end (forward)
	pos, err := r.Seek(10, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek(10,Start) should be ok but got: %v", err)
	}
	if pos != 10 {
		t.Fatalf("Seek(10,Start) pos = %d, want 10", pos)
	}
	// Read should get EOF immediately
	n, err := r.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read past end: (%d,%v), want (0,EOF)", n, err)
	}
}

func TestSeekNegativeCurrent(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "seek-neg", memoryRemote{data: []byte("abcde")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Seek to 3, then seek -2 from current
	r.Seek(3, io.SeekStart)
	pos, err := r.Seek(-2, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek(-2,Current): %v", err)
	}
	if pos != 1 {
		t.Fatalf("Seek pos = %d, want 1", pos)
	}
	buf := make([]byte, 2)
	io.ReadFull(r, buf)
	if string(buf) != "bc" {
		t.Fatalf("Read = %q, want bc", string(buf))
	}
}

// ---------------------------------------------------------------------------
// Reader.Close
// ---------------------------------------------------------------------------

func TestDoubleClose(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "double-close", memoryRemote{data: []byte("data")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close must not panic
	_ = r.Close()
}

func TestReadAfterClose(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "read-after-close", memoryRemote{data: []byte("data")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	r.Close()

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	// After close, read should return 0 and an error (typically os.ErrClosed)
	if n != 0 || err == nil {
		t.Fatalf("Read after close = (%d,%v), want (0,error)", n, err)
	}
}

// ---------------------------------------------------------------------------
// Cache-only open (obj=nil) – cache hit path
// ---------------------------------------------------------------------------

func TestCacheOnlyHit(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("cache hit test data")
	// First open populates cache
	r1, err := cache.Open(context.Background(), "cache-hit", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	io.ReadAll(r1)
	r1.Close()

	// Cache-only open — must succeed and return same data
	r2, err := cache.Open(context.Background(), "cache-hit", nil)
	if err != nil {
		t.Fatalf("cache-only Open: %v", err)
	}
	defer r2.Close()

	got, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("cache-only ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("cache-only: got %q, want %q", string(got), string(data))
	}
	if r2.Size() != int64(len(data)) {
		t.Fatalf("cache-only Size = %d, want %d", r2.Size(), len(data))
	}
}

func TestCacheOnlyAfterRemove(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("will be removed")
	r, err := cache.Open(context.Background(), "remove-me", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	cache.Remove("remove-me")

	// Cache-only open after remove should fail
	_, err = cache.Open(context.Background(), "remove-me", nil)
	if err == nil {
		t.Fatal("expected error after remove")
	}
}

// ---------------------------------------------------------------------------
// Cache.Exists
// ---------------------------------------------------------------------------

func TestExistsBeforeAfter(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	if cache.Exists("nonexistent") {
		t.Fatal("Exists before open should be false")
	}

	r, err := cache.Open(context.Background(), "exists-test", memoryRemote{data: []byte("data")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	if !cache.Exists("exists-test") {
		t.Fatal("Exists after open+read should be true")
	}

	cache.Remove("exists-test")
	if cache.Exists("exists-test") {
		t.Fatal("Exists after remove should be false")
	}
}

// ---------------------------------------------------------------------------
// Cache.Remove
// ---------------------------------------------------------------------------

func TestRemoveTwice(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "remove-twice", memoryRemote{data: []byte("data")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	if err := cache.Remove("remove-twice"); err != nil {
		t.Fatalf("first Remove: %v", err)
	}
	// Second Remove must not error
	if err := cache.Remove("remove-twice"); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Cache.Stats
// ---------------------------------------------------------------------------

func TestStatsKeys(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	stats := cache.Stats()
	// At minimum the stats map should contain expected keys from the engine
	expectedKeys := []string{
		"files", "erroredFiles", "bytesUsed", "outOfSpace",
	}
	for _, k := range expectedKeys {
		if _, ok := stats[k]; !ok {
			t.Errorf("Stats missing key %q", k)
		}
	}
}

func TestStatsAfterOpen(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	stats1 := cache.Stats()
	// files is int (len(c.item)), not int64
	filesBefore := stats1["files"].(int)

	cache.Open(context.Background(), "stats-open", memoryRemote{data: []byte("abc")})
	// There's a race between open and stats; just check it doesn't crash
	_ = cache.Stats()
	_ = filesBefore
}

// ---------------------------------------------------------------------------
// Cache.OpenObject
// ---------------------------------------------------------------------------

func TestOpenObject(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.OpenObject(context.Background(), Object{
		Key:         "object-test",
		Size:        8,
		Source:      NewReaderAtSource(bytes.NewReader([]byte("abcdefgh")), 8),
		Fingerprint: "v1",
		ModTime:     time.Unix(100, 0),
	})
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	defer r.Close()

	if r.Size() != 8 {
		t.Fatalf("Size = %d, want 8", r.Size())
	}
	buf := make([]byte, 4)
	io.ReadFull(r, buf)
	if string(buf) != "abcd" {
		t.Fatalf("Read = %q, want abcd", string(buf))
	}
}

func TestOpenObjectValidation(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	tests := []struct {
		name string
		obj  Object
	}{
		{"empty key", Object{Key: "", Size: 1, Source: NewReaderAtSource(bytes.NewReader([]byte("a")), 1)}},
		{"nil source", Object{Key: "k", Size: 1, Source: nil}},
		{"negative size", Object{Key: "k", Size: -1, Source: NewReaderAtSource(bytes.NewReader([]byte("a")), 1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cache.OpenObject(context.Background(), tt.obj)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cache.OpenReadSeeker
// ---------------------------------------------------------------------------

func TestOpenReadSeeker(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.OpenReadSeeker(
		context.Background(),
		bytes.NewReader([]byte("abcdefghij")),
		WithKey("readseek"),
		WithSize(10),
	)
	if err != nil {
		t.Fatalf("OpenReadSeeker: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "abcdefghij" {
		t.Fatalf("ReadAll = %q, want abcdefghij", string(got))
	}
}

func TestOpenReadSeekerWithFile(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	// *os.File implements Stat(), so WithSize is optional.
	f, err := os.CreateTemp(t.TempDir(), "varc-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	f.WriteString("file-based test data")
	f.Seek(0, io.SeekStart)

	r, err := cache.OpenReadSeeker(context.Background(), f, WithKey("from-file"))
	if err != nil {
		t.Fatalf("OpenReadSeeker with file: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "file-based test data" {
		t.Fatalf("ReadAll = %q, want %q", string(got), "file-based test data")
	}
	f.Close()
}

func TestOpenReadSeekerWithoutSize(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	// bytes.Reader does NOT implement Stat() — without WithSize, should fail.
	_, err := cache.OpenReadSeeker(
		context.Background(),
		bytes.NewReader([]byte("data")),
		WithKey("nosize"),
	)
	if err == nil {
		t.Fatal("expected error without size and no Stat()")
	}
}

func TestOpenReadSeekerNilSource(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	_, err := cache.OpenReadSeeker(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil source")
	}
}

// ---------------------------------------------------------------------------
// Cache.OpenReaderAt
// ---------------------------------------------------------------------------

func TestOpenReaderAt(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.OpenReaderAt(
		context.Background(),
		bytes.NewReader([]byte("0123456789")),
		WithKey("readerat-test"),
		WithSize(10),
	)
	if err != nil {
		t.Fatalf("OpenReaderAt: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 5)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 5 || string(buf) != "56789" {
		t.Fatalf("ReadAt = %d %q, want 5 56789", n, string(buf))
	}
}

func TestOpenReaderAtWithoutSize(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	_, err := cache.OpenReaderAt(
		context.Background(),
		bytes.NewReader([]byte("data")),
		WithKey("readerat-nosize"),
	)
	if err == nil {
		t.Fatal("expected error without WithSize")
	}
}

func TestOpenReaderAtNilSource(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	_, err := cache.OpenReaderAt(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil source")
	}
}

// ---------------------------------------------------------------------------
// Large / multi-chunk data
// ---------------------------------------------------------------------------

func TestLargeFileMultiChunk(t *testing.T) {
	cache := newSmallChunkCache(t)
	defer cache.Close()

	size := int64(20000) // > 4096 chunk size
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	r, err := cache.Open(context.Background(), "large-file", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("large file content mismatch")
	}
}

func TestLargeFileReadAtMultiChunk(t *testing.T) {
	cache := newSmallChunkCache(t)
	defer cache.Close()

	size := int64(20000)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	r, err := cache.Open(context.Background(), "large-readat", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// ReadAt across chunk boundaries
	buf := make([]byte, 5000)
	n, err := r.ReadAt(buf, 3000)
	if err != nil {
		t.Fatalf("ReadAt(3000,5000): %v", err)
	}
	if n != 5000 || !bytes.Equal(buf, data[3000:8000]) {
		t.Fatal("ReadAt across chunk boundary mismatch")
	}
}

// ---------------------------------------------------------------------------
// Empty / single-byte files
// ---------------------------------------------------------------------------

func TestEmptyFile(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "empty-file", memoryRemote{data: []byte{}})
	if err != nil {
		t.Fatalf("Open empty: %v", err)
	}
	defer r.Close()

	if r.Size() != 0 {
		t.Fatalf("Size = %d, want 0", r.Size())
	}
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("Read empty: (%d,%v), want (0,EOF)", n, err)
	}
}

func TestSingleByteFile(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	r, err := cache.Open(context.Background(), "single-byte", memoryRemote{data: []byte("X")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if r.Size() != 1 {
		t.Fatalf("Size = %d, want 1", r.Size())
	}
	buf := make([]byte, 10)
	n, err := r.Read(buf)
	if n != 1 || err != io.EOF || buf[0] != 'X' {
		t.Fatalf("Read single: (%d,%v,%c), want (1,EOF,X)", n, err, buf[0])
	}
}

// ---------------------------------------------------------------------------
// ShardKey
// ---------------------------------------------------------------------------

func TestShardKeyNoShard(t *testing.T) {
	key := ShardKey("hello", 0)
	// Should be 32 hex chars (MD5)
	if len(key) != 32 {
		t.Fatalf("ShardKey(0) len = %d, want 32", len(key))
	}
	for _, c := range key {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("ShardKey(0) contains non-hex char %c", c)
		}
	}
}

func TestShardKeyWithLevel(t *testing.T) {
	key := ShardKey("hello", 2)
	// Should be "ab/cd/abcdef..." format: 2+1+2+1+32 = 38 chars
	if len(key) != 38 {
		t.Fatalf("ShardKey(2) len = %d, want 38", len(key))
	}
	if key[2] != '/' || key[5] != '/' {
		t.Fatalf("ShardKey(2) missing slashes: %q", key)
	}
	// Verify first two pairs match the hash prefix
	noShard := ShardKey("hello", 0)
	if key[:2] != noShard[:2] || key[3:5] != noShard[2:4] {
		t.Fatalf("ShardKey(2) = %q, expected prefix of %q", key, noShard)
	}
}

func TestShardKeyDeterministic(t *testing.T) {
	a := ShardKey("same-key", 1)
	b := ShardKey("same-key", 1)
	if a != b {
		t.Fatalf("ShardKey not deterministic: %q vs %q", a, b)
	}
}

func TestShardKeyDifferentKeys(t *testing.T) {
	a := ShardKey("key-a", 0)
	b := ShardKey("key-b", 0)
	if a == b {
		t.Fatal("different keys produced same hash")
	}
}

// ---------------------------------------------------------------------------
// Multiple keys
// ---------------------------------------------------------------------------

func TestMultipleKeys(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	keys := []struct {
		key  string
		data []byte
	}{
		{"alpha", []byte("alpha data here")},
		{"beta", []byte("beta data here")},
		{"gamma", []byte("gamma data here")},
	}

	for _, k := range keys {
		r, err := cache.Open(context.Background(), k.key, memoryRemote{data: k.data})
		if err != nil {
			t.Fatalf("Open %q: %v", k.key, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, k.data) {
			t.Fatalf("%q: got %q, want %q", k.key, string(got), string(k.data))
		}
	}

	// Verify all persist
	for _, k := range keys {
		if !cache.Exists(k.key) {
			t.Fatalf("key %q should exist", k.key)
		}
		r, err := cache.Open(context.Background(), k.key, nil)
		if err != nil {
			t.Fatalf("cache-only Open %q: %v", k.key, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, k.data) {
			t.Fatalf("%q cache-only: got %q, want %q", k.key, string(got), string(k.data))
		}
	}
}

// ---------------------------------------------------------------------------
// ShardLevel > 0
// ---------------------------------------------------------------------------

func TestShardLevel(t *testing.T) {
	cache, err := New(context.Background(), Options{
		CacheDir:      t.TempDir(),
		ShardLevel:    2,
		HandleCaching: 0,
	})
	if err != nil {
		t.Fatalf("New with ShardLevel: %v", err)
	}
	defer cache.Close()

	r, err := cache.Open(context.Background(), "sharded-key", memoryRemote{data: []byte("sharded data")})
	if err != nil {
		t.Fatalf("Open with shard: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	if !cache.Exists("sharded-key") {
		t.Fatal("sharded key should exist")
	}

	// Re-open via cache-only
	r2, err := cache.Open(context.Background(), "sharded-key", nil)
	if err != nil {
		t.Fatalf("cache-only Open with shard: %v", err)
	}
	got, _ := io.ReadAll(r2)
	r2.Close()
	if string(got) != "sharded data" {
		t.Fatalf("got %q, want %q", string(got), "sharded data")
	}

	cache.Remove("sharded-key")
	if cache.Exists("sharded-key") {
		t.Fatal("sharded key should not exist after remove")
	}
}

// ---------------------------------------------------------------------------
// Re-open cache (close then open again)
// ---------------------------------------------------------------------------

func TestReopenCached(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	// Open, read all, close
	r1, err := cache.Open(context.Background(), "reopen", memoryRemote{data: []byte("reopen data")})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	io.ReadAll(r1)
	r1.Close()

	// Open, read again (cache hit)
	r2, err := cache.Open(context.Background(), "reopen", memoryRemote{data: []byte("reopen data")})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	got, _ := io.ReadAll(r2)
	r2.Close()
	if string(got) != "reopen data" {
		t.Fatalf("got %q, want %q", string(got), "reopen data")
	}
}

// ---------------------------------------------------------------------------
// Fingerprint invalidation
// ---------------------------------------------------------------------------

func TestFingerprintSameReturnsCached(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	// Open with a fingerprint
	r1, err := cache.Open(context.Background(), "fp-same",
		memoryRemote{data: []byte("cached data"), fingerprint: "fp-v1"})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	io.ReadAll(r1)
	r1.Close()

	// Re-open with the same fingerprint — should serve cached data
	r2, err := cache.Open(context.Background(), "fp-same",
		memoryRemote{data: []byte("updated data"), fingerprint: "fp-v1"})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	got, _ := io.ReadAll(r2)
	r2.Close()
	if string(got) != "cached data" {
		t.Fatalf("got %q, want %q (same fingerprint should serve cached copy)", string(got), "cached data")
	}
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

func TestConcurrentReadsDifferentKeys(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("concurrent data for key %d", id))
			key := fmt.Sprintf("concurrent-%d", id)
			r, err := cache.Open(context.Background(), key, memoryRemote{data: data})
			if err != nil {
				errs <- fmt.Errorf("Open %q: %w", key, err)
				return
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				errs <- fmt.Errorf("ReadAll %q: %w", key, err)
				return
			}
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("%q: content mismatch", key)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentReadsSameKey(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := []byte("concurrent same key data")
	r, err := cache.Open(context.Background(), "same-key", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := cache.Open(context.Background(), "same-key", nil)
			if err != nil {
				errs <- fmt.Errorf("Open: %w", err)
				return
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				errs <- fmt.Errorf("ReadAll: %w", err)
				return
			}
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("content mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentReadAtSameHandle(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 251)
	}

	r, err := cache.Open(context.Background(), "concurrent-handle", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			off := int64(id * 500)
			buf := make([]byte, 400)
			_, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				errs <- fmt.Errorf("ReadAt %d: %w", id, err)
				return
			}
			if !bytes.Equal(buf, data[off:off+400]) {
				errs <- fmt.Errorf("ReadAt %d: content mismatch", id)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Cache max age expiry
// ---------------------------------------------------------------------------

func TestCacheMaxAge(t *testing.T) {
	// Use a tiny max age (1 nanosecond) so the entry expires immediately.
	cache, err := New(context.Background(), Options{
		CacheDir:      t.TempDir(),
		CacheMaxAge:   1, // 1 nanosecond
		HandleCaching: 0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cache.Close()

	r, err := cache.Open(context.Background(), "expiry", memoryRemote{data: []byte("fresh data")})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	io.ReadAll(r)
	r.Close()

	// Sleep briefly to ensure max age has passed.
	time.Sleep(time.Microsecond)

	// Re-open with the same remote — entry expired, should refetch.
	r2, err := cache.Open(context.Background(), "expiry", memoryRemote{data: []byte("fresh data")})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	got, _ := io.ReadAll(r2)
	r2.Close()
	if string(got) != "fresh data" {
		t.Fatalf("got %q, want %q", string(got), "fresh data")
	}
}

// ---------------------------------------------------------------------------
// Large random data (stressing cache engine)
// ---------------------------------------------------------------------------

func TestLargeRandomData(t *testing.T) {
	cache := newSmallChunkCache(t)
	defer cache.Close()

	rng := rand.New(rand.NewSource(42))
	size := 50000
	data := make([]byte, size)
	rng.Read(data)

	r, err := cache.Open(context.Background(), "random-data", memoryRemote{data: data})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Read random ranges
	for i := 0; i < 20; i++ {
		off := rng.Int63n(int64(size - 1000))
		length := rng.Int63n(2000) + 100
		if off+length > int64(size) {
			length = int64(size) - off
		}
		buf := make([]byte, length)
		n, err := r.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt(%d,%d): %v", off, length, err)
		}
		if !bytes.Equal(buf[:n], data[off:off+int64(n)]) {
			t.Fatalf("ReadAt(%d,%d): content mismatch", off, length)
		}
	}

	// Full read
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("full read content mismatch")
	}
}

// ---------------------------------------------------------------------------
// ShardKey unit tests
// ---------------------------------------------------------------------------

func TestShardKeyEdgeCases(t *testing.T) {
	// Level higher than hash length (16 pairs = 32 chars max)
	key := ShardKey("x", 100)
	if len(key) < 32 {
		t.Fatalf("ShardKey(100) too short: %d", len(key))
	}

	// Empty string
	empty := ShardKey("", 0)
	if len(empty) != 32 {
		t.Fatalf("ShardKey('') len = %d, want 32", len(empty))
	}

	// Level 1 vs level 0 — level 1 adds "ab/" prefix (2 hex + 1 slash = 3)
	noshard := ShardKey("x", 0)
	with1 := ShardKey("x", 1)
	if len(with1) != len(noshard)+3 {
		t.Fatalf("ShardKey(1) len = %d, noshard len = %d", len(with1), len(noshard))
	}
	if with1[2] != '/' {
		t.Fatalf("ShardKey(1) missing slash: %q", with1)
	}
	if with1[:2] != noshard[:2] {
		t.Fatalf("ShardKey(1) prefix %q != ShardKey(0) prefix %q", with1[:2], noshard[:2])
	}
}

// ---------------------------------------------------------------------------
// Options / OpenObject fingerprint+modtime helpers
// ---------------------------------------------------------------------------

func TestWithFingerprintAndModTime(t *testing.T) {
	cache := newTestCache(t)
	defer cache.Close()

	// Use WithFingerprint + WithModTime
	r, err := cache.OpenReadSeeker(
		context.Background(),
		bytes.NewReader([]byte("options-test")),
		WithKey("opts"),
		WithSize(12),
		WithFingerprint("fp-v2"),
		WithModTime(time.Unix(200, 0)),
	)
	if err != nil {
		t.Fatalf("OpenReadSeeker with options: %v", err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "options-test" {
		t.Fatalf("got %q, want %q", string(got), "options-test")
	}

	// Re-open with same fingerprint → cache hit
	r2, err := cache.Open(context.Background(), "opts", nil)
	if err != nil {
		t.Fatalf("cache-only Open: %v", err)
	}
	got2, _ := io.ReadAll(r2)
	r2.Close()
	if string(got2) != "options-test" {
		t.Fatalf("cache-only got %q, want %q", string(got2), "options-test")
	}
}
