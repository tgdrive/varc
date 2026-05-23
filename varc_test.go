package varc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

type memoryRemote struct {
	data []byte
}

func (r memoryRemote) Open(ctx context.Context, options ...OpenOption) (io.ReadCloser, error) {
	start, end := int64(0), int64(len(r.data)-1)
	for _, option := range options {
		key, value := option.Header()
		if key == "Range" {
			_, _ = key, value
			var parsedStart, parsedEnd int64
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &parsedStart, &parsedEnd); err == nil {
				start, end = parsedStart, parsedEnd
			} else if _, err := fmt.Sscanf(value, "bytes=%d-", &parsedStart); err == nil {
				start, end = parsedStart, int64(len(r.data)-1)
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

func (r memoryRemote) Size() int64                           { return int64(len(r.data)) }
func (r memoryRemote) String() string                        { return "memory" }
func (r memoryRemote) Fingerprint() string                   { return "memory-fingerprint" }
func (r memoryRemote) ModTime(ctx context.Context) time.Time { return time.Unix(1, 0) }

func TestCacheOpenAsPackage(t *testing.T) {
	cache, err := New(context.Background(), Options{CacheDir: t.TempDir(), HandleCaching: 0})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer cache.Close()

	reader, err := cache.Open(context.Background(), "video.bin", memoryRemote{data: []byte("0123456789")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	if reader.Size() != 10 {
		t.Fatalf("Size() = %d, want 10", reader.Size())
	}

	buf := make([]byte, 4)
	n, err := reader.ReadAt(buf, 3)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if n != 4 || string(buf) != "3456" {
		t.Fatalf("ReadAt() = %d %q, want 4 %q", n, string(buf), "3456")
	}
}

func TestCacheOpenReadSeekerWithCustomKey(t *testing.T) {
	cache, err := New(context.Background(), Options{CacheDir: t.TempDir(), HandleCaching: 0})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer cache.Close()

	reader, err := cache.OpenReadSeeker(
		context.Background(),
		bytes.NewReader([]byte("abcdefghij")),
		WithKey("custom/readseeker-key"),
		WithSize(10),
	)
	if err != nil {
		t.Fatalf("OpenReadSeeker() error = %v", err)
	}

	buf := make([]byte, 3)
	n, err := reader.ReadAt(buf, 4)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if n != 3 || string(buf) != "efg" {
		t.Fatalf("ReadAt() = %d %q, want 3 %q", n, string(buf), "efg")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	cached, err := cache.Open(context.Background(), "custom/readseeker-key", nil)
	if err != nil {
		t.Fatalf("cache-only Open() error = %v", err)
	}
	defer cached.Close()

	buf = make([]byte, 3)
	n, err = cached.ReadAt(buf, 4)
	if err != nil {
		t.Fatalf("cache-only ReadAt() error = %v", err)
	}
	if n != 3 || string(buf) != "efg" {
		t.Fatalf("cache-only ReadAt() = %d %q, want 3 %q", n, string(buf), "efg")
	}
}

func TestCacheOpenReaderAt(t *testing.T) {
	cache, err := New(context.Background(), Options{CacheDir: t.TempDir(), HandleCaching: 0})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer cache.Close()

	reader, err := cache.OpenReaderAt(
		context.Background(),
		bytes.NewReader([]byte("0123456789")),
		WithKey("custom/readerat-key"),
		WithSize(10),
		WithFingerprint("readerat-v1"),
	)
	if err != nil {
		t.Fatalf("OpenReaderAt() error = %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 5)
	n, err := reader.ReadAt(buf, 5)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if n != 5 || string(buf) != "56789" {
		t.Fatalf("ReadAt() = %d %q, want 5 %q", n, string(buf), "56789")
	}
}

func TestCacheOnlyOpenDoesNotDeleteFingerprintedObject(t *testing.T) {
	cache, err := New(context.Background(), Options{CacheDir: t.TempDir(), HandleCaching: 0})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer cache.Close()

	reader, err := cache.OpenObject(context.Background(), Object{
		Key:         "fingerprinted/object",
		Size:        10,
		Source:      NewReaderAtSource(bytes.NewReader([]byte("0123456789")), 10),
		Fingerprint: "etag-v1",
		ModTime:     time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 2); err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	cached, err := cache.Open(context.Background(), "fingerprinted/object", nil)
	if err != nil {
		t.Fatalf("cache-only Open() error = %v", err)
	}
	defer cached.Close()

	buf = make([]byte, 4)
	n, err := cached.ReadAt(buf, 2)
	if err != nil {
		t.Fatalf("cache-only ReadAt() error = %v", err)
	}
	if n != 4 || string(buf) != "2345" {
		t.Fatalf("cache-only ReadAt() = %d %q, want 4 %q", n, string(buf), "2345")
	}
}
