package varc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingSource struct {
	data      []byte
	reads     atomic.Int64
	readBytes atomic.Int64
	delay     time.Duration
	failAt    atomic.Int64
}

func newCountingSource(size int) *countingSource {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + 17) % 251)
	}
	cs := &countingSource{data: data}
	cs.failAt.Store(-1)
	return cs
}

func (s *countingSource) ReadAt(p []byte, off int64) (int, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if fail := s.failAt.Load(); fail >= 0 && off >= fail {
		return 0, errors.New("forced source failure")
	}
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[off:])
	s.reads.Add(1)
	s.readBytes.Add(int64(n))
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func testOptions(dir string) Options {
	return Options{
		CacheDir:          dir,
		BlockSize:         1024,
		ChunkSize:         4096,
		ChunkStreams:      8,
		MaxInflightBytes:  1 << 20,
		ReadAhead:         0,
		NoBackground:      true,
		ReadRetryCount:    0,
		CacheMaxAge:       -1,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		TouchInterval:     -1,
	}
}

func openTestCache(t *testing.T) (*Cache, string) {
	t.Helper()
	dir := t.TempDir()
	c, err := New(context.Background(), testOptions(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, dir
}

func TestReadThroughAndCacheOnly(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(32 * 1024)
	r, err := c.Open(context.Background(), "movie.mkv", int64(len(src.data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 3000)
	n, err := r.ReadAt(buf, 2000)
	if err != nil {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, src.data[2000:5000]) {
		t.Fatal("read-through bytes mismatch")
	}
	if got := src.reads.Load(); got == 0 {
		t.Fatal("source was not read")
	}
	_ = r.Close()

	cached, err := c.Open(context.Background(), "movie.mkv", -1, nil)
	if err != nil {
		t.Fatalf("cache-only open: %v", err)
	}
	defer cached.Close()
	zero := make([]byte, 3000)
	n, err = cached.ReadAt(zero, 2000)
	if err != nil {
		t.Fatalf("cache-only read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(zero, src.data[2000:5000]) {
		t.Fatal("cache-only bytes mismatch")
	}

	missing := make([]byte, 1024)
	_, err = cached.ReadAt(missing, 20*1024)
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expected cache miss, got %v", err)
	}
}

func TestSequentialReadSeekAndEOF(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(5000)
	r, err := c.Open(context.Background(), "seq", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pos, err := r.Seek(4900, io.SeekStart)
	if err != nil || pos != 4900 {
		t.Fatalf("Seek got pos=%d err=%v", pos, err)
	}
	buf := make([]byte, 200)
	n, err := r.Read(buf)
	if n != 100 || err != io.EOF {
		t.Fatalf("tail read n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf[:100], src.data[4900:]) {
		t.Fatal("tail bytes mismatch")
	}
	pos, err = r.Seek(-10, io.SeekCurrent)
	if err != nil || pos != 4990 {
		t.Fatalf("relative seek pos=%d err=%v", pos, err)
	}
}

func TestFingerprintInvalidatesStaleData(t *testing.T) {
	c, _ := openTestCache(t)
	src1 := newCountingSource(16 * 1024)
	r1, err := c.Open(context.Background(), "asset", int64(len(src1.data)), src1, WithFingerprint("old"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r1.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r1.Close()

	src2 := newCountingSource(16 * 1024)
	for i := range src2.data {
		src2.data[i] ^= 0xff
	}
	r2, err := c.Open(context.Background(), "asset", int64(len(src2.data)), src2, WithFingerprint("new"))
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	buf := make([]byte, 4096)
	if _, err := r2.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, src2.data[:4096]) {
		t.Fatal("fingerprint invalidation did not replace old data")
	}
}

func TestConcurrentReadersCoalesceDownloads(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(128 * 1024)
	src.delay = time.Millisecond
	r, err := c.Open(context.Background(), "concurrent", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	var failed atomic.Bool
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for j := 0; j < 50; j++ {
				off := int64(rnd.Intn(len(src.data) - 2048))
				buf := make([]byte, 2048)
				n, err := r.ReadAt(buf, off)
				if err != nil || n != len(buf) || !bytes.Equal(buf, src.data[off:off+2048]) {
					failed.Store(true)
					return
				}
			}
		}(int64(i + 1))
	}
	wg.Wait()
	if failed.Load() {
		t.Fatal("concurrent read failed")
	}
	if src.reads.Load() > 512 {
		t.Fatalf("too many source reads; coalescing likely broken: %d", src.reads.Load())
	}
}

func TestWarmAllCompletesCoverage(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(10 * 1024)
	r, err := c.Open(context.Background(), "warm", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	cached, size, complete, err := c.Coverage("warm")
	if err != nil {
		t.Fatal(err)
	}
	if cached != int64(len(src.data)) || size != int64(len(src.data)) || !complete {
		t.Fatalf("bad coverage cached=%d size=%d complete=%v", cached, size, complete)
	}
	if !r.Complete() {
		t.Fatal("reader did not report complete")
	}
}

func TestMetadataAttributes(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "attrs", int64(len(src.data)), src, WithAttr("mime", "video/mp4"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got, ok := r.Attr("mime"); !ok || got != "video/mp4" {
		t.Fatalf("missing attr got=%q ok=%v", got, ok)
	}
	if err := r.SetAttr("remote", "/bucket/attrs"); err != nil {
		t.Fatal(err)
	}
	if got, ok := r.Attr("remote"); !ok || got != "/bucket/attrs" {
		t.Fatalf("set attr got=%q ok=%v", got, ok)
	}
	if err := r.RemoveAttr("remote"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Attr("remote"); ok {
		t.Fatal("attribute was not removed")
	}
}

func TestRemoveAndExists(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(8192)
	r, err := c.Open(context.Background(), "remove", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if !c.Exists("remove") {
		t.Fatal("entry should exist")
	}
	if err := c.Remove("remove"); err != nil {
		t.Fatal(err)
	}
	if c.Exists("remove") {
		t.Fatal("entry still exists after remove")
	}
}

func TestPruneMaxSize(t *testing.T) {
	c, _ := openTestCache(t)
	c.cacheMaxSize = 4096
	for i := 0; i < 3; i++ {
		src := newCountingSource(8192)
		r, err := c.Open(context.Background(), string(rune('a'+i)), int64(len(src.data)), src)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.WarmAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = r.Close()
		time.Sleep(time.Millisecond)
	}
	stats, err := c.Prune(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Removed == 0 || stats.ReasonSize == 0 {
		t.Fatalf("expected size prune, got %+v", stats)
	}
	_, bytesUsed := c.scanDataUsage()
	if bytesUsed > c.cacheMaxSize && stats.Removed < 3 {
		t.Fatalf("cache still too large: %d", bytesUsed)
	}
}

func TestListEntriesAndVerify(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "list", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	entries, err := c.ListEntries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries=%d", len(entries))
	}
	if entries[0].Key != "list" || !entries[0].Complete || entries[0].CachedBytes != int64(len(src.data)) {
		t.Fatalf("bad entry: %+v", entries[0])
	}
	verify, err := c.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if verify.Complete != 1 || verify.Entries != 1 {
		t.Fatalf("bad verify: %+v", verify)
	}
}

func TestRenameKey(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "old", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if err := c.RenameKey("old", "new"); err != nil {
		t.Fatal(err)
	}
	if c.Exists("old") || !c.Exists("new") {
		t.Fatalf("rename existence old=%v new=%v", c.Exists("old"), c.Exists("new"))
	}
	cached, size, complete, err := c.Coverage("new")
	if err != nil || cached != int64(len(src.data)) || size != int64(len(src.data)) || !complete {
		t.Fatalf("bad coverage after rename cached=%d size=%d complete=%v err=%v", cached, size, complete, err)
	}
}

func TestCopyTo(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(33 * 1024)
	r, err := c.Open(context.Background(), "copy", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out bytes.Buffer
	n, err := r.CopyTo(context.Background(), &out)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(src.data)) || !bytes.Equal(out.Bytes(), src.data) {
		t.Fatal("copy mismatch")
	}
}

func TestContextCancellation(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(64 * 1024)
	src.delay = 20 * time.Millisecond
	r, err := c.Open(context.Background(), "cancel", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	buf := make([]byte, 4096)
	_, err = r.ReadAtContext(ctx, buf, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled, got %v", err)
	}
}

func TestChecksumVerificationDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.VerifyChecksum = true
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "sum", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	f, err := os.OpenFile(c.KeyPath("sum"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{99}, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	stats, err := c.Verify(context.Background())
	if err == nil || stats.ChecksumErrors == 0 {
		t.Fatalf("expected checksum error, stats=%+v err=%v", stats, err)
	}
}

func TestShardKeyShape(t *testing.T) {
	key := ShardKey("abc", 2)
	if stringsCount(key, string(os.PathSeparator)) != 2 {
		t.Fatalf("bad shard key %q", key)
	}
	flat := ShardKey("abc", 0)
	if stringsCount(flat, string(os.PathSeparator)) != 0 {
		t.Fatalf("flat shard key has separator: %q", flat)
	}
}

func stringsCount(s, sub string) int {
	count := 0
	for {
		i := bytes.Index([]byte(s), []byte(sub))
		if i < 0 {
			return count
		}
		count++
		s = s[i+len(sub):]
	}
}

func TestMetaSnapshotAndFileInfo(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "dir/file.bin", int64(len(src.data)), src, WithModTime(time.Unix(100, 0)))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	snap, err := c.SnapshotMeta("dir/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if snap["key"] != "dir/file.bin" {
		t.Fatalf("bad snapshot key: %#v", snap["key"])
	}
	fi, err := c.FileInfo("dir/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "file.bin" || fi.Size() != int64(len(src.data)) {
		t.Fatalf("bad fileinfo: name=%s size=%d", fi.Name(), fi.Size())
	}
}

func TestWaitCompleteObservesActiveDownload(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(64 * 1024)
	src.delay = time.Millisecond
	r, err := c.Open(context.Background(), "wait", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	done := make(chan error, 1)
	go func() { done <- r.WarmAll(context.Background()) }()
	time.Sleep(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.WaitComplete(ctx, "wait"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCacheOnlyOpenFailsWithoutMeta(t *testing.T) {
	c, _ := openTestCache(t)
	_, err := c.Open(context.Background(), "missing", -1, nil)
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expected cache miss, got %v", err)
	}
}

func TestInvalidRange(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(1024)
	r, err := c.Open(context.Background(), "invalid", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, 1)
	_, err = r.ReadAt(buf, -1)
	if !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("expected invalid range, got %v", err)
	}
}

func TestPruneInvalidMeta(t *testing.T) {
	c, dir := openTestCache(t)
	bad := filepath.Join(dir, "bad.meta")
	if err := os.WriteFile(bad, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	stats, _ := c.Prune(context.Background())
	if stats.ReasonInvalid == 0 {
		t.Fatalf("expected invalid meta prune: %+v", stats)
	}
}
