package varc

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type readCall struct {
	off int64
	len int
}

type trackedReaderAt struct {
	mu    sync.Mutex
	data  []byte
	calls []readCall
}

type blockingReaderAt struct {
	mu      sync.Mutex
	size    int64
	calls   []readCall
	unblock chan struct{}
}

func (r *trackedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.calls = append(r.calls, readCall{off: off, len: len(p)})
	r.mu.Unlock()

	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *trackedReaderAt) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *trackedReaderAt) callSnapshot() []readCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := make([]readCall, len(r.calls))
	copy(calls, r.calls)
	return calls
}

func (r *blockingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.calls = append(r.calls, readCall{off: off, len: len(p)})
	callIndex := len(r.calls)
	r.mu.Unlock()
	if callIndex > 1 {
		<-r.unblock
	}
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	n := len(p)
	if off+int64(n) > r.size {
		n = int(r.size - off)
	}
	for i := 0; i < n; i++ {
		p[i] = byte((off + int64(i)) % 251)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *blockingReaderAt) firstCall() readCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return readCall{}
	}
	return r.calls[0]
}

func testData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func repeatedStringData() []byte {
	pattern := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ|"
	var b bytes.Buffer
	for b.Len() < 256*1024 {
		b.WriteString(pattern)
	}
	return b.Bytes()
}

func readRange(t *testing.T, r *Reader, start int64, length int64) []byte {
	t.Helper()
	section := io.NewSectionReader(r, start, length)
	got, err := io.ReadAll(section)
	if err != nil {
		t.Fatalf("read range %d+%d: %v", start, length, err)
	}
	return got
}

func requireBytes(t *testing.T, got []byte, data []byte, start int64) {
	t.Helper()
	want := data[start : start+int64(len(got))]
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes at %d len %d mismatch", start, len(got))
	}
}

func newTestCache(t *testing.T) *Cache {
	t.Helper()
	return newTestCacheAt(t, t.TempDir())
}

func newTestCacheAt(t *testing.T, dir string) *Cache {
	t.Helper()
	c, err := New(context.Background(), Options{
		CacheDir:      dir,
		ChunkSize:     4096,
		HandleCaching: -1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestNewAcceptsNilContext(t *testing.T) {
	c, err := New(nil, Options{CacheDir: t.TempDir(), HandleCaching: -1})
	if err != nil {
		t.Fatalf("New(nil): %v", err)
	}
	_ = c.Close()
}

func TestOpenValidatesInputs(t *testing.T) {
	c := newTestCache(t)
	src := bytes.NewReader([]byte("abc"))

	if _, err := c.Open(context.Background(), "", 3, src); err == nil {
		t.Fatal("Open with empty key succeeded")
	}
	if _, err := c.Open(context.Background(), "negative-size", -1, src); err == nil {
		t.Fatal("Open with source and negative size succeeded")
	}
	if _, err := c.Open(context.Background(), "missing-cache-only", -1, nil); err == nil {
		t.Fatal("cache-only Open for a missing key succeeded")
	}
}

func TestReadAtAndSequentialRead(t *testing.T) {
	c := newTestCache(t)
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	r, err := c.Open(context.Background(), "basic", int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 2)
	if err != nil || n != 5 || string(buf) != "cdefg" {
		t.Fatalf("ReadAt = %d %q %v, want 5 cdefg nil", n, string(buf), err)
	}

	if _, err := r.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	buf = make([]byte, 4)
	n, err = r.Read(buf)
	if err != nil || n != 4 || string(buf) != "klmn" {
		t.Fatalf("Read after Seek = %d %q %v, want 4 klmn nil", n, string(buf), err)
	}

	if got := r.Size(); got != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", got, len(data))
	}
}

func TestReadAtEOFBehavior(t *testing.T) {
	c := newTestCache(t)
	r, err := c.Open(context.Background(), "eof", 5, bytes.NewReader([]byte("abcde")))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 3)
	if n != 2 || err == nil || string(buf[:n]) != "de" {
		t.Fatalf("ReadAt near EOF = %d %q %v, want 2 de EOF", n, string(buf[:n]), err)
	}

	n, err = r.ReadAt(buf, 5)
	if n != 0 || err == nil {
		t.Fatalf("ReadAt at EOF = %d %v, want 0 EOF", n, err)
	}
}

func TestSectionReaderServesHTTPRangeShape(t *testing.T) {
	c := newTestCache(t)
	data := []byte("0123456789abcdef")
	r, err := c.Open(context.Background(), "section", int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	section := io.NewSectionReader(r, 4, 6)
	got, err := io.ReadAll(section)
	if err != nil {
		t.Fatalf("ReadAll(section): %v", err)
	}
	if string(got) != "456789" {
		t.Fatalf("section = %q, want 456789", string(got))
	}
}

func TestStringPlaybackRangesMatchExactSubstrings(t *testing.T) {
	c := newTestCache(t)
	data := repeatedStringData()
	src := &trackedReaderAt{data: data}
	r, err := c.Open(context.Background(), "string-video", int64(len(data)), src, WithFingerprint("text-v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, tt := range []struct {
		name   string
		start  int64
		length int64
	}{
		{"start of file", 0, 73},
		{"inside first chunk", 1234, 777},
		{"crosses chunk boundary", 3900, 600},
		{"later seek target", 73 * 1024, 2048},
		{"near eof", int64(len(data) - 333), 333},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := readRange(t, r, tt.start, tt.length)
			want := data[tt.start : tt.start+tt.length]
			if string(got) != string(want) {
				t.Fatalf("range %d+%d = %q, want %q", tt.start, tt.length, string(got), string(want))
			}
		})
	}
}

func TestPlaybackSeekPatternCachesOnlyMisses(t *testing.T) {
	c := newTestCache(t)
	data := testData(2 * 1024 * 1024)
	src := &trackedReaderAt{data: data}
	r, err := c.Open(context.Background(), "video", int64(len(data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got := readRange(t, r, 0, 8192)
	requireBytes(t, got, data, 0)
	afterColdStart := src.callCount()
	if afterColdStart == 0 {
		t.Fatal("cold first playback range did not fetch from source")
	}

	got = readRange(t, r, 0, 8192)
	requireBytes(t, got, data, 0)
	if gotCalls := src.callCount(); gotCalls != afterColdStart {
		t.Fatalf("repeat of cached start range fetched source again: calls %d -> %d", afterColdStart, gotCalls)
	}

	got = readRange(t, r, 1024*1024, 8192)
	requireBytes(t, got, data, 1024*1024)
	afterForwardSeek := src.callCount()
	if afterForwardSeek <= afterColdStart {
		t.Fatalf("forward seek to uncached range did not fetch source: calls stayed %d", afterForwardSeek)
	}

	got = readRange(t, r, 0, 8192)
	requireBytes(t, got, data, 0)
	if gotCalls := src.callCount(); gotCalls != afterForwardSeek {
		t.Fatalf("backward seek to cached start fetched source again: calls %d -> %d", afterForwardSeek, gotCalls)
	}
}

func TestLargeChunkWindowDoesNotBlockSmallPlaybackRead(t *testing.T) {
	dir := t.TempDir()
	c, err := New(context.Background(), Options{
		CacheDir:      dir,
		ChunkSize:     128 * 1024 * 1024,
		BlockSize:     4096,
		HandleCaching: -1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	src := &blockingReaderAt{size: 256 * 1024 * 1024, unblock: make(chan struct{})}
	r, err := c.Open(context.Background(), "large-window", src.size, src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got := readRange(t, r, 64*1024*1024, 1024)
	for i, b := range got {
		want := byte((64*1024*1024 + int64(i)) % 251)
		if b != want {
			close(src.unblock)
			_ = r.Close()
			t.Fatalf("byte %d = %d, want %d", i, b, want)
		}
	}
	first := src.firstCall()
	close(src.unblock)
	_ = r.Close()

	if first.off != 64*1024*1024 || first.len != 4096 {
		t.Fatalf("first source fetch = off %d len %d, want off %d len 4096", first.off, first.len, int64(64*1024*1024))
	}
}

func TestReopenPerHTTPRangeRequestUsesSharedDiskCache(t *testing.T) {
	c := newTestCache(t)
	data := repeatedStringData()
	src := &trackedReaderAt{data: data}

	openAndRead := func(start, length int64) []byte {
		r, err := c.Open(context.Background(), "http-ranges", int64(len(data)), src, WithFingerprint("v1"))
		if err != nil {
			t.Fatalf("Open range %d+%d: %v", start, length, err)
		}
		defer r.Close()
		return readRange(t, r, start, length)
	}

	got := openAndRead(0, 2048)
	requireBytes(t, got, data, 0)
	afterFirstRequest := src.callCount()
	if afterFirstRequest == 0 {
		t.Fatal("first HTTP range did not fetch source")
	}

	got = openAndRead(0, 2048)
	requireBytes(t, got, data, 0)
	if gotCalls := src.callCount(); gotCalls != afterFirstRequest {
		t.Fatalf("second identical HTTP range refetched source: calls %d -> %d", afterFirstRequest, gotCalls)
	}

	got = openAndRead(128*1024, 4096)
	requireBytes(t, got, data, 128*1024)
	if gotCalls := src.callCount(); gotCalls <= afterFirstRequest {
		t.Fatalf("new HTTP seek target did not fetch source: calls stayed %d", gotCalls)
	}
}

func TestOverlappingRangesFetchOnlyMissingChunks(t *testing.T) {
	c := newTestCache(t)
	data := testData(64 * 1024)
	src := &trackedReaderAt{data: data}
	r, err := c.Open(context.Background(), "overlap", int64(len(data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got := readRange(t, r, 4096, 4096)
	requireBytes(t, got, data, 4096)
	if calls := src.callSnapshot(); len(calls) != 1 || calls[0].off != 4096 || calls[0].len != 4096 {
		t.Fatalf("first chunk calls = %+v, want one 4096-byte call at 4096", calls)
	}

	got = readRange(t, r, 6144, 4096)
	requireBytes(t, got, data, 6144)
	calls := src.callSnapshot()
	if len(calls) != 2 {
		t.Fatalf("overlap fetched wrong number of chunks: %+v", calls)
	}
	if calls[1].off != 8192 || calls[1].len != 4096 {
		t.Fatalf("overlap second fetch = %+v, want one missing chunk at 8192", calls[1])
	}
}

func TestPlaybackScrubPatternReusesCachedSeekTargets(t *testing.T) {
	c := newTestCache(t)
	data := testData(512 * 1024)
	src := &trackedReaderAt{data: data}
	r, err := c.Open(context.Background(), "scrub", int64(len(data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, off := range []int64{32 * 1024, 96 * 1024, 160 * 1024, 224 * 1024} {
		got := readRange(t, r, off, 4096)
		requireBytes(t, got, data, off)
	}
	afterScrub := src.callCount()
	if afterScrub < 4 {
		t.Fatalf("scrub pattern produced too few source calls: %d", afterScrub)
	}

	got := readRange(t, r, 96*1024, 4096)
	requireBytes(t, got, data, 96*1024)
	if gotCalls := src.callCount(); gotCalls != afterScrub {
		t.Fatalf("repeat scrub target fetched source again: calls %d -> %d", afterScrub, gotCalls)
	}
}

func TestCrossChunkRangeReturnsCorrectBytes(t *testing.T) {
	c := newTestCache(t)
	data := testData(32 * 1024)
	src := &trackedReaderAt{data: data}
	r, err := c.Open(context.Background(), "cross-chunk", int64(len(data)), src)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	got := readRange(t, r, 3000, 5000)
	requireBytes(t, got, data, 3000)
	if len(src.callSnapshot()) < 2 {
		t.Fatalf("cross-chunk read should fetch multiple source chunks, got calls: %+v", src.callSnapshot())
	}
}

func TestCacheOnlyOpenAfterFill(t *testing.T) {
	c := newTestCache(t)
	data := []byte("cached data")
	src := &trackedReaderAt{data: data}

	r, err := c.Open(context.Background(), "cache-only", int64(len(data)), src)
	if err != nil {
		t.Fatalf("Open fill: %v", err)
	}
	if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("fill read = %q %v, want %q nil", string(got), err, string(data))
	}
	_ = r.Close()
	if !c.Exists("cache-only") {
		t.Fatal("Exists returned false after fill")
	}

	callsAfterFill := src.callCount()
	r, err = c.Open(context.Background(), "cache-only", -1, nil)
	if err != nil {
		t.Fatalf("Open cache-only: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("cache-only read = %q %v, want %q nil", string(got), err, string(data))
	}
	if src.callCount() != callsAfterFill {
		t.Fatal("cache-only read called upstream source")
	}
}

func TestPartialCacheAfterRestartAllowsCachedRangeOnly(t *testing.T) {
	dir := t.TempDir()
	data := repeatedStringData()
	src := &trackedReaderAt{data: data}

	c := newTestCacheAt(t, dir)
	r, err := c.Open(context.Background(), "partial-restart", int64(len(data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open fill: %v", err)
	}
	got := readRange(t, r, 0, 2048)
	requireBytes(t, got, data, 0)
	_ = r.Close()
	_ = c.Close()

	c = newTestCacheAt(t, dir)
	r, err = c.Open(context.Background(), "partial-restart", -1, nil)
	if err != nil {
		t.Fatalf("cache-only reopen: %v", err)
	}
	got = readRange(t, r, 0, 2048)
	requireBytes(t, got, data, 0)
	buf := make([]byte, 1024)
	if _, err := r.ReadAt(buf, 64*1024); err == nil {
		t.Fatal("cache-only read of uncached range succeeded")
	}
	_ = r.Close()
}

func TestFingerprintInvalidatesCachedData(t *testing.T) {
	c := newTestCache(t)
	oldData := []byte("old-data")
	newData := []byte("new-data")

	r, err := c.Open(context.Background(), "fingerprint", int64(len(oldData)), bytes.NewReader(oldData), WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open old: %v", err)
	}
	if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("old read = %q %v", string(got), err)
	}
	_ = r.Close()

	r, err = c.Open(context.Background(), "fingerprint", int64(len(newData)), bytes.NewReader(newData), WithFingerprint("v2"))
	if err != nil {
		t.Fatalf("Open new: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, newData) {
		t.Fatalf("new read = %q %v, want %q nil", string(got), err, string(newData))
	}
}

func TestFingerprintInvalidatesAfterCacheRestart(t *testing.T) {
	dir := t.TempDir()
	oldData := []byte("old-data")
	newData := []byte("new-data")

	c := newTestCacheAt(t, dir)
	r, err := c.Open(context.Background(), "restart-fingerprint", int64(len(oldData)), bytes.NewReader(oldData), WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open old: %v", err)
	}
	if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("old read = %q %v", string(got), err)
	}
	_ = r.Close()
	_ = c.Close()

	c = newTestCacheAt(t, dir)
	r, err = c.Open(context.Background(), "restart-fingerprint", int64(len(newData)), bytes.NewReader(newData), WithFingerprint("v2"))
	if err != nil {
		t.Fatalf("Open new after restart: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, newData) {
		t.Fatalf("new read after restart = %q %v, want %q nil", string(got), err, string(newData))
	}
}

func TestSameFingerprintReusesCachedData(t *testing.T) {
	c := newTestCache(t)
	oldData := []byte("cached-value")
	newDataSameFingerprint := []byte("changed-data")

	r, err := c.Open(context.Background(), "same-fingerprint", int64(len(oldData)), bytes.NewReader(oldData), WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open old: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("read old: %v", err)
	}
	_ = r.Close()

	r, err = c.Open(context.Background(), "same-fingerprint", int64(len(newDataSameFingerprint)), bytes.NewReader(newDataSameFingerprint), WithFingerprint("v1"))
	if err != nil {
		t.Fatalf("Open same fingerprint: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("same fingerprint read = %q %v, want cached %q nil", string(got), err, string(oldData))
	}
}

func TestNoFingerprintSameSizeReusesCachedData(t *testing.T) {
	c := newTestCache(t)
	oldData := []byte("version-one")
	newData := []byte("version-two")

	r, err := c.Open(context.Background(), "no-fingerprint", int64(len(oldData)), bytes.NewReader(oldData))
	if err != nil {
		t.Fatalf("Open old: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("read old: %v", err)
	}
	_ = r.Close()

	r, err = c.Open(context.Background(), "no-fingerprint", int64(len(newData)), bytes.NewReader(newData))
	if err != nil {
		t.Fatalf("Open same size no fingerprint: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("same size without fingerprint = %q %v, want cached %q nil", string(got), err, string(oldData))
	}
}

func TestNoFingerprintSizeChangeInvalidatesCachedData(t *testing.T) {
	c := newTestCache(t)
	oldData := []byte("short")
	newData := []byte("longer-data")

	r, err := c.Open(context.Background(), "size-change", int64(len(oldData)), bytes.NewReader(oldData))
	if err != nil {
		t.Fatalf("Open old: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("read old: %v", err)
	}
	_ = r.Close()

	r, err = c.Open(context.Background(), "size-change", int64(len(newData)), bytes.NewReader(newData))
	if err != nil {
		t.Fatalf("Open size change: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, newData) {
		t.Fatalf("size change read = %q %v, want %q nil", string(got), err, string(newData))
	}
}

func TestModTimePreserved(t *testing.T) {
	c := newTestCache(t)
	modTime := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	r, err := c.Open(context.Background(), "modtime", 3, bytes.NewReader([]byte("abc")), WithModTime(modTime))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if got := r.ModTime(); !got.Equal(modTime) {
		t.Fatalf("ModTime = %v, want %v", got, modTime)
	}
}

func TestConcurrentReadAtSameReader(t *testing.T) {
	c := newTestCache(t)
	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	r, err := c.Open(context.Background(), "concurrent", int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for j := 0; j < 50; j++ {
				off := rng.Intn(len(data) - 1024)
				buf := make([]byte, 1024)
				if _, err := r.ReadAt(buf, int64(off)); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(buf, data[off:off+1024]) {
					errs <- io.ErrUnexpectedEOF
					return
				}
			}
		}(int64(i + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ReadAt: %v", err)
		}
	}
}

func TestConcurrentIndependentReadersShareCachedChunks(t *testing.T) {
	c := newTestCache(t)
	data := testData(128 * 1024)
	src := &trackedReaderAt{data: data}

	const readers = 16
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Open(context.Background(), "shared-readers", int64(len(data)), src, WithFingerprint("v1"))
			if err != nil {
				errs <- err
				return
			}
			defer r.Close()
			got := readRange(t, r, 32*1024, 4096)
			if !bytes.Equal(got, data[32*1024:33*1024+3*1024]) {
				errs <- io.ErrUnexpectedEOF
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent independent reader: %v", err)
		}
	}
	if gotCalls := src.callCount(); gotCalls == 0 || gotCalls >= readers {
		t.Fatalf("shared readers fetched source %d times, want shared downloader rather than one fetch per reader", gotCalls)
	}
}

func TestRemoveAndShardKey(t *testing.T) {
	c := newTestCache(t)
	key := "remove-me"
	r, err := c.Open(context.Background(), key, 4, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	_ = r.Close()

	if !c.Exists(key) {
		t.Fatal("Exists returned false before remove")
	}
	if err := c.Remove(key); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if c.Exists(key) {
		t.Fatal("Exists returned true after remove")
	}

	if ShardKey("abc", 0) == ShardKey("abc", 2) {
		t.Fatal("sharded and unsharded keys unexpectedly match")
	}
}
