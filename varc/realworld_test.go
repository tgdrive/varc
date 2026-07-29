package varc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type generatedRangeSource struct {
	size  int64
	opens atomic.Int64
	delay time.Duration
}

func (s *generatedRangeSource) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= s.size {
		return 0, io.EOF
	}
	n := len(p)
	if remain := s.size - off; int64(n) > remain {
		n = int(remain)
	}
	for i := 0; i < n; i++ {
		p[i] = byte((off + int64(i)*31 + 17) % 251)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s *generatedRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end < start || end >= s.size {
		return nil, fmt.Errorf("invalid generated range %d-%d", start, end)
	}
	s.opens.Add(1)
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return io.NopCloser(io.NewSectionReader(s, start, end-start+1)), nil
}

func TestStalledSourceCancellationDrainsScheduler(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 128
	opt.ChunkSizeLimit = 128
	opt.PreloadChunks = 2
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	src := &blockingRangeSource{data: make([]byte, 512), started: make(chan struct{})}
	r, err := c.Open(context.Background(), "stalled", 512, src)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 0)
		done <- err
	}()
	<-src.started
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked read error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled read did not cancel")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	if len(c.activeTasks) != 0 || len(c.waitingTasks) != 0 {
		t.Fatalf("scheduler leaked active=%d queued=%d", len(c.activeTasks), len(c.waitingTasks))
	}
}

func TestIndependentEntriesDownloadConcurrently(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 2 * maxWriteBufferSize
	opt.ChunkSizeLimit = 2 * maxWriteBufferSize
	opt.PreloadChunks = -1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const concurrency = 8
	started := make(chan struct{}, concurrency)
	release := make(chan struct{})
	newSource := func(value byte) *gatedRangeSource {
		return &gatedRangeSource{data: bytes.Repeat([]byte{value}, 2*int(maxWriteBufferSize)), started: started, release: release}
	}

	readers := make([]*Reader, 0, concurrency)
	for i := range concurrency {
		r, err := c.Open(context.Background(), fmt.Sprintf("concurrent-entry-%d", i), 2*maxWriteBufferSize, newSource(byte(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
		defer r.Close()
	}

	errs := make(chan error, concurrency)
	for _, r := range readers {
		go func() {
			_, readErr := r.ReadAt(make([]byte, 1), 0)
			errs <- readErr
		}()
	}
	for range concurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("independent cache entries did not all start concurrently")
		}
	}
	close(release)
	for range concurrency {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSameEntryBlockingRangesDownloadConcurrently(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = maxWriteBufferSize
	opt.ChunkSizeLimit = maxWriteBufferSize
	opt.PreloadChunks = -1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	src := &gatedRangeSource{
		data:    bytes.Repeat([]byte{0x7a}, 2*int(maxWriteBufferSize)),
		started: started,
		release: release,
	}

	r1, err := c.Open(context.Background(), "shared-entry", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c.Open(context.Background(), "shared-entry", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	errs := make(chan error, 2)
	go func() {
		_, readErr := r1.ReadAt(make([]byte, 1), 0)
		errs <- readErr
	}()
	go func() {
		_, readErr := r2.ReadAt(make([]byte, 1), maxWriteBufferSize)
		errs <- readErr
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("same-entry blocking ranges did not start concurrently")
		}
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

type gatedRangeSource struct {
	data    []byte
	started chan<- struct{}
	release <-chan struct{}
}

func (s *gatedRangeSource) ReadAt(p []byte, off int64) (int, error) {
	return bytes.NewReader(s.data).ReadAt(p, off)
}

func (s *gatedRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

func TestRestartUsesDurableSparseCacheWithoutSource(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.ChunkSize = 4096
	data := newCountingSource(32 * 1024)
	c1, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := c1.Open(context.Background(), "restart", int64(len(data.data)), data)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 2048)
	if _, err := r1.ReadAt(want, 9000); err != nil {
		t.Fatal(err)
	}
	_ = r1.Close()
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	r2, err := c2.Open(context.Background(), "restart", -1, nil, WithCacheOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got := make([]byte, len(want))
	if _, err := r2.ReadAt(got, 9000); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restart cache bytes changed")
	}
}

func TestCacheWriteFailureReturnsErrorAndDoesNotClaimCoverage(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	key := "write-failure"
	parent := filepath.Dir(c.KeyPath(key))
	if err := os.MkdirAll(filepath.Dir(parent), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := newCountingSource(256)
	r, openErr := c.Open(context.Background(), key, int64(len(src.data)), src)
	if openErr == nil {
		defer r.Close()
		if _, err := r.ReadAt(make([]byte, 1), 0); err == nil {
			t.Fatal("expected cache directory/write failure")
		}
	}
	cached, _, _, coverageErr := c.Coverage(key)
	if coverageErr == nil && cached != 0 {
		t.Fatalf("failed write claimed %d cached bytes", cached)
	}
}

func TestHugeSparseOffsetNearMultiTerabyteBoundary(t *testing.T) {
	const size int64 = 8 << 40 // 8 TiB
	const off int64 = 6<<40 + 12345
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 4096
	opt.ChunkSizeLimit = 4096
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &generatedRangeSource{size: size}
	r, err := c.Open(context.Background(), "huge-sparse", size, src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got := make([]byte, 257)
	if _, err := r.ReadAt(got, off); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, len(got))
	_, _ = src.ReadAt(want, off)
	if !bytes.Equal(got, want) {
		t.Fatal("huge-offset data mismatch")
	}
	if _, err := r.ReadAt(make([]byte, 1), math.MaxInt64); !errors.Is(err, io.EOF) {
		t.Fatalf("max offset error=%v, want EOF", err)
	}
}

func TestHundredsOfReadersCoalesceOneOriginFetch(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 4096
	opt.ChunkSizeLimit = 4096
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &generatedRangeSource{size: 4096, delay: 20 * time.Millisecond}
	const readers = 256
	opened := make([]*Reader, readers)
	for i := range opened {
		opened[i], err = c.Open(context.Background(), "many-readers", src.size, src)
		if err != nil {
			t.Fatal(err)
		}
		defer opened[i].Close()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for _, r := range opened {
		wg.Add(1)
		go func(r *Reader) {
			defer wg.Done()
			<-start
			buf := make([]byte, 128)
			_, err := r.ReadAt(buf, 512)
			errCh <- err
		}(r)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := src.opens.Load(); got != 1 {
		t.Fatalf("origin opens=%d, want one coalesced fetch", got)
	}
}

func TestTwoCacheInstancesSharingDirectoryRemainReadable(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.ChunkSize = 4096
	src := newCountingSource(16 * 1024)
	c1, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	r1, err := c1.Open(context.Background(), "shared-dir", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c2.Open(context.Background(), "shared-dir", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, r := range []*Reader{r1, r2} {
		wg.Add(1)
		go func(r *Reader) {
			defer wg.Done()
			buf := make([]byte, 2048)
			_, err := r.ReadAt(buf, 4096)
			if err == nil && !bytes.Equal(buf, src.data[4096:6144]) {
				err = errors.New("shared-directory bytes mismatch")
			}
			errs <- err
		}(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTwoCacheInstancesMergeDisjointRangeMetadata(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.ChunkSize = 4096
	opt.ChunkSizeLimit = 4096
	src := newCountingSource(16 * 1024)
	c1, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	r1, err := c1.Open(context.Background(), "shared-disjoint", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c2.Open(context.Background(), "shared-disjoint", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, read := range []struct {
		r   *Reader
		off int64
	}{{r1, 0}, {r2, 8192}} {
		_ = i
		wg.Add(1)
		go func(read struct {
			r   *Reader
			off int64
		}) {
			defer wg.Done()
			_, err := read.r.ReadAt(make([]byte, 1), read.off)
			errs <- err
		}(read)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	meta, ok, err := loadMeta(c1.MetaPath("shared-disjoint"))
	if err != nil || !ok {
		t.Fatalf("load metadata: ok=%v err=%v", ok, err)
	}
	if !containsRange(meta.Ranges, 0, 4096) || !containsRange(meta.Ranges, 8192, 12288) {
		t.Fatalf("disjoint ranges were not merged: %+v", meta.Ranges)
	}
}

type cancellationIgnoringSource struct {
	data    []byte
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *cancellationIgnoringSource) ReadAt(p []byte, off int64) (int, error) {
	return bytes.NewReader(s.data).ReadAt(p, off)
}

func (s *cancellationIgnoringSource) OpenRange(context.Context, int64, int64) (io.ReadCloser, error) {
	s.once.Do(func() { close(s.started) })
	return io.NopCloser(&releaseReader{data: s.data, release: s.release}), nil
}

type releaseReader struct {
	data    []byte
	release <-chan struct{}
	off     int
}

func (r *releaseReader) Read(p []byte) (int, error) {
	<-r.release
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func TestCanceledGenerationCannotWriteIntoReplacement(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64
	opt.ChunkSizeLimit = 64
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oldData := bytes.Repeat([]byte("o"), 64)
	old := &cancellationIgnoringSource{data: oldData, started: make(chan struct{}), release: make(chan struct{})}
	r1, err := c.Open(context.Background(), "generation", 64, old, WithFingerprint("old"))
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, err := r1.ReadAt(make([]byte, 1), 0)
		readDone <- err
	}()
	<-old.started

	newData := bytes.Repeat([]byte("n"), 64)
	r2, err := c.Open(context.Background(), "generation", 64, bytes.NewReader(newData), WithFingerprint("new"))
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	close(old.release)
	if err := <-readDone; err == nil {
		t.Fatal("obsolete generation read unexpectedly succeeded")
	}
	got := make([]byte, len(newData))
	if _, err := r2.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("replacement data was corrupted: %q", got)
	}
	_ = r1.Close()
}

func TestRepeatedCancellationLeavesNoQueuedTasks(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64
	opt.ChunkSizeLimit = 64
	opt.PreloadChunks = 2
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		src := &blockingRangeSource{data: make([]byte, 256), started: make(chan struct{})}
		r, err := c.Open(context.Background(), fmt.Sprintf("cancel-%d", i), 256, src)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := r.ReadAt(make([]byte, 1), 0)
			done <- err
		}()
		<-src.started
		_ = r.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d did not cancel", i)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.schedulerMu.Lock()
	defer c.schedulerMu.Unlock()
	if len(c.activeTasks) != 0 || len(c.waitingTasks) != 0 {
		t.Fatalf("scheduler retained active=%d queued=%d", len(c.activeTasks), len(c.waitingTasks))
	}
}
