package varc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type controlledRangeSource struct {
	data       []byte
	firstReady chan struct{}
	release    chan struct{}
	failFirst  bool
	opens      atomic.Int64
	mu         sync.Mutex
	ranges     []byteRange
}

type truncatingRangeSource struct {
	data     []byte
	truncate int64
	opens    atomic.Int64
	mu       sync.Mutex
	ranges   []byteRange
}

func (s *truncatingRangeSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path not used")
}

func (s *truncatingRangeSource) OpenRange(_ context.Context, start, end int64) (io.ReadCloser, error) {
	attempt := s.opens.Add(1)
	s.mu.Lock()
	s.ranges = append(s.ranges, byteRange{Start: start, End: end + 1})
	s.mu.Unlock()
	if attempt == 1 && s.truncate > 0 && start+s.truncate < end+1 {
		end = start + s.truncate - 1
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

type parallelRangeSource struct {
	data      []byte
	want      int64
	opens     atomic.Int64
	ready     chan struct{}
	closeOnce sync.Once
}

func (s *parallelRangeSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path not used")
}

func (s *parallelRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if s.opens.Add(1) == s.want {
		s.closeOnce.Do(func() { close(s.ready) })
	}
	select {
	case <-s.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

func (s *controlledRangeSource) ReadAt(p []byte, off int64) (int, error) {
	return 0, errors.New("streaming path not used")
}

func (s *controlledRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	attempt := s.opens.Add(1)
	s.mu.Lock()
	s.ranges = append(s.ranges, byteRange{Start: start, End: end + 1})
	s.mu.Unlock()
	pr, pw := io.Pipe()
	go func() {
		firstEnd := min64(end+1, start+maxWriteBufferSize)
		_, err := pw.Write(s.data[start:firstEnd])
		if attempt == 1 && s.firstReady != nil {
			close(s.firstReady)
		}
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if attempt == 1 && s.release != nil {
			select {
			case <-s.release:
			case <-ctx.Done():
				_ = pw.CloseWithError(ctx.Err())
				return
			}
		}
		if attempt == 1 && s.failFirst {
			_ = pw.CloseWithError(io.ErrUnexpectedEOF)
			return
		}
		_, err = pw.Write(s.data[firstEnd : end+1])
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

func streamTestCache(t *testing.T, retries int) *Cache {
	t.Helper()
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 2 * maxWriteBufferSize
	opt.MaxInflightBytes = 4 * maxWriteBufferSize
	opt.ReadAhead = -1
	opt.ReadRetryCount = retries
	if retries == 0 {
		opt.ReadRetryCount = -1
	}
	opt.ReadRetryDelay = time.Millisecond
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestStreamedChunkIsVisibleBeforeDurableCoverage(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{
		data:       bytes.Repeat([]byte{0x5a}, 2*int(maxWriteBufferSize)),
		firstReady: make(chan struct{}),
		release:    make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "progressive", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := r.ReadAt(buf, 0)
		if err == nil && buf[0] != 0x5a {
			err = errors.New("wrong byte")
		}
		done <- err
	}()
	<-src.firstReady
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not resume after first internal commit")
	}
	cached, _, complete, err := c.Coverage("progressive")
	if err != nil || cached != 0 || complete {
		t.Fatalf("incomplete chunk persisted: cached=%d complete=%v err=%v", cached, complete, err)
	}
	close(src.release)
	waitForCoverage(t, c, "progressive", int64(len(src.data)))
}

func TestStreamTaskKeepsOneCacheFileOpen(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{
		data:       bytes.Repeat([]byte{0x5b}, 2*int(maxWriteBufferSize)),
		firstReady: make(chan struct{}),
		release:    make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "task-file", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	done := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 0)
		done <- err
	}()
	<-src.firstReady

	r.state.mu.Lock()
	var task *downloadTask
	for _, candidate := range r.state.tasks {
		task = candidate
		break
	}
	open := task != nil && task.file != nil
	r.state.mu.Unlock()
	if !open {
		t.Fatal("active stream did not retain its cache file descriptor")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(src.release)
	waitForCoverage(t, c, "task-file", int64(len(src.data)))

	r.state.mu.Lock()
	closed := task.file == nil
	r.state.mu.Unlock()
	if !closed {
		t.Fatal("completed stream retained its cache file descriptor")
	}
}

func TestInterruptedStreamDoesNotPersistPartialChunk(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{
		data:      bytes.Repeat([]byte{0x3c}, 2*int(maxWriteBufferSize)),
		release:   make(chan struct{}),
		failFirst: true,
	}
	r, err := c.Open(context.Background(), "interrupted", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 0)
		done <- err
	}()
	close(src.release)
	<-done
	waitForReaderTasksDone(t, r)
	cached, _, complete, err := c.Coverage("interrupted")
	if err != nil || cached != maxWriteBufferSize || complete {
		t.Fatalf("partial progress not retained: cached=%d complete=%v err=%v", cached, complete, err)
	}
}

func TestStreamRetryResumesAtLastCompletedCommit(t *testing.T) {
	c := streamTestCache(t, 1)
	src := &controlledRangeSource{
		data:      bytes.Repeat([]byte{0x7d}, 2*int(maxWriteBufferSize)),
		release:   make(chan struct{}),
		failFirst: true,
	}
	r, err := c.Open(context.Background(), "retry", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	close(src.release)
	if _, err := r.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	waitForCoverage(t, c, "retry", int64(len(src.data)))
	if src.opens.Load() != 2 {
		t.Fatalf("opens=%d, want 2", src.opens.Load())
	}
	src.mu.Lock()
	ranges := append([]byteRange(nil), src.ranges...)
	src.mu.Unlock()
	if len(ranges) != 2 || ranges[0] != (byteRange{Start: 0, End: 2 * maxWriteBufferSize}) || ranges[1] != (byteRange{Start: maxWriteBufferSize, End: 2 * maxWriteBufferSize}) {
		t.Fatalf("retry ranges=%+v", ranges)
	}
}

func TestLargeReadNeverExpandsOriginRequestBeyondChunkSize(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{data: bytes.Repeat([]byte{0x42}, 4*int(maxWriteBufferSize))}
	r, err := c.Open(context.Background(), "bounded", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, len(src.data))
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	ranges := append([]byteRange(nil), src.ranges...)
	src.mu.Unlock()
	if len(ranges) != 2 || ranges[0] != (byteRange{Start: 0, End: 2 * maxWriteBufferSize}) || ranges[1] != (byteRange{Start: 2 * maxWriteBufferSize, End: 4 * maxWriteBufferSize}) {
		t.Fatalf("origin ranges=%+v", ranges)
	}
}

func TestAdaptiveWritesGrowFrom4KiBTo1MiB(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 2 * maxWriteBufferSize
	opt.VerifyChecksum = true
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &controlledRangeSource{data: bytes.Repeat([]byte{0x6b}, 2*int(maxWriteBufferSize))}
	r, err := c.Open(context.Background(), "adaptive", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	meta, ok, err := loadMeta(c.MetaPath("adaptive"))
	if err != nil || !ok {
		t.Fatalf("load metadata ok=%v err=%v", ok, err)
	}
	want := []int64{4, 8, 16, 32, 64, 128, 256, 512, 1024, 4}
	if len(meta.Checksums) != len(want) {
		t.Fatalf("checksum segments=%d, want %d: %+v", len(meta.Checksums), len(want), meta.Checksums)
	}
	for i, checksum := range meta.Checksums {
		if got := (checksum.End - checksum.Start) / kibi; got != want[i] {
			t.Fatalf("segment %d size=%d KiB, want %d KiB", i, got, want[i])
		}
	}
}

func TestTruncatedStreamPersistsEveryByteAndResumesExactly(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64 * kibi
	opt.ReadRetryCount = 1
	opt.ReadRetryDelay = time.Millisecond
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &truncatingRangeSource{data: bytes.Repeat([]byte{0x37}, 64*int(kibi)), truncate: 7000}
	r, err := c.Open(context.Background(), "truncated", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	ranges := append([]byteRange(nil), src.ranges...)
	src.mu.Unlock()
	want := []byteRange{{Start: 0, End: 64 * kibi}, {Start: 7000, End: 64 * kibi}}
	if len(ranges) != len(want) || ranges[0] != want[0] || ranges[1] != want[1] {
		t.Fatalf("origin ranges=%+v, want %+v", ranges, want)
	}
	data, err := os.ReadFile(c.KeyPath("truncated"))
	if err != nil || !bytes.Equal(data, src.data) {
		t.Fatalf("cached bytes mismatch len=%d err=%v", len(data), err)
	}
}

func TestSequentialChunksDoubleUpToLimit(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64 * kibi
	opt.ChunkSizeLimit = 256 * kibi
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &controlledRangeSource{data: bytes.Repeat([]byte{0x18}, 448*int(kibi))}
	r, err := c.Open(context.Background(), "growth", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	ranges := append([]byteRange(nil), src.ranges...)
	src.mu.Unlock()
	want := []byteRange{{Start: 0, End: 64 * kibi}, {Start: 64 * kibi, End: 192 * kibi}, {Start: 192 * kibi, End: 448 * kibi}}
	if len(ranges) != len(want) {
		t.Fatalf("origin ranges=%+v, want %+v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("origin ranges=%+v, want %+v", ranges, want)
		}
	}
}

func TestParallelChunkStreamsOpenConcurrently(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64 * kibi
	opt.ChunkStreams = 3
	opt.MaxInflightBytes = 3 * opt.ChunkSize
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &parallelRangeSource{
		data:  bytes.Repeat([]byte{0x29}, 3*64*int(kibi)),
		want:  3,
		ready: make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "parallel", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.WarmAll(ctx); err != nil {
		t.Fatal(err)
	}
	if src.opens.Load() != 3 {
		t.Fatalf("parallel opens=%d, want 3", src.opens.Load())
	}
}

func TestRemoveDuringStreamCannotRecreateEvictedFiles(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{
		data:       bytes.Repeat([]byte{0x51}, 2*int(maxWriteBufferSize)),
		firstReady: make(chan struct{}),
		release:    make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "remove-active", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 0)
		done <- err
	}()
	<-src.firstReady
	if err := c.Remove("remove-active"); err != nil {
		t.Fatal(err)
	}
	close(src.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not stop after removal")
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := os.Stat(c.KeyPath("remove-active")); !os.IsNotExist(err) {
		t.Fatalf("evicted data file was recreated: %v", err)
	}
	if _, err := os.Stat(c.MetaPath("remove-active")); !os.IsNotExist(err) {
		t.Fatalf("evicted metadata file was recreated: %v", err)
	}
}

func TestConcurrentReadersShareOneChunkStream(t *testing.T) {
	c := streamTestCache(t, 0)
	src := &controlledRangeSource{
		data:       bytes.Repeat([]byte{0x21}, 2*int(maxWriteBufferSize)),
		firstReady: make(chan struct{}),
		release:    make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "coalesced", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1)
			if _, err := r.ReadAt(buf, 0); err != nil || buf[0] != 0x21 {
				t.Errorf("read byte=%x err=%v", buf[0], err)
			}
		}()
	}
	<-src.firstReady
	wg.Wait()
	if src.opens.Load() != 1 {
		t.Fatalf("opens=%d, want 1", src.opens.Load())
	}
	close(src.release)
}

func waitForCoverage(t *testing.T, c *Cache, key string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cached, _, complete, err := c.Coverage(key)
		if err == nil && cached == want && complete {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("coverage for %q did not reach %d", key, want)
}

func waitForReaderTasksDone(t *testing.T, r *Reader) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.state.mu.Lock()
		allDone := len(r.state.tasks) > 0
		for _, task := range r.state.tasks {
			allDone = allDone && task.done
		}
		r.state.mu.Unlock()
		if allDone {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("download task did not finish")
}
