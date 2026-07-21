package varc

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowRcloneSource models a range-capable facade backed by fixed-size remote
// objects. VARC sees OpenRange, while the facade must fetch every complete
// remote object touched by that range before returning readable bytes.
type slowRcloneSource struct {
	size      int64
	chunkSize int64
	fetchTime time.Duration

	active    atomic.Int64
	maxActive atomic.Int64
	bytesRead atomic.Int64

	mu             sync.Mutex
	logicalRanges  []byteRange
	physicalChunks map[int64]int
}

func (s *slowRcloneSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path not used")
}

func (s *slowRcloneSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end < start || end >= s.size {
		return nil, ErrInvalidRange
	}

	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		peak := s.maxActive.Load()
		if active <= peak || s.maxActive.CompareAndSwap(peak, active) {
			break
		}
	}

	firstChunk := start / s.chunkSize
	lastChunk := end / s.chunkSize
	s.mu.Lock()
	s.logicalRanges = append(s.logicalRanges, byteRange{Start: start, End: end + 1})
	for chunk := firstChunk; chunk <= lastChunk; chunk++ {
		s.physicalChunks[chunk]++
		chunkStart := chunk * s.chunkSize
		chunkEnd := min64(s.size, chunkStart+s.chunkSize)
		s.bytesRead.Add(chunkEnd - chunkStart)
	}
	s.mu.Unlock()

	for chunk := firstChunk; chunk <= lastChunk; chunk++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.fetchTime):
		}
	}
	return io.NopCloser(&virtualRangeReader{pos: start, end: end + 1}), nil
}

func (s *slowRcloneSource) snapshot() ([]byteRange, map[int64]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ranges := append([]byteRange(nil), s.logicalRanges...)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })
	chunks := make(map[int64]int, len(s.physicalChunks))
	for chunk, count := range s.physicalChunks {
		chunks[chunk] = count
	}
	return ranges, chunks
}

type virtualRangeReader struct {
	pos int64
	end int64
}

func (r *virtualRangeReader) Read(p []byte) (int, error) {
	if r.pos >= r.end {
		return 0, io.EOF
	}
	if int64(len(p)) > r.end-r.pos {
		p = p[:r.end-r.pos]
	}
	for i := range p {
		p[i] = byte((r.pos + int64(i)) % 251)
	}
	r.pos += int64(len(p))
	return len(p), nil
}

func TestSlowRemoteRcloneChunkFacade(t *testing.T) {
	const (
		chunkSize = 128 * mebi
		fileSize  = 3 * chunkSize
	)

	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = -1
	opt.ReadRetryCount = -1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := newSlowRcloneSource(fileSize, chunkSize)
	r, err := c.Open(context.Background(), "slow-rclone", fileSize, src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	start := time.Now()
	buf := make([]byte, 64*kibi)
	n, err := r.Read(buf)
	if err != nil || n <= 0 {
		t.Fatalf("initial read n=%d err=%v", n, err)
	}
	elapsed := time.Since(start)
	if elapsed < src.fetchTime {
		t.Fatalf("initial read returned before full remote chunk fetch: %v", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("initial read blocked too long: %v", elapsed)
	}
	assertVirtualPattern(t, buf[:n], 0)

	seekOffset := 2*chunkSize + 17*kibi
	seekStart := time.Now()
	pos, err := r.Seek(seekOffset, io.SeekStart)
	if err != nil || pos != seekOffset {
		t.Fatalf("seek pos=%d err=%v", pos, err)
	}
	if elapsed := time.Since(seekStart); elapsed > 50*time.Millisecond {
		t.Fatalf("seek blocked for %v", elapsed)
	}

	seekBuf := make([]byte, 64*kibi)
	n, err = r.Read(seekBuf)
	if err != nil || n <= 0 {
		t.Fatalf("post-seek read n=%d err=%v", n, err)
	}
	assertVirtualPattern(t, seekBuf[:n], seekOffset)

	waitForCachedBytesDeadline(t, c, "slow-rclone", 2*chunkSize-17*kibi, 10*time.Second)
	logical, physical := src.snapshot()
	if physical[0] != 1 || physical[2] != 1 || physical[1] != 0 {
		t.Fatalf("physical chunk fetches=%v logical=%v", physical, logical)
	}
	if got := src.bytesRead.Load(); got != 2*chunkSize {
		t.Fatalf("physical bytes=%d, want %d", got, 2*chunkSize)
	}
	if got := src.maxActive.Load(); got != 1 {
		t.Fatalf("peak origin streams=%d, want 1", got)
	}
}

func TestSlowRemoteRcloneChunkFacadeThreeStreams(t *testing.T) {
	const (
		chunkSize = 128 * mebi
		fileSize  = 3 * chunkSize
	)
	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = 2
	opt.ReadRetryCount = -1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := newSlowRcloneSource(fileSize, chunkSize)
	r, err := c.Open(context.Background(), "slow-rclone-parallel", fileSize, src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.ReadAtContext(ctx, make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	waitForCoverageDeadline(t, c, "slow-rclone-parallel", fileSize, 10*time.Second)
	assertRcloneFetches(t, src, fileSize, 1)
}

func newSlowRcloneSource(size, chunkSize int64) *slowRcloneSource {
	return &slowRcloneSource{
		size:           size,
		chunkSize:      chunkSize,
		fetchTime:      75 * time.Millisecond,
		physicalChunks: make(map[int64]int),
	}
}

func assertRcloneFetches(t *testing.T, src *slowRcloneSource, fileSize, wantPeak int64) {
	t.Helper()
	logical, physical := src.snapshot()
	if len(logical) != 3 {
		t.Fatalf("logical origin calls=%+v, want 3", logical)
	}
	for chunk := int64(0); chunk < 3; chunk++ {
		if physical[chunk] != 1 {
			t.Fatalf("physical chunk %d fetched %d times, want once; calls=%+v", chunk, physical[chunk], logical)
		}
	}
	if got := src.bytesRead.Load(); got != fileSize {
		t.Fatalf("physical bytes=%d, want %d", got, fileSize)
	}
	if got := src.maxActive.Load(); got != wantPeak {
		t.Fatalf("peak origin streams=%d, want %d", got, wantPeak)
	}
}

func assertVirtualPattern(t *testing.T, p []byte, off int64) {
	t.Helper()
	for i, got := range p {
		want := byte((off + int64(i)) % 251)
		if got != want {
			t.Fatalf("byte %d=%d, want %d", i, got, want)
		}
	}
}

func waitForCachedBytesDeadline(t *testing.T, c *Cache, key string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cached, _, _, err := c.Coverage(key)
		if err == nil && cached == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cached, size, complete, err := c.Coverage(key)
	t.Fatalf("cached bytes for %q did not reach %d: cached=%d size=%d complete=%v err=%v", key, want, cached, size, complete, err)
}

func waitForCoverageDeadline(t *testing.T, c *Cache, key string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cached, _, complete, err := c.Coverage(key)
		if err == nil && cached == want && complete {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("coverage for %q did not reach %d", key, want)
}
