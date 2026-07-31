package varc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

type recordedGeneratedSource struct {
	size int64

	mu     sync.Mutex
	ranges []byteRange
}

func (s *recordedGeneratedSource) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= s.size {
		return 0, io.EOF
	}
	n := len(p)
	if remaining := s.size - off; int64(n) > remaining {
		n = int(remaining)
	}
	fillGeneratedBytes(p[:n], off)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s *recordedGeneratedSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end < start || end >= s.size {
		return nil, fmt.Errorf("invalid range %d-%d for size %d", start, end, s.size)
	}
	s.mu.Lock()
	s.ranges = append(s.ranges, byteRange{Start: start, End: end + 1})
	s.mu.Unlock()
	return io.NopCloser(io.NewSectionReader(s, start, end-start+1)), nil
}

func (s *recordedGeneratedSource) snapshotRanges() []byteRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byteRange(nil), s.ranges...)
}

func fillGeneratedBytes(p []byte, off int64) {
	for i := range p {
		absolute := off + int64(i)
		p[i] = byte((absolute*17 + 29) % 251)
	}
}

func verifyGeneratedBytes(t *testing.T, p []byte, off int64) {
	t.Helper()
	for i, got := range p {
		absolute := off + int64(i)
		want := byte((absolute*17 + 29) % 251)
		if got != want {
			t.Fatalf("byte mismatch at offset %d: got %d want %d", absolute, got, want)
		}
	}
}

func TestLargeFileSequentialReadHasNoDuplicateOrMissingFetches(t *testing.T) {
	const (
		fileSize  = int64(256 << 20)
		chunkSize = int64(32 << 20)
		readSize  = 256 << 10
	)

	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = 1
	cache, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	src := &recordedGeneratedSource{size: fileSize}
	reader, err := cache.Open(context.Background(), "large-sequential", fileSize, src)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, readSize)
	var off int64
	for off < fileSize {
		need := int64(len(buf))
		if fileSize-off < need {
			need = fileSize - off
		}
		n, readErr := reader.Read(buf[:need])
		if n > 0 {
			verifyGeneratedBytes(t, buf[:n], off)
			off += int64(n)
		}
		if readErr != nil && readErr != io.EOF {
			t.Fatalf("read stalled at %d: %v", off, readErr)
		}
		if readErr == io.EOF {
			break
		}
		if n == 0 {
			t.Fatalf("zero-byte read without EOF at %d", off)
		}
	}
	if off != fileSize {
		t.Fatalf("read %d bytes, want %d", off, fileSize)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	ranges := src.snapshotRanges()
	wantRequests := int(fileSize / chunkSize)
	if len(ranges) != wantRequests {
		t.Fatalf("origin requests=%d, want %d: %+v", len(ranges), wantRequests, ranges)
	}
	for i, span := range ranges {
		wantStart := int64(i) * chunkSize
		wantEnd := wantStart + chunkSize
		if span.Start != wantStart || span.End != wantEnd {
			t.Fatalf("origin request %d=%d-%d, want %d-%d", i, span.Start, span.End, wantStart, wantEnd)
		}
	}

	cached, total, complete, err := cache.Coverage("large-sequential")
	if err != nil {
		t.Fatal(err)
	}
	if cached != fileSize || total != fileSize || !complete {
		t.Fatalf("coverage cached=%d total=%d complete=%v", cached, total, complete)
	}

	before := cache.Metrics()
	cachedReader, err := cache.Open(context.Background(), "large-sequential", -1, nil, WithCacheOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer cachedReader.Close()
	for off = 0; off < fileSize; {
		need := int64(len(buf))
		if fileSize-off < need {
			need = fileSize - off
		}
		n, readErr := cachedReader.Read(buf[:need])
		if n > 0 {
			verifyGeneratedBytes(t, buf[:n], off)
			off += int64(n)
		}
		if readErr != nil && readErr != io.EOF {
			t.Fatal(readErr)
		}
		if readErr == io.EOF {
			break
		}
	}
	after := cache.Metrics()
	if after.SourceReads != before.SourceReads {
		t.Fatalf("cache-only replay made source reads: before=%d after=%d", before.SourceReads, after.SourceReads)
	}
	if after.Misses != before.Misses {
		t.Fatalf("fully cached replay added misses: before=%d after=%d", before.Misses, after.Misses)
	}
}

func TestLargeFileFrequentRandomSeeksRemainCorrect(t *testing.T) {
	const (
		fileSize  = int64(20 << 30)
		chunkSize = int64(1 << 20)
		readSize  = 64 << 10
	)

	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = 4 * chunkSize
	opt.PreloadChunks = 2
	cache, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	src := &recordedGeneratedSource{size: fileSize}
	reader, err := cache.Open(context.Background(), "large-random-seeks", fileSize, src)
	if err != nil {
		t.Fatal(err)
	}

	offsets := []int64{
		0,
		17*chunkSize + 123,
		fileSize - readSize,
		3*chunkSize + 77,
		12<<30 + 19,
		8*chunkSize + 4096,
		19<<30 + 333,
		2<<30 + 55,
		64*chunkSize + 7,
		15<<30 + 2048,
		5*chunkSize + 99,
		10<<30 + 8191,
	}
	buf := make([]byte, readSize)
	for i, off := range offsets {
		n, err := reader.ReadAt(buf, off)
		if err != nil {
			t.Fatalf("seek %d at %d failed: %v", i, off, err)
		}
		if n != len(buf) {
			t.Fatalf("seek %d at %d read %d bytes, want %d", i, off, n, len(buf))
		}
		verifyGeneratedBytes(t, buf, off)
	}

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := make(chan struct{})
	go func() {
		cache.wg.Wait()
		close(deadline)
	}()
	select {
	case <-deadline:
	case <-time.After(2 * time.Second):
		t.Fatal("downloads did not stop after final reader close")
	}

	reader.state.mu.Lock()
	for _, task := range reader.state.tasks {
		if !task.done {
			reader.state.mu.Unlock()
			t.Fatalf("task %d-%d remained active after final reader close", task.start, task.end)
		}
	}
	reader.state.mu.Unlock()

	if got := src.snapshotRanges(); len(got) == 0 {
		t.Fatal("random seeks made no origin requests")
	}
}

func TestRollingPreloadsAdvanceOnCachedReadButNotCompletion(t *testing.T) {
	const (
		chunkSize = int64(1 << 20)
		fileSize  = 6 * chunkSize
	)

	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = 2
	cache, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	src := &recordedGeneratedSource{size: fileSize}
	reader, err := cache.Open(context.Background(), "rolling-preloads", fileSize, src)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, err := reader.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	waitForCachedBytesDeadline(t, cache, "rolling-preloads", 3*chunkSize, 2*time.Second)
	initialRanges := src.snapshotRanges()
	if len(initialRanges) != 3 {
		t.Fatalf("initial origin ranges=%+v, want active chunk plus two preloads", initialRanges)
	}

	// Completing preloads must not pull the rest of a paused stream.
	time.Sleep(25 * time.Millisecond)
	if ranges := src.snapshotRanges(); len(ranges) != 3 {
		t.Fatalf("preload completion scheduled more work without consumption: %+v", ranges)
	}

	// Entering an already-cached window must add one new chunk at the tail.
	if _, err := reader.Seek(chunkSize, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	waitForCachedBytesDeadline(t, cache, "rolling-preloads", 4*chunkSize, 2*time.Second)
	ranges := src.snapshotRanges()
	if len(ranges) != 4 {
		t.Fatalf("rolling preloads made duplicate origin requests: %+v", ranges)
	}
	wantTail := byteRange{Start: 3 * chunkSize, End: 4 * chunkSize}
	foundTail := false
	for _, span := range ranges {
		if span == wantTail {
			foundTail = true
			break
		}
	}
	if !foundTail {
		t.Fatalf("cached read did not extend rolling preload tail to %+v: %+v", wantTail, ranges)
	}
}
