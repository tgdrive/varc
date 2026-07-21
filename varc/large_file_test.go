package varc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
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
