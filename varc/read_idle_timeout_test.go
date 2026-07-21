package varc

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type idleRetrySource struct {
	data  []byte
	mu    sync.Mutex
	calls int
}

func (s *idleRetrySource) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }

func (s *idleRetrySource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(s.data[start : start+4096])
			<-ctx.Done()
			_ = pw.CloseWithError(ctx.Err())
		}()
		return pr, nil
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

func TestReadIdleTimeoutRetriesFromLastProgress(t *testing.T) {
	data := bytes.Repeat([]byte{0x5a}, 128*1024)
	src := &idleRetrySource{data: data}
	opt := DefaultOptions()
	opt.CacheDir = t.TempDir()
	opt.ChunkSize = int64(len(data))
	opt.ChunkSizeLimit = opt.ChunkSize
	opt.PreloadChunks = 0
	opt.ReadIdleTimeout = 20 * time.Millisecond
	opt.ReadRetryCount = 2
	opt.ReadRetryDelay = time.Millisecond
	opt.NoBackground = true
	cache, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	r, err := cache.Open(context.Background(), "idle-retry", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want %d", len(got), len(data))
	}
	src.mu.Lock()
	calls := src.calls
	src.mu.Unlock()
	if calls != 2 {
		t.Fatalf("origin calls=%d, want 2", calls)
	}
}

type pacedRangeSource struct {
	data    []byte
	delay   time.Duration
	maxRead int
	mu      sync.Mutex
	calls   int
}

func (s *pacedRangeSource) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }

func (s *pacedRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return &pacedReadCloser{
		ctx:     ctx,
		data:    s.data[start : end+1],
		delay:   s.delay,
		maxRead: s.maxRead,
	}, nil
}

type pacedReadCloser struct {
	ctx     context.Context
	data    []byte
	off     int
	delay   time.Duration
	maxRead int
}

func (r *pacedReadCloser) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	}
	n := r.maxRead
	if n > len(p) {
		n = len(p)
	}
	if remaining := len(r.data) - r.off; n > remaining {
		n = remaining
	}
	copy(p[:n], r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}

func (*pacedReadCloser) Close() error { return nil }

func TestReadIdleTimeoutTracksEverySourceRead(t *testing.T) {
	data := bytes.Repeat([]byte{0x31}, 32*1024)
	src := &pacedRangeSource{
		data:    data,
		delay:   10 * time.Millisecond,
		maxRead: 512,
	}
	opt := DefaultOptions()
	opt.CacheDir = t.TempDir()
	opt.ChunkSize = int64(len(data))
	opt.ChunkSizeLimit = opt.ChunkSize
	opt.PreloadChunks = 0
	opt.ReadIdleTimeout = 120 * time.Millisecond
	opt.ReadRetryCount = 0
	opt.NoBackground = true
	cache, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	r, err := cache.Open(context.Background(), "paced-origin", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want %d", len(got), len(data))
	}
	src.mu.Lock()
	calls := src.calls
	src.mu.Unlock()
	if calls != 1 {
		t.Fatalf("origin calls=%d, want 1; continuously arriving bytes must not trigger the idle timeout", calls)
	}
}
