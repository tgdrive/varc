package varc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type preemptibleRangeSource struct {
	data           []byte
	mu             sync.Mutex
	calls          int
	preloadStarted chan struct{}
	preloadStopped chan struct{}
	startOnce      sync.Once
	stopOnce       sync.Once
}

func (s *preemptibleRangeSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path only")
}

func (s *preemptibleRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	if call == 2 {
		s.startOnce.Do(func() { close(s.preloadStarted) })
		<-ctx.Done()
		s.stopOnce.Do(func() { close(s.preloadStopped) })
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

func TestBlockingSeekPreemptsActivePreload(t *testing.T) {
	const chunkSize int64 = 64
	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = 1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := &preemptibleRangeSource{
		data:           bytes.Repeat([]byte{0x5a}, 4*int(chunkSize)),
		preloadStarted: make(chan struct{}),
		preloadStopped: make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "preempt-active-preload", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-src.preloadStarted:
	case <-time.After(time.Second):
		t.Fatal("preload did not start")
	}

	seekDone := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 2*chunkSize)
		seekDone <- err
	}()

	select {
	case <-src.preloadStopped:
	case <-time.After(time.Second):
		t.Fatal("blocking seek did not cancel active preload")
	}
	select {
	case err := <-seekDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("seek remained blocked behind canceled preload")
	}
}
