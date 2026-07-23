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

func TestPreloadCanceledWhenLastReaderCloses(t *testing.T) {
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
		data:           bytes.Repeat([]byte{0x6b}, 3*int(chunkSize)),
		preloadStarted: make(chan struct{}),
		preloadStopped: make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "preload-canceled-on-close", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-src.preloadStarted:
	case <-time.After(time.Second):
		t.Fatal("preload did not start")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-src.preloadStopped:
	case <-time.After(time.Second):
		t.Fatal("closing the last reader did not cancel active preload")
	}
}

type cancelThenResumeSource struct {
	data          []byte
	mu            sync.Mutex
	calls         int
	firstStarted  chan struct{}
	firstCanceled chan struct{}
	startOnce     sync.Once
	cancelOnce    sync.Once
}

func (s *cancelThenResumeSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path only")
}

func (s *cancelThenResumeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call != 1 {
		return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
	}

	pr, pw := io.Pipe()
	go func() {
		prefixEnd := start + 4096
		if prefixEnd > end+1 {
			prefixEnd = end + 1
		}
		_, err := pw.Write(s.data[start:prefixEnd])
		s.startOnce.Do(func() { close(s.firstStarted) })
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		<-ctx.Done()
		s.cancelOnce.Do(func() { close(s.firstCanceled) })
		_ = pw.CloseWithError(ctx.Err())
	}()
	return pr, nil
}

func TestCanceledReaderDoesNotPoisonReplacementReader(t *testing.T) {
	data := bytes.Repeat([]byte{0x73}, 128*1024)
	src := &cancelThenResumeSource{
		data:          data,
		firstStarted:  make(chan struct{}),
		firstCanceled: make(chan struct{}),
	}
	opt := testOptions(t.TempDir())
	opt.ChunkSize = int64(len(data))
	opt.ChunkSizeLimit = opt.ChunkSize
	opt.PreloadChunks = 0
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r1, err := c.Open(context.Background(), "replacement-after-cancel", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(data))
	n, err := r1.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("first reader made no progressive progress")
	}
	select {
	case <-src.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first source request did not start")
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-src.firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("closing the first reader did not cancel its blocking task")
	}

	deadline := time.Now().Add(time.Second)
	for {
		r1.state.mu.Lock()
		allDone := true
		for _, task := range r1.state.tasks {
			if !task.done {
				allDone = false
				break
			}
		}
		r1.state.mu.Unlock()
		if allDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled task did not finish")
		}
		time.Sleep(time.Millisecond)
	}

	r2, err := c.Open(context.Background(), "replacement-after-cancel", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("replacement reader inherited canceled task error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("replacement reader got %d bytes, want %d", len(got), len(data))
	}
}
