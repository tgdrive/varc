package varc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	corevarc "github.com/tgdrive/varc/varc"
)

type progressiveHTTPSource struct {
	data    []byte
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *progressiveHTTPSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path only")
}

func (s *progressiveHTTPSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	go func() {
		firstEnd := start + 4096
		if firstEnd > end+1 {
			firstEnd = end + 1
		}
		_, err := pw.Write(s.data[start:firstEnd])
		s.once.Do(func() { close(s.started) })
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			_ = pw.CloseWithError(ctx.Err())
			return
		}
		_, err = pw.Write(s.data[firstEnd : end+1])
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

type firstWriteRecorder struct {
	header     http.Header
	status     int
	firstWrite chan struct{}
	once       sync.Once
	mu         sync.Mutex
	body       bytes.Buffer
}

func (w *firstWriteRecorder) Header() http.Header { return w.header }

func (w *firstWriteRecorder) WriteHeader(status int) { w.status = status }

func (w *firstWriteRecorder) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.firstWrite) })
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(p)
}

func TestServeReaderStreamsFirstBytesBeforeChunkCompletes(t *testing.T) {
	data := bytes.Repeat([]byte{0x4d}, 128*1024)
	opt := corevarc.DefaultOptions()
	opt.CacheDir = t.TempDir()
	opt.ChunkSize = int64(len(data))
	opt.ChunkSizeLimit = opt.ChunkSize
	opt.PreloadChunks = 0
	opt.NoBackground = true
	cache, err := corevarc.New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	src := &progressiveHTTPSource{
		data:    data,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reader, err := cache.Open(context.Background(), "progressive-http", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.test/video", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &firstWriteRecorder{header: make(http.Header), firstWrite: make(chan struct{})}
	h := &Handler{}
	done := make(chan error, 1)
	go func() {
		done <- h.serveReader(w, req, reader, byteSpan{Start: 0, End: int64(len(data)), Size: int64(len(data)), Partial: true}, RemoteObject{Size: int64(len(data))}, "MISS", "source", "key")
	}()

	select {
	case <-src.started:
	case <-time.After(time.Second):
		t.Fatal("source did not start")
	}
	select {
	case <-w.firstWrite:
		// The response started while the source was still blocked after 4 KiB.
	case <-time.After(time.Second):
		t.Fatal("handler waited for the full chunk before writing response bytes")
	}

	close(src.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not finish after source resumed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !bytes.Equal(w.body.Bytes(), data) {
		t.Fatalf("response bytes=%d, want %d", w.body.Len(), len(data))
	}
}

func TestStreamingBodyIsNotKilledByConfiguredTimeout(t *testing.T) {
	data := []byte("abcdefgh")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-7/8")
		w.Header().Set("Content-Length", "8")
		w.WriteHeader(http.StatusPartialContent)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(75 * time.Millisecond)
		_, _ = w.Write(data)
	}))
	defer origin.Close()

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	h := &Handler{
		CacheDir:        t.TempDir(),
		Timeout:         caddy.Duration(20 * time.Millisecond),
		ProbeTimeout:    caddy.Duration(time.Second),
		ResponseTimeout: caddy.Duration(time.Second),
	}
	if err := h.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Cleanup()
	if h.client.Timeout != 0 {
		t.Fatalf("streaming client timeout=%v, want no whole-body timeout", h.client.Timeout)
	}

	src := &HTTPRangeSource{Client: h.client, URL: origin.URL, ValidateSize: int64(len(data))}
	buf := make([]byte, len(data))
	if _, err := src.ReadAt(buf, 0); err != nil {
		t.Fatalf("stream failed after configured timeout: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("body=%q, want %q", buf, data)
	}
}

func TestClientDisconnectErrorsAreIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "reset", ctx: context.Background(), err: syscall.ECONNRESET, want: true},
		{name: "broken pipe", ctx: context.Background(), err: syscall.EPIPE, want: true},
		{name: "canceled request", ctx: ctx, err: errors.New("write failed"), want: true},
		{name: "real error", ctx: context.Background(), err: errors.New("disk read failed"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isClientDisconnect(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("isClientDisconnect()=%v, want %v", got, tc.want)
			}
		})
	}
}
