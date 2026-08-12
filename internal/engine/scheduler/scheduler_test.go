package scheduler

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/tgdrive/varc/internal/engine/objectio"
	"github.com/tgdrive/varc/ranges"
)

type downloaderTestItem struct {
	mu   sync.Mutex
	rs   ranges.Ranges
	size int64
}

func (i *downloaderTestItem) FindMissing(r ranges.Range) ranges.Range {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := i.rs.FindMissing(r)
	out.Clip(i.size)
	return out
}

func (i *downloaderTestItem) HasRange(r ranges.Range) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.rs.Present(r)
}

func (i *downloaderTestItem) WriteAtNoOverwrite(b []byte, off int64) (n int, skipped int, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	r := ranges.Range{Pos: off, Size: int64(len(b))}
	parts := i.rs.FindAll(r)
	for _, part := range parts {
		size := int(part.R.Size)
		n += size
		if part.Present {
			skipped += size
		} else {
			i.rs.Insert(part.R)
		}
	}
	return n, skipped, nil
}

type downloaderTestObject struct {
	data []byte
	mu   sync.Mutex
	open int
}

type cancelBlockingObject struct {
	started chan struct{}
	size    int64
}

func (o *cancelBlockingObject) Size() int64 { return o.size }

func (o *cancelBlockingObject) Open(ctx context.Context, _ *objectio.Span) (io.ReadCloser, error) {
	return &cancelBlockingReader{ctx: ctx, started: o.started}, nil
}

type cancelBlockingReader struct {
	ctx     context.Context
	started chan struct{}
}

func (r *cancelBlockingReader) Read([]byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (*cancelBlockingReader) Close() error { return nil }

func (o *downloaderTestObject) Size() int64 { return int64(len(o.data)) }

func (o *downloaderTestObject) Open(_ context.Context, span *objectio.Span) (io.ReadCloser, error) {
	start := int64(0)
	end := int64(len(o.data))
	if span != nil {
		start = span.Start
		if span.End >= 0 {
			end = span.End + 1
		}
	}
	if end > int64(len(o.data)) {
		end = int64(len(o.data))
	}
	o.mu.Lock()
	o.open++
	o.mu.Unlock()
	return io.NopCloser(bytes.NewReader(o.data[start:end])), nil
}

func TestDownloaderIsReusedForNearbySequentialRange(t *testing.T) {
	ctx := objectio.WithConfig(context.Background(), objectio.Config{BufferSize: 0, LowLevelRetries: 10})
	object := &downloaderTestObject{data: bytes.Repeat([]byte("x"), 128*1024)}
	item := &downloaderTestItem{size: object.Size()}
	opt := &Config{ChunkSize: 4096, ChunkSizeLimit: 4096}
	dls := New(ctx, item, opt, "object", object)
	defer dls.Close(nil)

	if err := dls.Download(ranges.Range{Pos: 0, Size: 2}); err != nil {
		t.Fatal(err)
	}
	dls.mu.Lock()
	if len(dls.dls) != 1 {
		dls.mu.Unlock()
		t.Fatalf("downloaders after first read = %d, want 1", len(dls.dls))
	}
	first := dls.dls[0]
	dls.mu.Unlock()

	if err := dls.Download(ranges.Range{Pos: 32 * 1024, Size: 2}); err != nil {
		t.Fatal(err)
	}
	dls.mu.Lock()
	defer dls.mu.Unlock()
	if len(dls.dls) != 1 || dls.dls[0] != first {
		t.Fatalf("nearby read did not reuse downloader: before=%p after=%v", first, dls.dls)
	}
}

func TestDownloadAndEnsureDownloader(t *testing.T) {
	ctx := objectio.WithConfig(context.Background(), objectio.Config{BufferSize: 0, LowLevelRetries: 10})
	object := &downloaderTestObject{data: bytes.Repeat([]byte("x"), 3*1024*1024+123)}
	item := &downloaderTestItem{size: object.Size()}
	opt := &Config{ChunkSize: 64 * 1024, ChunkSizeLimit: 256 * 1024}
	dls := New(ctx, item, opt, "object", object)
	defer dls.Close(nil)

	for _, r := range []ranges.Range{
		{Pos: 100, Size: 250},
		{Pos: 500 * 1024, Size: 250},
		{Pos: 2 * 1024 * 1024, Size: 250},
	} {
		if err := dls.Download(r); err != nil {
			t.Fatalf("Download(%+v): %v", r, err)
		}
		if !item.HasRange(r) {
			t.Fatalf("Download(%+v) did not populate requested range", r)
		}
	}

	target := ranges.Range{Pos: 1536 * 1024, Size: 250}
	if err := dls.EnsureDownloader(target); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !item.HasRange(target) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !item.HasRange(target) {
		t.Fatal("EnsureDownloader did not asynchronously populate requested range")
	}
}

func TestCloseCancelsParallelDownloaderBeforeStoppingBuffer(t *testing.T) {
	ctx := objectio.WithConfig(context.Background(), objectio.Config{BufferSize: 4 * 1024 * 1024, LowLevelRetries: 1})
	object := &cancelBlockingObject{started: make(chan struct{}), size: 8 * 1024 * 1024}
	item := &downloaderTestItem{size: object.Size()}
	dls := New(ctx, item, &Config{ChunkSize: 1024 * 1024, ChunkSizeLimit: 1024 * 1024, ChunkStreams: 2}, "object", object)

	downloadResult := make(chan error, 1)
	go func() {
		downloadResult <- dls.Download(ranges.Range{Pos: 0, Size: 32 * 1024})
	}()
	select {
	case <-object.started:
	case <-time.After(time.Second):
		t.Fatal("parallel downloader did not start")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- dls.Close(nil) }()
	select {
	case <-closeResult:
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked in StopBuffering")
	}
	select {
	case <-downloadResult:
	case <-time.After(time.Second):
		t.Fatal("Download remained blocked after Close")
	}
}

var _ Item = (*downloaderTestItem)(nil)
var _ objectio.Object = (*downloaderTestObject)(nil)
var _ objectio.Object = (*cancelBlockingObject)(nil)
