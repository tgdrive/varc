package downloaders

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"vfs-cache/internal/cachecore/fs"
	"vfs-cache/internal/cachecore/vfscommon"
	"vfs-cache/ranges"
)

type downloaderTestItem struct {
	mu sync.Mutex
	rs ranges.Ranges
}

func (i *downloaderTestItem) FindMissing(r ranges.Range) ranges.Range {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.rs.FindMissing(r)
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

func (o *downloaderTestObject) Size() int64 { return int64(len(o.data)) }

func (o *downloaderTestObject) Open(_ context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	start := int64(0)
	end := int64(len(o.data))
	for _, option := range options {
		if r, ok := option.(*fs.RangeOption); ok {
			start = r.Start
			if r.End >= 0 {
				end = r.End + 1
			}
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
	ctx := fs.WithConfig(context.Background(), fs.ConfigInfo{BufferSize: 0})
	item := new(downloaderTestItem)
	object := &downloaderTestObject{data: bytes.Repeat([]byte("x"), 128*1024)}
	opt := &vfscommon.Options{ChunkSize: 4096, ChunkSizeLimit: 4096}
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

var _ Item = (*downloaderTestItem)(nil)
var _ fs.Object = (*downloaderTestObject)(nil)
