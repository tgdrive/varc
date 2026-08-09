package chunkstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"varc/internal/engine/objectio"
)

type openedRange struct {
	start int64
	end   int64 // half-open
}

type testObject struct {
	data []byte
	mu   sync.Mutex
	open []openedRange
}

func (o *testObject) Size() int64 { return int64(len(o.data)) }

func (o *testObject) Open(_ context.Context, span *objectio.Span) (io.ReadCloser, error) {
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
	o.open = append(o.open, openedRange{start: start, end: end})
	o.mu.Unlock()
	return io.NopCloser(bytes.NewReader(o.data[start:end])), nil
}

func (o *testObject) ranges() []openedRange {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]openedRange(nil), o.open...)
}

func TestSequentialChunkGrowth(t *testing.T) {
	o := &testObject{data: bytes.Repeat([]byte("x"), 64)}
	reader := New(context.Background(), o, 4, 16, 0)
	defer reader.Close()

	buf := make([]byte, 28)
	if n, err := io.ReadFull(reader, buf); err != nil || n != len(buf) {
		t.Fatalf("ReadFull = %d, %v", n, err)
	}

	got := o.ranges()
	want := []openedRange{{0, 4}, {4, 12}, {12, 28}}
	if len(got) != len(want) {
		t.Fatalf("opened ranges = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("opened ranges = %+v, want %+v", got, want)
		}
	}
}

func TestParallelStartsConfiguredStreams(t *testing.T) {
	const mib = 1024 * 1024
	o := &testObject{data: bytes.Repeat([]byte("x"), 4*mib)}
	reader := New(context.Background(), o, mib, -1, 3)
	if _, err := reader.Open(); err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	deadline := time.Now().Add(2 * time.Second)
	for len(o.ranges()) < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := o.ranges()
	if len(got) < 3 {
		t.Fatalf("opened only %d parallel streams: %+v", len(got), got)
	}
	got = got[:3]
	sort.Slice(got, func(i, j int) bool { return got[i].start < got[j].start })
	want := []openedRange{{0, mib}, {mib, 2 * mib}, {2 * mib, 3 * mib}}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parallel ranges = %+v, want %+v", got, want)
		}
	}
}

func TestReaderErrorsAfterClose(t *testing.T) {
	for _, streams := range []int{0, 3} {
		t.Run(fmt.Sprintf("streams_%d", streams), func(t *testing.T) {
			o := &testObject{data: bytes.Repeat([]byte("x"), 2*1024*1024)}
			reader := New(context.Background(), o, 1024*1024, 1024*1024, streams)
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err == nil {
				t.Fatal("second Close unexpectedly succeeded")
			}
			if _, err := reader.Read(make([]byte, 1)); err == nil {
				t.Fatal("Read after Close unexpectedly succeeded")
			}
			if _, err := reader.Seek(1, io.SeekCurrent); err == nil {
				t.Fatal("Seek after Close unexpectedly succeeded")
			}
			if _, err := reader.RangeSeek(context.Background(), 1, io.SeekCurrent, 0); err == nil {
				t.Fatal("RangeSeek after Close unexpectedly succeeded")
			}
		})
	}
}

func TestRangeSeekReadsExactBytes(t *testing.T) {
	content := make([]byte, 2*1024*1024+257)
	for i := range content {
		content[i] = byte(i % 251)
	}
	for _, streams := range []int{0, 3} {
		t.Run(fmt.Sprintf("streams_%d", streams), func(t *testing.T) {
			reader := New(context.Background(), &testObject{data: content}, 1024*1024, 1024*1024, streams)
			defer reader.Close()
			for _, offset := range []int64{0, 1, 31, 1024, 1024*1024 - 17, 1024*1024 + 13, int64(len(content) - 33)} {
				gotOffset, err := reader.RangeSeek(context.Background(), offset, io.SeekStart, 32)
				if err != nil {
					t.Fatalf("RangeSeek(%d): %v", offset, err)
				}
				if gotOffset != offset {
					t.Fatalf("RangeSeek(%d) offset = %d", offset, gotOffset)
				}
				buf := make([]byte, 32)
				n, err := reader.Read(buf)
				end := offset + int64(len(buf))
				if end > int64(len(content)) {
					end = int64(len(content))
				}
				if n != int(end-offset) {
					t.Fatalf("Read at %d = %d bytes, want %d", offset, n, end-offset)
				}
				if !bytes.Equal(buf[:n], content[offset:end]) {
					t.Fatalf("Read at %d returned wrong bytes", offset)
				}
				if n < len(buf) && err != io.EOF {
					t.Fatalf("Read at %d error = %v, want EOF", offset, err)
				}
			}
		})
	}
}

var _ objectio.Object = (*testObject)(nil)
