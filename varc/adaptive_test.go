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

func newAdaptiveTestReader(base, limit int64) *Reader {
	return &Reader{cache: &Cache{chunkSize: base, chunkSizeLimit: limit}}
}

func TestAdaptiveChunkGrowth(t *testing.T) {
	r := newAdaptiveTestReader(32, 128)
	reads := []struct {
		off, n int64
		want   int64
	}{
		{0, 4, 32},
		{4, 4, 64},
		{8, 4, 64},
		{12, 4, 128},
		{16, 4, 128},
	}
	for i, tc := range reads {
		r.beginAdaptiveAccess(tc.off)
		r.finishAdaptiveAccess(tc.off, tc.n)
		if got := r.adaptiveChunkSize(); got != tc.want {
			t.Fatalf("read %d chunk size=%d, want %d", i, got, tc.want)
		}
	}
}

func TestAdaptiveRandomAccessAndSeekResetGrowth(t *testing.T) {
	r := newAdaptiveTestReader(32, 128)
	for i := int64(0); i < 4; i++ {
		off := i * 4
		r.beginAdaptiveAccess(off)
		r.finishAdaptiveAccess(off, 4)
	}
	if got := r.adaptiveChunkSize(); got != 128 {
		t.Fatalf("grown chunk size=%d, want 128", got)
	}

	r.beginAdaptiveAccess(4096)
	if got := r.adaptiveChunkSize(); got != 32 {
		t.Fatalf("random access chunk size=%d, want 32", got)
	}

	r.finishAdaptiveAccess(4096, 4)
	r.resetAdaptive(8192)
	if got := r.adaptiveChunkSize(); got != 32 {
		t.Fatalf("seek reset chunk size=%d, want 32", got)
	}
}

func TestAdaptiveStateIsPerReader(t *testing.T) {
	cache := &Cache{chunkSize: 32, chunkSizeLimit: 128}
	sequential := &Reader{cache: cache}
	random := &Reader{cache: cache}
	for i := int64(0); i < 4; i++ {
		off := i * 4
		sequential.beginAdaptiveAccess(off)
		sequential.finishAdaptiveAccess(off, 4)
	}
	random.beginAdaptiveAccess(9000)
	random.finishAdaptiveAccess(9000, 4)
	if got := sequential.adaptiveChunkSize(); got != 128 {
		t.Fatalf("sequential reader chunk size=%d, want 128", got)
	}
	if got := random.adaptiveChunkSize(); got != 32 {
		t.Fatalf("random reader chunk size=%d, want 32", got)
	}
}

func TestPreloadChunksScheduleDistinctTasksAndDeduplicate(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64
	opt.ChunkSizeLimit = 256
	opt.PreloadChunks = 2
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := newCountingSource(512)
	r, err := c.Open(context.Background(), "adaptive-preload", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	st := r.state
	st.mu.Lock()
	r.ensureAdaptiveTasksLocked(st, 0, 1, int64(len(src.data)))
	r.ensureAdaptiveTasksLocked(st, 0, 1, int64(len(src.data)))
	if got := len(st.tasks); got != 3 {
		st.mu.Unlock()
		t.Fatalf("tasks=%d, want active plus two preloads", got)
	}
	want := []byteRange{{Start: 0, End: 64}, {Start: 64, End: 128}, {Start: 128, End: 192}}
	for _, expected := range want {
		if st.tasks[rangeKey(expected.Start, expected.End)] == nil {
			st.mu.Unlock()
			t.Fatalf("missing task %+v", expected)
		}
	}
	st.mu.Unlock()
}

func TestSharedReadersDeduplicateAdaptiveTasks(t *testing.T) {
	opt := testOptions(t.TempDir())
	opt.ChunkSize = 64
	opt.ChunkSizeLimit = 256
	opt.PreloadChunks = 1
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := &controlledRangeSource{
		data:       bytes.Repeat([]byte{0x44}, 256),
		firstReady: make(chan struct{}),
		release:    make(chan struct{}),
	}
	r1, err := c.Open(context.Background(), "shared-adaptive", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c.Open(context.Background(), "shared-adaptive", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	st := r1.state
	st.mu.Lock()
	r1.ensureAdaptiveTasksLocked(st, 0, 1, int64(len(src.data)))
	r2.ensureAdaptiveTasksLocked(st, 0, 1, int64(len(src.data)))
	got := len(st.tasks)
	blocking := st.tasks[rangeKey(0, 64)]
	owners := 0
	if blocking != nil {
		owners = len(blocking.owners)
	}
	st.mu.Unlock()
	if got != 2 {
		t.Fatalf("shared tasks=%d, want one active and one preload", got)
	}
	if owners != 2 {
		t.Fatalf("blocking task owners=%d, want 2", owners)
	}

	r1.cancelStaleTasks()
	st.mu.Lock()
	blockingCanceled := blocking.ctx.Err() != nil
	owners = len(blocking.owners)
	st.mu.Unlock()
	if blockingCanceled || owners != 1 {
		t.Fatalf("detaching one reader canceled shared blocking task: canceled=%v owners=%d", blockingCanceled, owners)
	}
	close(src.release)
}

type priorityRangeSource struct {
	data         []byte
	firstStarted chan struct{}
	releaseFirst chan struct{}
	once         sync.Once
	mu           sync.Mutex
	opens        []byteRange
}

func (s *priorityRangeSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("streaming path not used")
}

func (s *priorityRangeSource) OpenRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	index := len(s.opens)
	s.opens = append(s.opens, byteRange{Start: start, End: end + 1})
	s.mu.Unlock()
	if index == 0 {
		s.once.Do(func() { close(s.firstStarted) })
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return io.NopCloser(bytes.NewReader(s.data[start : end+1])), nil
}

func (s *priorityRangeSource) snapshotOpens() []byteRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byteRange(nil), s.opens...)
}

func TestBlockingSeekRunsBeforeQueuedPreloads(t *testing.T) {
	const chunkSize int64 = 64
	opt := testOptions(t.TempDir())
	opt.ChunkSize = chunkSize
	opt.ChunkSizeLimit = chunkSize
	opt.PreloadChunks = 2
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	src := &priorityRangeSource{
		data:         bytes.Repeat([]byte{0x62}, 6*int(chunkSize)),
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	r, err := c.Open(context.Background(), "priority-seek", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), 0)
		firstDone <- err
	}()
	<-src.firstStarted

	seekOffset := 4 * chunkSize
	seekDone := make(chan error, 1)
	go func() {
		_, err := r.ReadAt(make([]byte, 1), seekOffset)
		seekDone <- err
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := r.state
		st.mu.Lock()
		blockingQueued := false
		for _, task := range st.tasks {
			if !task.done && task.priority == priorityBlocking && task.start == seekOffset {
				blockingQueued = true
				break
			}
		}
		st.mu.Unlock()
		if blockingQueued {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(src.releaseFirst)

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(src.snapshotOpens()) >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	opens := src.snapshotOpens()
	if len(opens) < 2 {
		t.Fatalf("origin opens=%+v, want at least two", opens)
	}
	if opens[1] != (byteRange{Start: seekOffset, End: seekOffset + chunkSize}) {
		t.Fatalf("second origin open=%+v, want blocking seek %d-%d; all=%+v", opens[1], seekOffset, seekOffset+chunkSize, opens)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-seekDone; err != nil {
		t.Fatal(err)
	}
}

func TestRandomSeekCancelsOnlyStaleReaderPreloads(t *testing.T) {
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

	src := bytes.NewReader(bytes.Repeat([]byte{0x31}, 6*int(chunkSize)))
	r1, err := c.Open(context.Background(), "shared-preload-ownership", int64(src.Len()), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := c.Open(context.Background(), "shared-preload-ownership", int64(src.Len()), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	st := r1.state
	st.mu.Lock()
	r1.ensureAdaptiveTasksLocked(st, 0, 1, int64(src.Len()))
	r2.ensureAdaptiveTasksLocked(st, 0, 1, int64(src.Len()))
	preload := st.tasks[rangeKey(chunkSize, 2*chunkSize)]
	st.mu.Unlock()
	if preload == nil {
		t.Fatal("shared preload was not scheduled")
	}

	r1.beginAdaptiveAccess(4 * chunkSize)
	if err := preload.ctx.Err(); err != nil {
		t.Fatalf("preload canceled while second reader still owned it: %v", err)
	}

	r2.beginAdaptiveAccess(5 * chunkSize)
	if !errors.Is(preload.ctx.Err(), context.Canceled) {
		t.Fatalf("stale preload context error=%v, want context canceled", preload.ctx.Err())
	}
}
