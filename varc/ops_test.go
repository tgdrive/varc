package varc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPlanRangeShowsCachedAndMissingSegments(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(32 * 1024)
	r, err := c.Open(context.Background(), "plan", int64(len(src.data)), src, WithFingerprint("v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.WarmRange(context.Background(), 4096, 8192); err != nil {
		t.Fatal(err)
	}
	plan, err := c.Plan(context.Background(), "plan", 0, 12*1024, WithFingerprint("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Size != int64(len(src.data)) || plan.RangeLength() != 12*1024 {
		t.Fatalf("bad plan size/length: %+v", plan)
	}
	if plan.CachedBytes != 4096 || plan.MissingBytes != 8*1024 || !plan.NeedFetch() {
		t.Fatalf("bad coverage cached=%d missing=%d need=%v", plan.CachedBytes, plan.MissingBytes, plan.NeedFetch())
	}
	if got := len(plan.Segments); got != 3 {
		t.Fatalf("expected 3 segments got %d: %+v", got, plan.Segments)
	}
	if plan.Segments[0].Cached || !plan.Segments[1].Cached || plan.Segments[2].Cached {
		t.Fatalf("unexpected segments: %+v", plan.Segments)
	}
	if _, err := c.Plan(context.Background(), "plan", 0, 1, WithFingerprint("v2")); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("expected invalidated cache miss, got %v", err)
	}
}

func TestPinProtectsEntriesFromPrune(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.CacheMaxSize = 6 * 1024
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, key := range []string{"pinned", "victim"} {
		src := newCountingSource(8 * 1024)
		r, err := c.Open(context.Background(), key, int64(len(src.data)), src)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.WarmAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = r.Close()
		// Ensure deterministic LRU order on filesystems with coarse time.
		time.Sleep(time.Millisecond)
	}
	if err := c.Pin(context.Background(), "pinned"); err != nil {
		t.Fatal(err)
	}
	pinned, err := c.IsPinned("pinned")
	if err != nil || !pinned {
		t.Fatalf("expected pinned=true err=%v", err)
	}
	stats, err := c.Prune(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Removed == 0 {
		t.Fatalf("expected prune to remove an unpinned entry: %+v", stats)
	}
	if !c.Exists("pinned") {
		t.Fatal("pinned entry was evicted")
	}
	if err := c.Unpin(context.Background(), "pinned"); err != nil {
		t.Fatal(err)
	}
	pinned, err = c.IsPinned("pinned")
	if err != nil || pinned {
		t.Fatalf("expected pinned=false err=%v", err)
	}
}

func TestWarmBatchManifestAndHealth(t *testing.T) {
	c, _ := openTestCache(t)
	srcA := newCountingSource(12 * 1024)
	srcB := newCountingSource(20 * 1024)
	results, err := c.WarmBatch(context.Background(), []WarmJob{
		{Key: "a", Size: int64(len(srcA.data)), Source: srcA, Ranges: []byteRange{{Start: 0, End: 4096}}, OpenOptions: []OpenOption{WithFingerprint("a1")}},
		{Key: "b", Size: int64(len(srcB.data)), Source: srcB, Ranges: []byteRange{{Start: 4096, End: 12288}}, OpenOptions: []OpenOption{WithAttr("kind", "video")}},
	}, WarmOptions{Concurrency: 2, Class: "startup"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].WarmedBytes != 4096 || results[1].WarmedBytes != 8192 {
		t.Fatalf("bad warm results: %+v", results)
	}
	plan, err := c.Plan(context.Background(), "b", 4096, 12288)
	if err != nil || plan.MissingBytes != 0 {
		t.Fatalf("bad warmed plan %+v err=%v", plan, err)
	}
	var buf bytes.Buffer
	if err := c.ExportManifest(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "\"key\": \"a\"") || !strings.Contains(buf.String(), "\"key\": \"b\"") {
		t.Fatalf("manifest missing keys: %s", buf.String())
	}
	var manifest Manifest
	if err := json.Unmarshal(buf.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != manifestVersion || len(manifest.Entries) != 2 {
		t.Fatalf("bad manifest: %+v", manifest)
	}

	clone, err := New(context.Background(), testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	st, err := clone.ImportManifest(context.Background(), bytes.NewReader(buf.Bytes()), ImportOptions{MarkImported: true})
	if err != nil {
		t.Fatal(err)
	}
	if st.Imported != 2 || st.Skipped != 0 {
		t.Fatalf("bad import stats: %+v", st)
	}
	if _, _, _, err := clone.Coverage("a"); err != nil {
		t.Fatalf("imported coverage should have metadata: %v", err)
	}
	health := c.Health(context.Background())
	if !health.Writable || health.Entries != 2 || health.BytesUsed == 0 {
		t.Fatalf("bad health: %+v", health)
	}
}

func TestRepairDropsBadRangesAndMissingMeta(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(8 * 1024)
	r, err := c.Open(context.Background(), "repair", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmRange(context.Background(), 0, 4096); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	meta, ok, err := loadMeta(c.MetaPath("repair"))
	if err != nil || !ok {
		t.Fatalf("load meta ok=%v err=%v", ok, err)
	}
	meta.Ranges = append(meta.Ranges, byteRange{Start: 7000, End: 9000})
	if err := saveMeta(c.MetaPath("repair"), meta, c.dirMode, false); err != nil {
		t.Fatal(err)
	}
	stats, err := c.Repair(context.Background(), RepairOptions{DropBadRanges: true, TouchRepaired: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.BadRanges == 0 || stats.Repaired == 0 {
		t.Fatalf("expected repair of bad ranges: %+v", stats)
	}
	meta, _, err = loadMeta(c.MetaPath("repair"))
	if err != nil {
		t.Fatal(err)
	}
	if !rangesValid(meta.Ranges, meta.Size) {
		t.Fatalf("ranges still invalid: %+v", meta.Ranges)
	}
	if meta.Attrs[attrLastRepair] == "" {
		t.Fatal("repair timestamp attr missing")
	}

	missingMeta := c.MetaPath("missing-data")
	m := cacheMeta{Version: metaVersion, Key: "missing-data", Size: 10, CreatedAt: time.Now(), UpdatedAt: time.Now(), AccessedAt: time.Now(), Ranges: []byteRange{{Start: 0, End: 10}}}
	if err := saveMeta(missingMeta, m, c.dirMode, false); err != nil {
		t.Fatal(err)
	}
	stats, err = c.Repair(context.Background(), RepairOptions{RemoveMissingData: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.MissingData == 0 || c.Exists("missing-data") {
		t.Fatalf("expected missing data metadata removal: %+v", stats)
	}
}

func TestWarmBatchContextCancelNoDeadlock(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(64 * 1024)
	src.delay = 2 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make([]WarmJob, 16)
	for i := range jobs {
		jobs[i] = WarmJob{Key: "cancel-" + string(rune('a'+i)), Size: int64(len(src.data)), Source: src}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var results []WarmResult
	var err error
	go func() {
		defer wg.Done()
		results, err = c.WarmBatch(ctx, jobs, WarmOptions{Concurrency: 3, StopOnError: true})
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WarmBatch did not exit after context cancellation")
	}
	if err == nil && len(results) == len(jobs) {
		// It is possible for a very fast system to finish before cancellation, but
		// with delayed source this should normally return a cancellation error.
		t.Fatal("expected cancellation or partial results")
	}
}

func TestManifestImportRequireDataFiles(t *testing.T) {
	c, _ := openTestCache(t)
	m := Manifest{Version: manifestVersion, CreatedAt: time.Now(), Entries: []ManifestEntry{{Key: "ghost", Size: 99, CreatedAt: time.Now(), UpdatedAt: time.Now(), AccessedAt: time.Now(), Ranges: []byteRange{{Start: 0, End: 99}}}}}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatal(err)
	}
	stats, err := c.ImportManifest(context.Background(), &buf, ImportOptions{RequireDataFiles: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Imported != 0 || stats.Skipped != 1 {
		t.Fatalf("expected skip without data file: %+v", stats)
	}
	if _, err := os.Stat(c.MetaPath("ghost")); !os.IsNotExist(err) {
		t.Fatalf("ghost metadata should not exist, stat err=%v", err)
	}
}

func TestPlanCacheOnlyAfterManifestImportWithoutData(t *testing.T) {
	c, _ := openTestCache(t)
	m := Manifest{Version: manifestVersion, CreatedAt: time.Now(), Entries: []ManifestEntry{{Key: "meta-only", Size: 100, CreatedAt: time.Now(), UpdatedAt: time.Now(), AccessedAt: time.Now(), Ranges: []byteRange{{Start: 0, End: 50}}}}}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ImportManifest(context.Background(), &buf, ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	plan, err := c.Plan(context.Background(), "meta-only", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if plan.OnDisk || plan.CachedBytes != 0 || plan.MissingBytes != 100 {
		t.Fatalf("bad metadata-only plan: %+v", plan)
	}
	_, err = c.Open(context.Background(), "meta-only", -1, nil)
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("cache-only open without data should miss, got %v", err)
	}
}

func TestCopyToPropagatesWriterShortWrite(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "copy-short", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = r.CopyTo(context.Background(), shortWriter{})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected ErrShortWrite got %v", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}
