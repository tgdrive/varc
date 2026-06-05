package varc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type flakySource struct {
	data      []byte
	failures  atomic.Int64
	failUntil int64
}

func (s *flakySource) ReadAt(p []byte, off int64) (int, error) {
	if s.failures.Add(1) <= s.failUntil {
		return 0, errors.New("temporary source failure")
	}
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestOptionsValidationAccessorsStatsAndClosedPaths(t *testing.T) {
	if err := validateOptions(&Options{}); err == nil {
		t.Fatal("expected missing cache dir error")
	}
	if _, err := New(context.Background(), Options{CacheDir: t.TempDir(), ShardLevel: 9}); err == nil {
		t.Fatal("expected high shard level error")
	}
	if _, err := New(context.Background(), Options{CacheDir: t.TempDir(), ChunkStreams: 2048}); err == nil {
		t.Fatal("expected high chunk stream error")
	}

	opt := testOptions(t.TempDir())
	opt.BlockSize = 5000
	opt.ChunkSize = 1024
	opt.ChunkSizeLimit = 2048
	opt.ChunkStreams = 0
	opt.MaxInflightBytes = 0
	opt.ReadAhead = 99999
	opt.ShardLevel = -1
	opt.FileMode = 0
	opt.DirMode = 0
	opt.ReadRetryCount = -1
	opt.ReadRetryDelay = -1
	opt.TouchInterval = 0
	c, err := New(context.TODO(), opt)
	if err != nil {
		t.Fatalf("New normalized options: %v", err)
	}
	if c.CacheDir() != opt.CacheDir || c.BlockSize() != 1024 || c.ChunkSize() != 1024 {
		t.Fatalf("bad accessors dir=%q block=%d chunk=%d", c.CacheDir(), c.BlockSize(), c.ChunkSize())
	}
	if got := c.KeyPath("abc"); !strings.HasPrefix(got, c.CacheDir()) || c.MetaPath("abc") != got+".meta" {
		t.Fatalf("bad key/meta path got=%q meta=%q", got, c.MetaPath("abc"))
	}
	if c.Exists("") {
		t.Fatal("empty key should not exist")
	}
	if m := c.Metrics(); m.OpenTrackedEntries != 0 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
	if st := c.Stats(); st["files"] == nil || st["bytesUsed"] == nil {
		t.Fatalf("stats missing fields: %+v", st)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := c.Open(context.Background(), "x", 1, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrClosed) {
		t.Fatalf("Open closed err=%v", err)
	}
	if _, err := c.ListEntries(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("List closed err=%v", err)
	}
	if _, err := c.Prune(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Prune closed err=%v", err)
	}
	if _, err := c.Plan(context.Background(), "x", 0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Plan closed err=%v", err)
	}
	if _, err := c.WarmBatch(context.Background(), nil, WarmOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("WarmBatch closed err=%v", err)
	}
	if _, err := c.ImportManifest(context.Background(), strings.NewReader(`{"version":1}`), ImportOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Import closed err=%v", err)
	}
	if _, err := c.Repair(context.Background(), RepairOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Repair closed err=%v", err)
	}
	if err := c.Remove("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove closed err=%v", err)
	}
}

func TestReaderClosedInvalidReadSeekWarmAndCopyPaths(t *testing.T) {
	c, _ := openTestCache(t)
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "edges", int64(len(src.data)), src, WithAttr("", "ignored"))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 0)
	if n, err := r.ReadAtContext(context.TODO(), buf, 0); n != 0 || err != nil {
		t.Fatalf("zero read n=%d err=%v", n, err)
	}
	if _, err := r.ReadAt(make([]byte, 1), -1); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("negative ReadAt err=%v", err)
	}
	if _, err := r.Seek(0, 99); err == nil {
		t.Fatal("expected bad whence seek error")
	}
	if _, err := r.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("expected negative seek error")
	}
	if err := r.WarmRange(context.Background(), -1, 1); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("warm negative err=%v", err)
	}
	if err := r.WarmRange(context.Background(), int64(len(src.data))+1, int64(len(src.data))+2); err != io.EOF {
		t.Fatalf("warm past EOF err=%v", err)
	}
	if _, err := r.CopyTo(context.Background(), nil); err == nil {
		t.Fatal("expected nil writer error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.CopyTo(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy canceled err=%v", err)
	}
	if err := r.SetAttr("kind", "video"); err != nil {
		t.Fatal(err)
	}
	if v, ok := r.Attr("kind"); !ok || v != "video" {
		t.Fatalf("attr got=%q ok=%v", v, ok)
	}
	if err := r.RemoveAttr("kind"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Attr("kind"); ok {
		t.Fatal("attr still present after remove")
	}
	if err := r.SetAttr("", "bad"); err == nil {
		t.Fatal("expected empty attr error")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second reader close: %v", err)
	}
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("read closed err=%v", err)
	}
	if _, err := r.ReadAtContext(context.Background(), make([]byte, 1), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("readatcontext closed err=%v", err)
	}
	if _, err := r.PlanRange(context.Background(), 0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("plan closed err=%v", err)
	}
}

func TestStrictFingerprintCacheOnlyAndSizeInvalidation(t *testing.T) {
	c, _ := openTestCache(t)
	if _, err := c.Open(context.Background(), "missing", 10, newCountingSource(10), WithCacheOnly()); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("cache-only with source should ignore source and miss, got %v", err)
	}
	src := newCountingSource(2048)
	r, err := c.Open(context.Background(), "fp", int64(len(src.data)), src, WithFingerprint("old"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.Fingerprint() != "old" || !r.Complete() || r.CachedBytes() != int64(len(src.data)) || len(r.CachedRanges()) != 1 {
		t.Fatalf("bad reader metadata fp=%q complete=%v cached=%d ranges=%+v", r.Fingerprint(), r.Complete(), r.CachedBytes(), r.CachedRanges())
	}
	_ = r.Close()

	if _, err := c.Plan(context.Background(), "fp", 0, -1, WithStrictFingerprint()); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("strict empty fingerprint should invalidate, got %v", err)
	}
	changed := newCountingSource(1024)
	r2, err := c.Open(context.Background(), "fp", int64(len(changed.data)), changed)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Size() != int64(len(changed.data)) || r2.Fingerprint() != "" {
		t.Fatalf("size/fingerprint not updated size=%d fp=%q", r2.Size(), r2.Fingerprint())
	}
	if cached, err := c.RangeCached("fp", 0, 1); !errors.Is(err, ErrCacheMiss) || cached {
		t.Fatalf("new size should have no data cached=%v err=%v", cached, err)
	}
	if _, err := c.RangeCached("", 0, 1); err == nil {
		t.Fatal("expected empty key range error")
	}
	if _, err := c.RangeCached("fp", -1, 1); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("negative range err=%v", err)
	}
}

func TestReadRetryMetricsAndWaitCompleteErrors(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.ReadRetryCount = 2
	opt.ReadRetryDelay = time.Nanosecond
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	data := []byte(strings.Repeat("retry", 512))
	src := &flakySource{data: data, failUntil: 1}
	r, err := c.Open(context.Background(), "retry", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatalf("read after retry: %v", err)
	}
	if !bytes.Equal(buf, data[:64]) || src.failures.Load() < 2 {
		t.Fatalf("retry did not happen failures=%d", src.failures.Load())
	}
	m := c.Metrics()
	if m.Reads == 0 || m.Misses == 0 || m.SourceReads == 0 || m.MetaWrites == 0 {
		t.Fatalf("metrics not populated: %+v", m)
	}
	if err := c.WaitComplete(context.Background(), "does-not-exist"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("wait missing err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WaitComplete(ctx, "retry"); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait canceled err=%v", err)
	}
}

func TestPruneByAgeOpenReaderProtectionCleanOnStartAndJanitor(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.CacheMaxAge = time.Nanosecond
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	src := newCountingSource(2048)
	r, err := c.Open(context.Background(), "hot", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	st, err := c.Prune(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Removed != 0 || !c.Exists("hot") {
		t.Fatalf("open reader should be protected: %+v", st)
	}
	_ = r.Close()
	st, err = c.Prune(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Removed == 0 || st.ReasonAge == 0 || c.Exists("hot") {
		t.Fatalf("closed old entry should be age-pruned: %+v exists=%v", st, c.Exists("hot"))
	}
	_ = c.Close()

	// CleanOnStart should remove stale entries before returning.
	c1, err := New(context.Background(), testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	src2 := newCountingSource(1024)
	r2, _ := c1.Open(context.Background(), "startup", int64(len(src2.data)), src2)
	if err := r2.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r2.Close()
	_ = c1.Close()
	opt2 := testOptions(dir)
	opt2.CacheMaxAge = time.Nanosecond
	opt2.CleanOnStart = true
	time.Sleep(time.Millisecond)
	c2, err := New(context.Background(), opt2)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Exists("startup") {
		t.Fatal("CleanOnStart did not prune stale entry")
	}
	_ = c2.Close()

	// Janitor should run and update its counter.
	opt3 := testOptions(t.TempDir())
	opt3.NoBackground = false
	opt3.CachePollInterval = 5 * time.Millisecond
	c3, err := New(context.Background(), opt3)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := c3.Metrics().BackgroundPrunes; got == 0 {
		t.Fatal("background janitor did not run")
	}
	_ = c3.Close()
}

func TestChecksumVerifyRepairAndLowLevelChecksumHelpers(t *testing.T) {
	dir := t.TempDir()
	opt := testOptions(dir)
	opt.VerifyChecksum = true
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := newCountingSource(4096)
	r, err := c.Open(context.Background(), "sum", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	vs, err := c.Verify(context.Background())
	if err != nil || vs.ChecksumErrors != 0 || vs.Complete != 1 {
		t.Fatalf("verify before corruption stats=%+v err=%v", vs, err)
	}
	path := c.KeyPath("sum")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	vs, err = c.Verify(context.Background())
	if err == nil || vs.ChecksumErrors == 0 {
		t.Fatalf("expected checksum failure stats=%+v err=%v", vs, err)
	}
	rs, err := c.Repair(context.Background(), RepairOptions{DropBadChecksums: true, TouchRepaired: true})
	if err != nil || rs.ChecksumFailures == 0 || rs.Repaired == 0 {
		t.Fatalf("repair checksum stats=%+v err=%v", rs, err)
	}
	meta, ok, err := loadMeta(c.MetaPath("sum"))
	if err != nil || !ok {
		t.Fatalf("load meta: ok=%v err=%v", ok, err)
	}
	if len(meta.Checksums) >= 4 || meta.Attrs[attrLastRepair] == "" {
		t.Fatalf("repair did not drop bad checksum/touch: %+v", meta)
	}
	if got := checksumIEEE([]byte("abc")); got != crc32.ChecksumIEEE([]byte("abc")) {
		t.Fatalf("checksum wrapper got=%d", got)
	}
	original := []blockChecksum{{Start: 0, End: 1, CRC32: 1}}
	cloned := cloneChecksums(original)
	cloned[0].CRC32 = 2
	if cloneChecksums(nil) != nil || original[0].CRC32 != 1 {
		t.Fatal("clone checksum edge failed")
	}
}

func TestManifestImportExportOverwriteErrorsAndDryRunRepair(t *testing.T) {
	c, _ := openTestCache(t)
	if err := c.ExportManifest(context.Background(), nil); err == nil {
		t.Fatal("expected nil writer export error")
	}
	if _, err := c.ImportManifest(context.Background(), nil, ImportOptions{}); err == nil {
		t.Fatal("expected nil reader import error")
	}
	if _, err := c.ImportManifest(context.Background(), strings.NewReader(`{"version":999}`), ImportOptions{}); err == nil {
		t.Fatal("expected unsupported manifest version error")
	}
	badManifest := Manifest{Version: manifestVersion, Entries: []ManifestEntry{{Key: "", Size: -1}}}
	var bad bytes.Buffer
	if err := json.NewEncoder(&bad).Encode(badManifest); err != nil {
		t.Fatal(err)
	}
	st, err := c.ImportManifest(context.Background(), &bad, ImportOptions{})
	if err == nil || st.Skipped != 1 || len(st.Errors) == 0 {
		t.Fatalf("bad manifest stats=%+v err=%v", st, err)
	}

	src := newCountingSource(1024)
	r, err := c.Open(context.Background(), "manifest", int64(len(src.data)), src, WithFingerprint("one"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	var manifest bytes.Buffer
	if err := c.ExportManifest(context.Background(), &manifest); err != nil {
		t.Fatal(err)
	}
	imp, err := c.ImportManifest(context.Background(), bytes.NewReader(manifest.Bytes()), ImportOptions{})
	if err != nil || imp.Skipped == 0 || imp.Imported != 0 {
		t.Fatalf("existing import should skip stats=%+v err=%v", imp, err)
	}
	imp, err = c.ImportManifest(context.Background(), bytes.NewReader(manifest.Bytes()), ImportOptions{Overwrite: true})
	if err != nil || imp.Imported == 0 {
		t.Fatalf("overwrite import stats=%+v err=%v", imp, err)
	}

	// Corrupt meta should be reported but preserved in dry-run repair.
	corrupt := filepath.Join(c.CacheDir(), "bad.meta")
	if err := os.WriteFile(corrupt, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	repair, err := c.Repair(context.Background(), RepairOptions{DryRun: true, RemoveCorruptMeta: true})
	if err == nil || repair.CorruptMeta == 0 || repair.Removed != 0 {
		t.Fatalf("dry-run repair stats=%+v err=%v", repair, err)
	}
	if _, err := os.Stat(corrupt); err != nil {
		t.Fatalf("dry run removed corrupt meta: %v", err)
	}
}

func TestRangePlanningAndUtilityHelpers(t *testing.T) {
	seg := RangeSegment{Start: 10, End: 4}
	if seg.Length() != 0 {
		t.Fatalf("negative segment length=%d", seg.Length())
	}
	if (RangePlan{}).RangeLength() != 0 || (RangePlan{}).CoveragePercent() != 100 {
		t.Fatal("empty plan length/coverage failed")
	}
	plan := RangePlan{Start: 0, End: 100, CachedBytes: 25}
	if plan.RangeLength() != 100 || plan.CoveragePercent() != 25 || !(RangePlan{MissingBytes: 1}).NeedFetch() {
		t.Fatalf("plan helper failed %+v", plan)
	}
	ranges := normalizeRanges([]byteRange{{Start: 10, End: 20}, {Start: -5, End: 5}, {Start: 5, End: 10}, {Start: 50, End: 40}, {Start: 90, End: 150}}, 100)
	want := []byteRange{{Start: 0, End: 20}, {Start: 90, End: 100}}
	if !sameRanges(ranges, want) {
		t.Fatalf("normalize got=%+v want=%+v", ranges, want)
	}
	if !containsRange(ranges, 0, 10) || containsRange(ranges, 20, 30) {
		t.Fatalf("contains failed: %+v", ranges)
	}
	ms := missingRanges(ranges, 0, 100)
	if !sameRanges(ms, []byteRange{{Start: 20, End: 90}}) {
		t.Fatalf("missing=%+v", ms)
	}
	start, end, ok := firstMissingRange(ranges, 0, 100)
	if !ok || start != 20 || end != 90 {
		t.Fatalf("first missing start=%d end=%d ok=%v", start, end, ok)
	}
	if got := addRange([]byteRange{{Start: 0, End: 10}}, 10, 20); !sameRanges(got, []byteRange{{Start: 0, End: 20}}) {
		t.Fatalf("addRange=%+v", got)
	}
	if rangesLen(ranges) != 30 || !rangesValid(ranges, 100) || rangesValid([]byteRange{{Start: 10, End: 9}}, 100) {
		t.Fatalf("ranges len/valid failed ranges=%+v", ranges)
	}
	if alignDown(15, 4) != 12 || roundUp(15, 4) != 16 || roundUp(16, 4) != 16 || min64(1, 2) != 1 || max64(1, 2) != 2 || maxInt(1, 2) != 2 {
		t.Fatal("numeric helpers failed")
	}
	if joinErrors(nil, errors.New("a"), errors.New("b")).Error() != "a; b" || joinErrors(nil) != nil {
		t.Fatal("joinErrors failed")
	}
	if !isTempFile("abc.tmp") || !isTempFile("abc~") || isTempFile("abc") {
		t.Fatal("isTempFile failed")
	}
	m := map[string]string{"a": "b"}
	clone := cloneStringMap(m)
	clone["a"] = "c"
	if m["a"] != "b" || cloneStringMap(nil) != nil {
		t.Fatal("cloneStringMap failed")
	}
}

func TestFileInfoSnapshotAndMetaCorruptionPaths(t *testing.T) {
	c, _ := openTestCache(t)
	if _, err := c.FileInfo("missing"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("missing fileinfo err=%v", err)
	}
	if _, err := c.SnapshotMeta("missing"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("missing snapshot err=%v", err)
	}
	src := newCountingSource(1024)
	r, err := c.Open(context.Background(), "dir/name", int64(len(src.data)), src, WithModTime(time.Unix(100, 0)), WithAttr("x", "y"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WarmAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	fi, err := c.FileInfo("dir/name")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "name" || fi.Size() != int64(len(src.data)) || fi.Mode() != 0o444 || fi.IsDir() || fi.Sys() != nil || fi.ModTime().Unix() != 100 {
		t.Fatalf("bad fileinfo %+v", fi)
	}
	snap, err := c.SnapshotMeta("dir/name")
	if err != nil || snap["key"] != "dir/name" || snap["attrs"] == nil {
		t.Fatalf("bad snapshot %+v err=%v", snap, err)
	}
	if !c.IsComplete("dir/name") {
		t.Fatal("expected complete")
	}

	// Corrupt metadata should be surfaced by list/verify/prune invalid path.
	if err := os.MkdirAll(filepath.Dir(c.MetaPath("corrupt")), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.MetaPath("corrupt"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := c.ListEntries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if strings.HasSuffix(e.MetaPath, filepath.Base(c.MetaPath("corrupt"))) && e.MetadataErr != nil {
			found = true
		}
	}
	if !found {
		t.Fatalf("corrupt meta not surfaced: %+v", entries)
	}
	vs, err := c.Verify(context.Background())
	if err == nil || vs.CorruptMeta == 0 {
		t.Fatalf("verify corrupt stats=%+v err=%v", vs, err)
	}
	ps, err := c.Prune(context.Background())
	if err != nil || ps.ReasonInvalid == 0 || ps.Removed == 0 {
		t.Fatalf("prune corrupt stats=%+v err=%v", ps, err)
	}
}

func TestSyncWritesAccessorsNoopLoggerAndRangeCachedSuccess(t *testing.T) {
	noopLogger{}.Debugf("debug")
	noopLogger{}.Infof("info")
	noopLogger{}.Warnf("warn")
	noopLogger{}.Errorf("error")

	dir := t.TempDir()
	opt := testOptions(dir)
	opt.SyncWrites = true
	c, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mod := time.Unix(777, 0).UTC()
	src := newCountingSource(2048)
	r, err := c.Open(context.Background(), "range-ok", int64(len(src.data)), src, WithModTime(mod))
	if err != nil {
		t.Fatal(err)
	}
	if r.ModTime().Unix() != mod.Unix() {
		t.Fatalf("modtime accessor got=%v want=%v", r.ModTime(), mod)
	}
	if err := r.WarmRange(context.Background(), 0, 1024); err != nil {
		t.Fatal(err)
	}
	if cached, err := c.RangeCached("range-ok", 0, 512); err != nil || !cached {
		t.Fatalf("expected range cached cached=%v err=%v", cached, err)
	}
	if cached, err := c.RangeCached("range-ok", 0, 512, WithFingerprint("missing")); !errors.Is(err, ErrCacheMiss) || cached {
		t.Fatalf("fingerprint mismatch cached=%v err=%v", cached, err)
	}
	if cached, err := c.RangeCached("range-ok", 0, int64(len(src.data))+1); err != io.EOF || cached {
		t.Fatalf("range past EOF cached=%v err=%v", cached, err)
	}
	_ = r.Close()
}

func TestWarmBatchErrorModesAndPlanRangeContext(t *testing.T) {
	c, _ := openTestCache(t)
	results, err := c.WarmBatch(context.Background(), []WarmJob{{Key: "", Size: 1, Source: newCountingSource(1)}}, WarmOptions{StopOnError: true})
	if err == nil || len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected stop-on-error warm result=%+v err=%v", results, err)
	}
	results, err = c.WarmBatch(context.Background(), []WarmJob{{Key: "nosrc", Size: 1}}, WarmOptions{})
	if err == nil || !errors.Is(results[0].Err, ErrSourceRequired) {
		t.Fatalf("expected source required result=%+v err=%v", results, err)
	}
	results, err = c.WarmBatch(context.Background(), []WarmJob{{Key: "badsize", Size: -1, Source: newCountingSource(1)}}, WarmOptions{})
	if err == nil || results[0].Err == nil {
		t.Fatalf("expected bad size result=%+v err=%v", results, err)
	}
	src := newCountingSource(1024)
	r, err := c.Open(context.Background(), "planctx", int64(len(src.data)), src)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.PlanRange(ctx, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("plan canceled err=%v", err)
	}
	if _, err := r.PlanRange(context.Background(), -1, 1); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("plan invalid err=%v", err)
	}
	_ = r.Close()
}
