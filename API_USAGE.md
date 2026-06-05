# varc — Read-through range cache for Go

> **Looking for the Caddy HTTP handler module?** See [README.md](README.md) for Caddyfile config, build instructions, and deployment.

`varc` is a production-oriented read-through cache for immutable byte streams. It is designed for media servers, HTTP range handlers, object-storage gateways, and any workload that repeatedly reads byte ranges from a slower `io.ReaderAt` source.

The cache stores fetched byte ranges in a sparse local data file and writes a `.meta` sidecar that records which ranges are available. Later reads for the same key are served from disk; missing ranges are fetched from the source, persisted, and then returned to the caller.

## Features

- Sparse range caching with persistent metadata
- Cache-only/offline reads
- Fingerprint and size-based invalidation
- Duplicate download coalescing across readers
- Bounded concurrent source reads
- Optional read-ahead
- Optional CRC32 block verification
- Background and manual pruning
- LRU/age/max-size/free-space cleanup
- Metadata attributes for cache consumers
- Metrics, cache inspection, verification, and coverage APIs
- Context-aware `ReadAtContext`, `WarmRange`, `WarmAll`, and `CopyTo`
- Safe atomic metadata writes
- Directory sharding for large caches

## Installation

The `varc` package lives under `github.com/tgdrive/varc/varc`. Import it directly:

```go
import "github.com/tgdrive/varc/varc"
```

To vendor it into your own project, copy the `varc/` directory and adjust the import path accordingly.

## Basic usage

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "log"
    "os"

    "github.com/tgdrive/varc/varc"
)

func main() {
    ctx := context.Background()

    cache, err := varc.New(ctx, varc.Options{
        CacheDir:     "./.cache/varc",
        BlockSize:    1 << 20,       // 1 MiB committed blocks
        ChunkSize:    16 << 20,      // 16 MiB downloader windows
        ChunkStreams: 4,             // max concurrent fetch workers
        ReadAhead:    4 << 20,       // opportunistic readahead
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    // Any io.ReaderAt can be a source: file, bytes.Reader, HTTP range client,
    // object store adapter, database blob reader, etc.
    data := []byte("hello from a remote object")
    src := bytes.NewReader(data)

    key := "bucket/video/example.mp4"
    size := int64(len(data))

    r, err := cache.Open(
        ctx,
        key,
        size,
        src,
        varc.WithFingerprint("etag-or-content-hash-v1"),
        varc.WithAttr("content-type", "video/mp4"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    buf := make([]byte, 5)
    n, err := r.ReadAt(buf, 0)
    if err != nil && err != io.EOF {
        log.Fatal(err)
    }

    fmt.Printf("read %d bytes: %q\n", n, buf[:n])

    cached, total, complete, err := cache.Coverage(key)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("coverage: %d/%d complete=%v\n", cached, total, complete)

    _ = os.RemoveAll("./.cache")
}
```

## Opening a cached object

```go
r, err := cache.Open(ctx, key, size, source, varc.WithFingerprint(fingerprint))
```

Arguments:

| Argument | Meaning |
|---|---|
| `ctx` | Context used for reader lifetime and cancellation. |
| `key` | Stable cache key. It is hashed on disk, but stored in metadata. |
| `size` | Required when `source != nil`; must be known and non-negative. |
| `source` | Any `io.ReaderAt`. Missing ranges are fetched from this source. |
| `OpenOption` | Optional metadata and behavior controls. |

Important rule: for a given `key` and `fingerprint`, the source should be immutable. If the remote content changes, pass a new fingerprint so stale data is discarded.

## Cache keys and fingerprints

Use a key that is stable for the logical object:

```go
key := backendName + ":" + remotePath
```

Use a fingerprint that changes whenever content changes:

```go
r, err := cache.Open(ctx, key, size, src,
    varc.WithFingerprint(etag),
    varc.WithModTime(remoteModTime),
)
```

Good fingerprint values:

- HTTP `ETag`
- S3/GCS object generation
- content hash
- database version
- inode/generation pair
- `{size}:{mtime}` fallback when no better value exists

If no fingerprint is supplied, `varc` falls back to size-based validation. That is less safe because different content can have the same size.

## Reading data

### Sequential reads

`Reader` implements `io.Reader` and `io.Seeker`:

```go
_, _ = r.Seek(1<<20, io.SeekStart)

buf := make([]byte, 64*1024)
n, err := r.Read(buf)
if err != nil && err != io.EOF {
    return err
}
_ = n
```

### Random reads

`Reader` implements `io.ReaderAt`:

```go
buf := make([]byte, 128*1024)
n, err := r.ReadAt(buf, 10<<20)
if err != nil && err != io.EOF {
    return err
}
_ = n
```

### Context-aware reads

Use `ReadAtContext` when serving HTTP requests, FUSE requests, or any request that may be cancelled by the caller:

```go
reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

buf := make([]byte, 1<<20)
n, err := r.ReadAtContext(reqCtx, buf, offset)
if err != nil {
    return err
}
_ = n
```

## Cache-only / offline mode

Open with no source to read only from already cached ranges:

```go
r, err := cache.Open(ctx, key, -1, nil)
if err != nil {
    if errors.Is(err, varc.ErrCacheMiss) {
        // The object is not present locally.
    }
    return err
}
defer r.Close()
```

You can also force cache-only behavior even if a source variable is present:

```go
r, err := cache.Open(ctx, key, -1, src, varc.WithCacheOnly())
```

A cache-only reader returns `ErrCacheMiss` when the requested range is absent.

## Warming the cache

Warm the beginning of a media file before serving it:

```go
r, err := cache.Open(ctx, key, size, src, varc.WithFingerprint(etag))
if err != nil {
    return err
}
defer r.Close()

// Cache the first 8 MiB. Useful for media headers/indexes.
if err := r.WarmRange(ctx, 0, 8<<20); err != nil {
    return err
}
```

Warm the full object:

```go
if err := r.WarmAll(ctx); err != nil {
    return err
}
```

Wait for an already-started full download to complete:

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
defer cancel()

if err := cache.WaitComplete(ctx, key); err != nil {
    return err
}
```

`WaitComplete` observes active downloads. It does not start a new download by itself.

## HTTP Range source example

`varc` requires an `io.ReaderAt`. For HTTP/object storage, implement `ReadAt` with range requests:

```go
type HTTPRangeSource struct {
    Client *http.Client
    URL    string
}

func (s HTTPRangeSource) ReadAt(p []byte, off int64) (int, error) {
    if len(p) == 0 {
        return 0, nil
    }

    req, err := http.NewRequest(http.MethodGet, s.URL, nil)
    if err != nil {
        return 0, err
    }

    end := off + int64(len(p)) - 1
    req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))

    client := s.Client
    if client == nil {
        client = http.DefaultClient
    }

    resp, err := client.Do(req)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
        return 0, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
    }

    n, err := io.ReadFull(resp.Body, p)
    if err == io.ErrUnexpectedEOF || err == io.EOF {
        return n, io.EOF
    }
    return n, err
}
```

Then use it like this:

```go
src := HTTPRangeSource{URL: remoteURL}

r, err := cache.Open(ctx, key, remoteSize, src,
    varc.WithFingerprint(remoteETag),
    varc.WithModTime(remoteModTime),
)
```

For production HTTP usage, add request contexts, retries, auth headers, status validation, and protection against servers that ignore `Range`.

## Serving HTTP range responses

A typical handler flow:

```go
func serveRange(w http.ResponseWriter, req *http.Request, cache *varc.Cache, obj Object) {
    ctx := req.Context()

    src := obj.ReaderAt()
    r, err := cache.Open(ctx, obj.CacheKey, obj.Size, src,
        varc.WithFingerprint(obj.Fingerprint),
        varc.WithModTime(obj.ModTime),
        varc.WithAttr("content-type", obj.ContentType),
    )
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadGateway)
        return
    }
    defer r.Close()

    w.Header().Set("Accept-Ranges", "bytes")
    w.Header().Set("Content-Type", obj.ContentType)
    http.ServeContent(w, req, filepath.Base(obj.Path), obj.ModTime, io.NewSectionReader(r, 0, obj.Size))
}
```

Because `Reader` implements `ReadAt`, it works with `io.NewSectionReader`.

## Copying through the cache

`CopyTo` fetches missing ranges and writes the complete object to a destination:

```go
out, err := os.Create("movie.mp4")
if err != nil {
    return err
}
defer out.Close()

written, err := r.CopyTo(ctx, out)
if err != nil {
    return err
}
fmt.Println("copied", written, "bytes")
```

## Metadata attributes

Attributes are arbitrary strings stored in the `.meta` sidecar. They are useful for cache consumers that need to remember remote object metadata (content type, backend ID, remote path, etc.).

```go
r, err := cache.Open(ctx, key, size, src,
    varc.WithFingerprint(etag),
    varc.WithAttr("backend", "s3"),
    varc.WithAttr("mime", "video/mp4"),
)
if err != nil {
    return err
}
defer r.Close()

_ = r.SetAttr("inode-hint", "123456")

mime, ok := r.Attr("mime")
if ok {
    fmt.Println(mime)
}

_ = r.RemoveAttr("inode-hint")
```

## Inspecting cache state

### Coverage

```go
cached, size, complete, err := cache.Coverage(key)
if err != nil {
    return err
}
fmt.Printf("%d/%d bytes cached, complete=%v\n", cached, size, complete)
```

### Entry listing

```go
entries, err := cache.ListEntries(ctx)
if err != nil {
    return err
}

for _, e := range entries {
    fmt.Printf("%s %.1f%% complete=%v readers=%d fetches=%d\n",
        e.Key, e.Percent, e.Complete, e.OpenReaders, e.ActiveFetches)
}
```

### File info

```go
info, err := cache.FileInfo(key)
if err != nil {
    return err
}
fmt.Println(info.Name(), info.Size(), info.ModTime())
```

### Raw metadata snapshot

```go
meta, err := cache.SnapshotMeta(key)
if err != nil {
    return err
}
fmt.Printf("%#v\n", meta)
```

## Metrics

```go
m := cache.Metrics()
fmt.Printf("opens=%d reads=%d hits=%d misses=%d inflight=%d\n",
    m.Opens,
    m.Reads,
    m.Hits,
    m.Misses,
    m.InflightBytes,
)
```

`Stats` returns a simpler map that is convenient for debugging endpoints:

```go
stats := cache.Stats()
fmt.Println(stats["files"], stats["bytesUsed"])
```

## Verification

Run consistency checks:

```go
stats, err := cache.Verify(ctx)
if err != nil {
    return err
}
fmt.Printf("entries=%d complete=%d incomplete=%d corrupt=%d checksum_errors=%d\n",
    stats.Entries,
    stats.Complete,
    stats.Incomplete,
    stats.CorruptMeta,
    stats.ChecksumErrors,
)
```

Enable checksum recording and verification:

```go
cache, err := varc.New(ctx, varc.Options{
    CacheDir:       "./.cache/varc",
    VerifyChecksum: true,
})
```

Checksum verification is useful for detecting local disk corruption, but it increases CPU work and metadata writes.

## Pruning and cleanup

Manual prune:

```go
stats, err := cache.Prune(ctx)
if err != nil {
    return err
}
fmt.Printf("removed=%d bytes=%d\n", stats.Removed, stats.RemovedBytes)
```

Production cleanup configuration:

```go
cache, err := varc.New(ctx, varc.Options{
    CacheDir:          "/var/cache/myapp/varc",
    CacheMaxAge:       7 * 24 * time.Hour,
    CacheMaxSize:      500 << 30, // 500 GiB
    CacheMinFreeSpace: 20 << 30,  // keep 20 GiB free
    CachePollInterval: 10 * time.Minute,
    CleanOnStart:      true,
})
```

Behavior:

- Active readers and active fetches are skipped.
- Oldest `AccessedAt` entries are evicted first for size/free-space pressure.
- Invalid metadata entries are removed when possible.
- `NoBackground: true` disables the janitor goroutine.

## Removing or renaming entries

Remove one key:

```go
if err := cache.Remove(key); err != nil {
    return err
}
```

Rename a cached key:

```go
if err := cache.RenameKey("tmp:path", "stable:path"); err != nil {
    return err
}
```

`RenameKey` is useful when a consumer first opens by temporary path and later discovers a stable content key. It does not move active readers.

## Recommended production options

```go
opts := varc.Options{
    CacheDir:          "/var/cache/myapp/varc",
    BlockSize:         1 << 20,        // 1 MiB
    ChunkSize:         32 << 20,       // 32 MiB
    ChunkStreams:      8,
    MaxInflightBytes:  512 << 20,      // 512 MiB
    ReadAhead:         8 << 20,        // 8 MiB
    CacheMaxSize:      250 << 30,      // 250 GiB
    CacheMinFreeSpace: 10 << 30,       // 10 GiB
    CachePollInterval: 5 * time.Minute,
    ReadRetryCount:    3,
    ReadRetryDelay:    200 * time.Millisecond,
    ShardLevel:        2,
    SyncWrites:        false,
}
```

Tuning notes:

| Option | Recommendation |
|---|---|
| `BlockSize` | 1 MiB is a good default. Smaller blocks improve partial-read latency but increase metadata churn. |
| `ChunkSize` | 16–128 MiB for media/range workloads. Use smaller chunks for slow or expensive backends. |
| `ChunkStreams` | Start with 4–8. Increase only if the backend and disk can handle it. |
| `MaxInflightBytes` | Keep below available memory. It limits concurrent block buffers. |
| `ReadAhead` | Useful for sequential media playback. Disable or reduce for purely random reads. |
| `SyncWrites` | Enable only when crash consistency is more important than throughput. |
| `VerifyChecksum` | Enable for paranoid local-disk verification, not for maximum throughput. |
| `ShardLevel` | Use `2` for long-running caches with many objects. |

## Error handling

Use `errors.Is` with exported sentinel errors:

```go
r, err := cache.Open(ctx, key, -1, nil)
if err != nil {
    switch {
    case errors.Is(err, varc.ErrCacheMiss):
        // Not cached locally.
    case errors.Is(err, varc.ErrClosed):
        // Cache or reader is closed.
    case errors.Is(err, varc.ErrCorruptMeta):
        // Metadata is unreadable or invalid.
    default:
        // Source/disk/context error.
    }
    return err
}
_ = r
```

Common errors:

| Error | Meaning |
|---|---|
| `ErrCacheMiss` | Cache-only open/read cannot satisfy the requested data. |
| `ErrSourceRequired` | A missing range needs a source, but none exists. |
| `ErrInvalidRange` | Negative or malformed range. |
| `ErrCorruptMeta` | Metadata could not be decoded or failed validation. |
| `ErrClosed` | Cache or reader has been closed. |

## Concurrency model

- Multiple readers can open the same key.
- Concurrent reads for overlapping missing ranges are coalesced.
- A global semaphore limits active download tasks.
- `MaxInflightBytes` limits source-read buffers.
- `Reader` methods are safe for ordinary concurrent use, but for high-throughput code prefer one reader per request/stream.
- `Close` cancels active downloads owned by that reader.
- `Cache.Close` cancels background work and active cache operations.

## Caching integration pattern

A typical cache consumer does this:

1. Map a remote path/inode to a stable `key`.
2. Fetch object metadata from the backend: size, fingerprint, modtime, content type.
3. Build an `io.ReaderAt` for the backend.
4. Open through `varc`.
5. Serve random reads through `ReadAtContext`.
6. Use `WarmRange` for headers/indexes if needed.
7. Use `Prune`/background janitor for cache limits.

Example:

```go
func OpenCachedFile(ctx context.Context, cache *varc.Cache, obj RemoteObject) (*varc.Reader, error) {
    src := obj.NewReaderAt()

    return cache.Open(ctx, obj.StableKey(), obj.Size(), src,
        varc.WithFingerprint(obj.Generation()),
        varc.WithModTime(obj.ModTime()),
        varc.WithAttr("remote-path", obj.Path()),
        varc.WithAttr("content-type", obj.ContentType()),
    )
}
```

## Testing

Run the included tests:

```bash
go test ./...
```

Run with the race detector:

```bash
go test -race ./...
```

Run selected tests:

```bash
go test -run TestReadThroughAndCacheOnly ./...
go test -run TestConcurrentReadersCoalesceDownloads ./...
go test -run TestChecksumVerificationDetectsCorruption ./...
```

## Operational checklist

Before deploying:

- Use stable fingerprints. Do not rely only on size if content can change.
- Put `CacheDir` on a filesystem with enough space and good random I/O.
- Set `CacheMaxSize` and `CacheMinFreeSpace`.
- Decide whether you need `SyncWrites` or maximum throughput.
- Use request contexts for all user-facing reads.
- Export `Metrics` to logs or monitoring.
- Run `Verify` periodically if local disk corruption matters.
- Run `go test -race` after changing cache internals.

## Limitations

- Sources must provide `io.ReaderAt`; sequential-only streams need an adapter.
- Object size must be known when a source is provided.
- The cache assumes immutable content for a given fingerprint.
- It is a range cache, not a general mutable filesystem.
- Checksum verification validates local cached blocks, not remote authenticity unless your source/fingerprint also guarantees authenticity.

## Production operations

### Range planning without upstream access

Use `Cache.Plan` before constructing an expensive backend client. It returns cached segments, missing segments, local coverage, and pin/completion state. This is the safest path when you want to avoid initializing a remote client if the requested range is already local.

```go
plan, err := cache.Plan(ctx, key, start, end, varc.WithFingerprint(etag))
if err == nil && !plan.NeedFetch() {
    r, err := cache.Open(ctx, key, -1, nil)
    // serve from local cache only
    _ = r
    _ = err
}
```

`Reader.PlanRange` provides the same planning from an already-open reader. Both APIs are metadata/data-file checks only; they never call `ReadAt` on the source.

### Pinning hot objects

`Cache.Pin(ctx, key)` stores a persistent metadata marker. `Prune` skips pinned entries for age, size, and free-space eviction. `Cache.Remove(key)` remains explicit and still deletes pinned entries.

```go
_ = cache.Pin(ctx, key)
pinned, _ := cache.IsPinned(key)
_ = pinned
_ = cache.Unpin(ctx, key)
```

### Batch warming

`WarmBatch` warms multiple objects with bounded concurrency. Each job uses the normal `Open` path, so fingerprint invalidation, retries, coalescing, block commits, and checksum metadata all remain consistent.

```go
results, err := cache.WarmBatch(ctx, []varc.WarmJob{{
    Key: key,
    Size: size,
    Source: src,
    Ranges: []varc.Range{{Start: 0, End: 4 << 20}},
    OpenOptions: []varc.OpenOption{varc.WithFingerprint(etag)},
}}, varc.WarmOptions{Concurrency: 4, Class: "startup", StopOnError: true})
_ = results
_ = err
```

### Manifest export/import

`ExportManifest` writes a compact JSON snapshot of metadata sidecars. `ImportManifest` restores them. This is useful for cache moves, pre-seeding, or recovery after accidental metadata deletion. It does not copy data bytes.

```go
_ = cache.ExportManifest(ctx, w)
stats, err := cache.ImportManifest(ctx, r, varc.ImportOptions{
    Overwrite:        false,
    RequireDataFiles: false,
    MarkImported:     true,
})
_ = stats
_ = err
```

### Repair and health

`Health` is a cheap admin snapshot. `Repair` scans sidecars and data files, optionally removing corrupt/missing metadata and normalizing damaged range/checksum records.

```go
health := cache.Health(ctx)
_ = health

stats, err := cache.Repair(ctx, varc.RepairOptions{
    RemoveCorruptMeta: true,
    RemoveMissingData: true,
    DropBadRanges:     true,
    DropBadChecksums:  true,
    TouchRepaired:     true,
})
_ = stats
_ = err
```


## Caddy handler hardening controls

The Caddy handler now includes production-facing controls around the core range cache:

- `strip_query`, `sort_query`, and `lowercase_host` canonicalize upstream URLs/default keys.
- `vary_header` appends selected request header values to the cache key.
- `bypass_header`, `bypass_cookie`, and `bypass_query` stream from origin without caching.
- `Authorization` bypasses by default. Enable `cache_authorization on` only when the key includes the authorization scope or the bytes are identical for every user.
- `Set-Cookie`, `Cache-Control: private`, and `Cache-Control: no-store` bypass by default. Enable `cache_set_cookie`, `cache_private`, or `cache_no_store` only for trusted origins.
- `stale_if_error` serves an already-cached requested range when origin probing/opening fails.
- The admin endpoint supports status, Prometheus metrics, object plans, prune, purge, pin, unpin, repair, and warm.

Admin examples:

```bash
curl http://localhost:8080/_varc
curl http://localhost:8080/_varc/metrics
curl 'http://localhost:8080/_varc/object?key=https://origin.example.com/video.mp4'
curl -X POST 'http://localhost:8080/_varc?action=purge&key=https://origin.example.com/video.mp4'
curl -X POST 'http://localhost:8080/_varc?action=warm&url=https://origin.example.com/video.mp4&range=0-8388607'
```
