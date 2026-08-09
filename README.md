# vfs-cache

A read-only sparse object cache for Go HTTP servers and Caddy.

The read/cache path preserves the proven VFS sparse-cache behavior while the public source boundary stays backend-neutral and reusable.

## Architecture

```text
net/http or Caddy
       |
       v
  proxy.Handler
       |
       v
   cache.Cache ------> source.Source
       |                    |
       |                    v
       |               HTTP source
       v
 sparse cache file + persisted range metadata
       |
       v
 downloader state machine
       |
       +--> sequential adaptive chunk reader
       |
       +--> parallel chunk streams
```

The core `cache`, `source`, and `proxy` packages do not depend on Caddy. Caddy support is a separate nested module under `caddy/`, so normal Go-library users do not inherit Caddy's dependency tree.

## VFS read-path behavior retained

The standalone cache keeps the read-only behavior that matters for ranged object serving:

- Sparse local cache files and persisted cached-range metadata.
- Cache metadata reload and cached-byte usage reconstruction on process restart.
- Deterministic SHA-256 cache-directory sharding with configurable depth and startup migration when depth changes.
- Downloader waiter coordination for concurrent and overlapping reads.
- Reuse of an existing downloader for nearby sequential reads.
- Downloader idle grace period of 5 seconds.
- Minimum 1 MiB downloader reuse window and 1 MiB already-cached skip threshold.
- `ReadAhead` extension of the downloader target range.
- Sequential chunked reading with chunk-size doubling up to `ChunkSizeLimit`.
- Parallel ranged chunk streams controlled by `ChunkStreams`.
- Asynchronous read buffering and pooled buffers used by the downloader path.
- Handle-caching grace period before downloader/file teardown.
- Max-age, max-size, and minimum-free-disk-space cache cleaning.
- ENOSPC-triggered cleaner wakeup, open-item cache reset, and read retry coordination.
- Windows sparse-file marking; Unix sparse files use the normal filesystem behavior.
- Source-version validation so bytes from different object versions are never intentionally mixed.

The downloader/chunk-reader scheduling is kept structurally aligned with the proven VFS implementation. Internal compatibility packages under `internal/cachecore/` provide the narrow object and buffer plumbing used by the cache engine.

## Intentionally excluded

This project is a read-only object cache, not a mounted VFS. It intentionally does not include:

- writes or writeback uploads;
- directory cache/listing behavior;
- rename/remove/write VFS operations;
- FUSE/mount integration;
- ownership, permissions, or symlink handling;
- backend/remote discovery;
- RC/status integrations, transfer statistics, or bandwidth limiting.

The cache directory should be owned by one cache process at a time; multi-process cache-directory locking is not implemented.

## Cache options

`cache.DefaultOptions()` currently returns:

```text
CachePollInterval = 60s
CacheMaxAge       = 1h
CacheMaxSize      = -1        # unlimited
CacheMinFreeSpace = -1        # disabled
CacheShardDepth    = 1         # one 2-hex-character shard directory
ChunkSize         = 128 MiB
ChunkSizeLimit    = -1        # unlimited growth limit
ChunkStreams      = 0         # sequential reader
ReadAhead         = 0
BufferSize        = 16 MiB
HandleCaching     = 5s
LowLevelRetries   = 10
```

For parallel range fetching, set `ChunkStreams` above 1. For sequential adaptive fetching, leave `ChunkStreams` at 0 or 1 and configure `ChunkSize` / `ChunkSizeLimit` as needed.

## Go usage

The module path is currently local (`vfs-cache`) until a public repository/import path is chosen.

```go
src := httpsource.New(http.DefaultClient, func(ctx context.Context, key string) (string, error) {
    return "https://origin.example/" + url.PathEscape(key), nil
})

opt := cache.DefaultOptions()
opt.ReadAhead = 8 << 20
opt.ChunkStreams = 3

c, err := cache.New(context.Background(), "/var/cache/vfs-cache", src, opt)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

handler := &proxy.Handler{Cache: c}
log.Fatal(http.ListenAndServe(":8080", handler))
```

Incoming HTTP request headers such as Authorization, Cookie, tenant, and custom headers are forwarded to the origin for metadata and range requests. Cache identity remains based only on the object key, so different credentials for the same key share the same cached file. The cache owns representation-control headers: client `Range`, `If-Range`, `If-Match`, `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` are stripped from internal metadata requests, and cache-generated `Range` / `If-Range` values are used for range fetches. Static headers configured on `source/http.Source` override same-named incoming headers.

## Caddy module

The optional Caddy module registers:

```text
http.handlers.vfs_cache
```

and the Caddyfile directive:

```caddyfile
vfs_cache https://origin.example/files {
    cache_dir /var/cache/vfs-cache
    max_size 100GiB
    min_free_space 5GiB
    shard_depth 2
    max_age 24h
    poll_interval 1m

    chunk_size 128MiB
    chunk_size_limit 1GiB
    chunk_streams 3
    read_ahead 16MiB
    buffer_size 16MiB

    handle_caching 5s
    retries 10
    header_up Authorization "Bearer token"
}
```

The directive is ordered before `reverse_proxy`. GET and HEAD requests are served terminally through the cache; cache misses are ranged reads from the configured `upstream`. Incoming request headers are forwarded upstream, while cache identity stays object-key-only. Other HTTP methods continue to the next Caddy handler.

The Caddy adapter provisions one cache instance for a config load and closes it during Caddy cleanup/reload.

When the module has a public import path, a custom Caddy build can include the nested module with `xcaddy build --with <module-path>/caddy`.

## Validation

The test suite covers cache persistence, range invalidation, concurrent downloader reuse, sequential adaptive chunk growth, parallel streams, seek/close behavior, quota reset, minimum-free-space eviction, HTTP range handling, and Caddy integration. Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

Run the same commands from `caddy/` for the optional Caddy module.

## Third-party notice

Parts of the range/cache/downloader implementation retain an upstream MIT permission notice. Keep `THIRD_PARTY_NOTICES.md` when distributing substantial copied portions.
