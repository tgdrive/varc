# varc

`varc` is a read-only sparse object cache for Go HTTP servers and Caddy. It is designed for large objects that are served through HTTP byte ranges and should be cached locally without downloading the complete object first.

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
 range scheduler
       |
       +--> sequential adaptive chunk reader
       |
       +--> parallel chunk streams
```

The core `cache`, `source`, and `proxy` packages do not depend on Caddy. Caddy support is a separate nested module under `caddy/`, so ordinary library users do not inherit Caddy's dependency tree.

The internal engine is organized by its responsibilities rather than by compatibility layers:

```text
internal/engine/
  scheduler/    reusable range-fetch coordination
  chunkstream/  sequential and parallel chunk readers
  streambuf/    asynchronous origin-read buffering
  bufferpool/   reusable byte buffers and stream FIFO storage
  objectio/     narrow internal object/range contract
  sparsefile/   platform sparse-file support
  diskspace/    free-space queries
  ioerrors/     filesystem error classification
  mapbuffer/    platform memory-mapping support
  readutil/     small read helpers
```

## Read-cache behavior

The cache provides:

- Sparse local cache files with persisted cached-range metadata.
- Cache metadata reload and cached-byte accounting after process restart.
- Deterministic SHA-256 cache-directory sharding with configurable depth and startup migration when shard depth changes.
- Waiter coordination for concurrent and overlapping reads.
- Reuse of an existing range fetch for nearby sequential reads.
- A 5-second idle grace period for reusable fetchers.
- A 1 MiB reuse window and already-cached skip threshold.
- `ReadAhead` extension of requested ranges.
- Sequential chunk reading with chunk-size doubling up to `ChunkSizeLimit`.
- Parallel ranged chunk streams controlled by `ChunkStreams`.
- Asynchronous read buffering and pooled buffers.
- Handle-caching grace before range-fetch and file teardown.
- Max-age, max-size, and minimum-free-disk-space cleaning.
- ENOSPC-triggered cleaner wakeup, active-item range reset, and read retry coordination.
- Windows sparse-file marking; Unix files rely on normal sparse-file behavior.
- Source-version validation so cached bytes from different object versions are not intentionally mixed.

The project is intentionally read-only. It does not implement writes, upload/writeback behavior, directory listings, rename/remove operations, mount integration, ownership/permission emulation, backend discovery, transfer statistics, or bandwidth limiting.

The cache directory should be owned by one cache process at a time; multi-process cache-directory locking is not implemented.

## Cache options

`cache.DefaultOptions()` returns:

```text
CachePollInterval = 60s
CacheMaxAge       = 1h
CacheMaxSize      = -1        # unlimited
CacheMinFreeSpace = -1        # disabled
CacheShardDepth   = 1         # one 2-hex-character shard directory
ChunkSize         = 128 MiB
ChunkSizeLimit    = -1        # unlimited growth limit
ChunkStreams      = 0         # sequential reader
ReadAhead         = 0
BufferSize        = 16 MiB
HandleCaching     = 5s
LowLevelRetries   = 10
```

For parallel range fetching, set `ChunkStreams` above 1. For sequential adaptive fetching, leave `ChunkStreams` at 0 or 1 and configure `ChunkSize` / `ChunkSizeLimit` as needed.

`CacheShardDepth` controls the number of two-hex-character hash directories. For example, depth 2 stores an object under `data/ab/cd/<sha256>` and its metadata under the matching `meta/ab/cd/` path.

## Go usage

The module path is currently local (`varc`) until a public repository/import path is chosen.

```go
src := httpsource.New(http.DefaultClient, func(ctx context.Context, key string) (string, error) {
    return "https://origin.example/" + url.PathEscape(key), nil
})

opt := cache.DefaultOptions()
opt.ReadAhead = 8 << 20
opt.ChunkStreams = 3

c, err := cache.New(context.Background(), "/var/cache/varc", src, opt)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

handler := &proxy.Handler{Cache: c}
log.Fatal(http.ListenAndServe(":8080", handler))
```

Incoming HTTP headers such as `Authorization`, `Cookie`, `Host`, tenant headers, and custom headers are forwarded to the origin for metadata and range requests. By default, cache identity is the object key, so different credentials for the same key share the same cached file. Embedders can override only the cache identity with `proxy.Handler.CacheKey` or call `Cache.OpenWithCacheKey` directly without changing the source object key.

The cache owns representation-control headers. Client `Range`, `If-Range`, `If-Match`, `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` values are removed from internal metadata requests, and cache-generated `Range` / `If-Range` values are used for range fetches. Static headers configured on `source/http.Source` override same-named incoming headers.

## Caddy module

The optional Caddy module registers:

```text
http.handlers.varc
```

and the Caddyfile directive:

```caddyfile
varc https://origin.example/files {
    cache_dir /var/cache/varc
    key {host}:{uri}
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

The directive is ordered before `reverse_proxy`. GET and HEAD requests are served terminally through the cache; misses produce ranged reads against the configured upstream. If `key` is omitted, the cache key is the cleaned request path without its leading slash, exactly as before; host, query, and headers do not vary the default key. A configured `key` is expanded per request and changes cache identity only—the origin object path remains the normal cleaned request path. `{host}` and `{uri}` are convenience aliases for Caddy's `{http.request.host}` and `{http.request.uri}` placeholders; `{uri}` includes the query string. Full Caddy request placeholders such as `{http.request.header.X-Tenant}` are also supported. Incoming ordinary request headers are still forwarded upstream. Other HTTP methods continue to the next Caddy handler.

The Caddy adapter provisions one cache instance for a config load and closes it during Caddy cleanup/reload.

When the module has a public import path, a custom Caddy build can include the nested module with `xcaddy build --with <module-path>/caddy`.

## Validation

The test suite covers persistence, version invalidation, concurrent fetch reuse, sequential adaptive chunk growth, parallel streams, seek/close behavior, quota reset, minimum-free-space eviction, HTTP range handling, request-header forwarding, sharding migration, and Caddy integration.

Run from the repository root:

```text
go test ./...
go test -race ./...
go vet ./...
```

Run the same commands from `caddy/` for the optional Caddy module.

## Third-party notice

Parts of the range/cache/read-scheduling implementation retain an upstream MIT permission notice. Keep `THIRD_PARTY_NOTICES.md` when distributing substantial copied portions.
