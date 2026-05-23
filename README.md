# varc — Range-Caching HTTP Proxy

A high-performance HTTP reverse proxy with native Range request caching. Built from the ground up for streaming media, it downloads content from upstream in parallel chunks and caches everything on disk — including byte ranges.

## Features

- **Native Range Caching**: Unlike Varnish (which passthroughs Range requests), varc caches byte ranges on disk and serves them from cache. Concurrent range requests are coalesced into a single upstream fetch.
- **Parallel Chunked Downloading**: Downloads files in parallel streams for maximum throughput on high-latency connections.
- **Disk-Backed Cache**: Sparse file support, hash-verified metadata, configurable max age/size with background eviction.
- **Cache Purge**: Send `PURGE` requests to evict specific URLs from cache immediately.
- **Stale-Serve on Error**: When upstream is unreachable, varc serves stale cached content instead of returning 5xx.
- **Conditional Requests**: `If-Modified-Since` and `If-None-Match` are respected — returns 304 when content hasn't changed.
- **Smart Passthrough**: POST, PUT, PATCH, DELETE, and requests with Authorization or Cookie headers bypass the cache and proxy directly upstream.
- **Built-in Metrics**: Track hit/miss ratio, bytes served, purge count, and cache size via a live JSON endpoint.
- **Multiple Access Modes**: Standalone HTTP server, Caddy module, or Go library.
- **Flexible Cache Keys**: Optional query parameter stripping, domain stripping, hash sharding.
- **Caddy Ready**: Native Caddy module for easy integration.
- **Docker Ready**: Minimal Alpine-based Docker image.

## Getting Started

### Prerequisites

- Docker or Go 1.25+

### Installation & Run

You can run `varc` directly using Docker:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/host/cache:/tmp/varc_cache \
  ghcr.io/tgdrive/varc --cache-dir /tmp/varc_cache
```

### Usage

```
# Stream a file via query parameter
curl "http://localhost:8080/stream?url=https://example.com/video.mp4"

# Or Base64-encoded path
curl "http://localhost:8080/stream/aHR0cHM6Ly9leGFtcGxlLmNvbS92aWRlby5tcDQ"

# Range requests are cached natively
curl -H "Range: bytes=0-999999" "http://localhost:8080/stream?url=https://example.com/video.mp4"
```

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--port` | `8080` | Port to listen on |
| `--cache-dir` | `$TMPDIR/varc_cache` | Cache directory on local disk |
| `--chunk-size` | `128M` | Chunk size for parallel downloads; accepts suffixes (K, M, G, T) |
| `--chunk-streams` | `2` | Number of parallel download streams |
| `--max-age` | `1h` | Maximum cache age (Go duration format) |
| `--max-size` | _unlimited_ | Maximum cache size (e.g., `10G`); disables eviction when unset |
| `--strip-query` | `false` | Strip query params from URL before hashing cache key |
| `--strip-domain` | `false` | Strip domain from URL before hashing cache key (shared cache for any origin) |
| `--shard-level` | `1` | Hash shard depth for cache paths (0 = flat, 3 = `ab/cd/ef/hash`) |

## Caddy Module

```caddyfile
# Static upstream (proxy prefix)
example.com {
    varc https://upstream.example.com {
        cache_dir      /data/cache
        chunk_size     4M
        chunk_streams  4
        max_age        24h
        max_size       50G
        strip_query
        shard_level    3
        passthrough
    }

    reverse_proxy localhost:8080
}
```

```caddyfile
# Dynamic upstream (resolve from request query/base64 path — no upstream prefix needed)
example.com {
    varc {
        cache_dir     /data/cache
        metrics       /varc/stats
    }

    reverse_proxy localhost:8080
}
```

### Caddyfile Subdirectives

| Subdirective | Default | Description |
|---|---|---|
| _positional_ | `""` | Upstream URL (optional, first argument; can also use `upstream` subdirective) |
| `upstream` | `""` | Upstream URL via named subdirective (alternative to positional arg) |
| `passthrough` | `false` | Enable cache bypass (POST/auth/cookie) + call next handler on cache miss |
| `metrics` | `""` | Path to serve JSON metrics (e.g., `/varc/stats`) |
| `cache_dir` | `$TMPDIR/varc_cache` | Cache directory on local disk |
| `chunk_size` | `128M` | Chunk size for parallel downloads (accepts K, M, G, T suffixes) |
| `chunk_streams` | `2` | Number of parallel download streams |
| `max_age` | `1h` | Maximum cache age (Go duration: `24h`, `7d` not supported — use `168h`) |
| `max_size` | _unlimited_ | Maximum cache size (e.g., `10G`); oldest entries evicted first |
| `strip_query` | `false` | Boolean flag — omit value to enable |
| `strip_domain` | `false` | Boolean flag — omit value to enable |
| `shard_level` | `1` | Hash shard depth (0 = flat directory, N = N levels of 2-char subdirectories) |

### Dynamic Upstream Resolution

When no upstream is configured, varc resolves the target URL from the request:
- **Query param**: `?url=https://example.com/file` (same as standalone mode)
- **Base64 path**: `/stream/<base64-encoded-url>` (same as standalone mode)

This is useful for caching arbitrary URLs at a single entry point.

## Go Library

Import `github.com/tgdrive/varc` when your Go application needs a read-through
disk cache for byte-range-addressable remote content — without running the HTTP
proxy.

The API is **read-through only**. Varc writes fetched ranges to the local disk
cache, but it does not expose rclone-style writeback or upload semantics.

### Quick Start

```go
import "github.com/tgdrive/varc"

cache, err := varc.New(context.Background(), varc.Options{
    CacheDir: "/tmp/varc-cache",
})
if err != nil {
    log.Fatal(err)
}
defer cache.Close()

// Open a cache-backed reader for any io.ReaderAt source.
// On first access, only missing chunks are fetched from upstream.
reader, err := cache.Open(ctx, "my-cache-key", size, myReaderAt,
    varc.WithFingerprint(etag),
    varc.WithModTime(modTime),
)
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

// Reader implements io.Reader, io.ReaderAt, io.Seeker, io.Closer.
section := io.NewSectionReader(reader, rangeStart, rangeLength)
_, _ = io.Copy(responseWriter, section)
```

The returned `varc.Reader` also exposes `Size() int64` and `ModTime() time.Time`.

### Source Contract

Varc only needs `io.ReaderAt` from upstream sources:

```go
type ReaderAt interface {
    ReadAt(p []byte, off int64) (n int, err error)
}
```

When a player requests `bytes=1048576-1056767`, call `Open` once and serve that
range with `io.NewSectionReader`. Varc checks the local sparse cache and calls
your source's `ReadAt` only for missing chunks.

```go
type TelegramFile struct {
    // fields needed to fetch Telegram chunks
}

func (f *TelegramFile) ReadAt(p []byte, off int64) (int, error) {
    // Fetch exactly len(p) bytes starting at off from Telegram.
    // Return io.EOF only for short reads at the end of the file.
    return fetchTelegramRange(p, off)
}
```

### Cache Validation

Pass validation metadata as options:

```go
reader, err := cache.Open(ctx, key, size, src,
    varc.WithFingerprint(etagOrContentHash),
    varc.WithModTime(updatedAt),
)
```

Rules:

- Same fingerprint: cached ranges are reused.
- Changed fingerprint: cached data and metadata are invalidated.
- No fingerprint, same size: cached ranges are reused because varc cannot prove content changed.
- No fingerprint, changed size: cached data is invalidated.

`WithModTime` preserves the upstream modification time on `Reader.ModTime()`.

### Playback / Seeking Behavior

For a media player:

```text
read 0-8191          -> source ReadAt for missing chunk(s), writes cache
read 0-8191 again    -> disk cache hit, no source call
seek to 1 MiB        -> source ReadAt for missing chunk(s) near 1 MiB
seek back to 0       -> disk cache hit, no source call
```

Use standard library section readers for HTTP ranges:

```go
cached, err := cache.Open(ctx, key, size, source, varc.WithFingerprint(etag))
section := io.NewSectionReader(cached, start, length)
_, err = io.Copy(w, section)
```

### Cache Key Management

Cache keys are hashed with MD5 and optionally sharded into subdirectories for
filesystem scalability.

```go
// ShardKey hashes key and applies directory sharding.
// level=1: "ab/abcdef..."
// level=2: "ab/cd/abcdef..."
// level=3: "ab/cd/ef/abcdef..."
cachePath := varc.ShardKey("https://example.com/video.mp4", 2)

// When Options.ShardLevel > 0, Open automatically shards the key.
cache, _ := varc.New(ctx, varc.Options{ShardLevel: 2})
reader, _ := cache.Open(ctx, "https://example.com/video.mp4", size, source)
//                         → cache at "ab/cd/abcdef..."
```

### Cache Management

```go
// Shut down the cache — purges, stops background cleaner, cancels context.
// Always call when the application exits.
defer cache.Close()
// Close() is safe to call multiple times — context.CancelFunc is
// idempotent, os.RemoveAll returns nil for already-deleted paths,
// and the inUse atomic store is always safe.

// Check if a key exists in cache.
exists := cache.Exists("my-key")

// Remove a single entry from cache.
err := cache.Remove("my-key")

// Remove multiple entries.
cache.Remove("key-a")
cache.Remove("key-b")

// Cache statistics.
stats := cache.Stats()
// map[string]interface{}{
//   "files":           24,
//   "bytesUsed":       1048576,
//   "erroredFiles":    0,
// }
```

### Options Reference

```go
type Options struct {
    CacheDir          string        // Disk cache root (default: $TMPDIR/varc_cache)
    ChunkSize         int64         // Chunk size for parallel downloads (default: 128M)
    ChunkSizeLimit    int64         // Max chunk size; -1 = unlimited (default: -1)
    ChunkStreams      int           // Parallel download streams (default: 2)
    CacheMaxAge       time.Duration // Max cache entry age (default: 1h)
    CacheMaxSize      int64         // Max total cache size; -1 = unlimited (default: -1)
    CacheMinFreeSpace int64         // Min free disk space; -1 = disabled (default: -1)
    CachePollInterval time.Duration // Background cleanup interval (default: 1m)
    ReadAhead         int64         // Read-ahead bytes beyond requested offset (default: 0)
    FastFingerprint   bool          // Use fast fingerprint mode (default: false)
    HandleCaching     time.Duration // Handle reuse window (default: 5s)
    ShardLevel        int           // Hash shard depth; 0 = flat (default: 0)
    Logger            Logger        // Structured logger; nil = no logging
}
```

Zero-valued options are filled from `varc.DefaultOptions()`. Call it to inspect
production-safe defaults:

```go
fmt.Println(varc.DefaultOptions().ChunkSize) // 134217728 (128 MiB)
```

### HTTP Handler Adapter

For an HTTP handler that wraps an upstream URL with varc caching, import
`github.com/tgdrive/varc/httpcache`:

```go
import "github.com/tgdrive/varc/httpcache"

handler, err := httpcache.NewHandler(httpcache.Options{
    CacheDir: "/tmp/varc-cache",
})
if err != nil {
    log.Fatal(err)
}
http.Handle("/", handler)
```

The handler resolves the upstream URL from `?url=` query parameter or a
base64-encoded path (same routing as the standalone proxy).

### Building with xcaddy

Build a custom Caddy binary with the varc module baked in using [xcaddy](https://github.com/caddyserver/xcaddy):

```bash
# Install xcaddy
go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest

# Build Caddy + varc
xcaddy build --with github.com/tgdrive/varc/caddy

# Verify the module is registered
./caddy list-modules | grep varc
# → http.handlers.varc
```

By default xcaddy pulls the latest tagged release. To pin a specific version:

```bash
xcaddy build --with github.com/tgdrive/varc/caddy@v1.2.3
```

To work with a local checkout during development:

```bash
xcaddy build --with github.com/tgdrive/varc/caddy=/absolute/path/to/varc
```

### Docker + xcaddy

For production, build a custom Caddy image with varc:

```dockerfile
FROM caddy:builder AS builder

RUN xcaddy build \
    --with github.com/tgdrive/varc/caddy=github.com/tgdrive/varc

FROM caddy:latest
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
```

Or use a single-stage build:

```dockerfile
FROM caddy:builder
RUN xcaddy build \
    --with github.com/tgdrive/varc/caddy=github.com/tgdrive/varc
```

### Caddyfile Example

Once built, use the `varc` directive in your Caddyfile:

```caddyfile
example.com {
    varc https://upstream.example.com {
        cache_dir      /data/caddy/cache/varc
        chunk_size     4M
        chunk_streams  4
        max_age        24h
        max_size       50G
        strip_query
        passthrough
        metrics        /varc/stats
    }

    reverse_proxy localhost:8080
}
```

## Architecture

1. **Request arrives** → proxy resolves the upstream URL via query param or base64 path.
2. **Passthrough check** → POST/PUT/PATCH/DELETE and requests with `Authorization` or `Cookie` headers skip the cache and are proxied directly to upstream.
3. **Cache check** → if the file is already cached on disk and not stale, serve directly from cache (with `ETag` and `Last-Modified` for conditional validation).
4. **Conditional validation** → `If-Modified-Since` and `If-None-Match` are checked against the cached entry; returns 304 if content is unchanged.
5. **Cache miss** → file is downloaded from upstream in parallel chunks using Range requests, written to disk cache, and streamed to the client.
6. **Range requests** → if the requested range is partially cached, only the missing bytes are fetched from upstream. Fully cached ranges are served without touching the upstream.
7. **Error fallback** → if the upstream fetch fails and stale data exists in cache, the stale data is served with an `X-Cache: STALE` header.
8. **Cache cleanup** → background cleaner evicts expired or oversized entries.

## Operations

### Cache Purge

Evict a specific URL from the cache immediately by sending a `PURGE` request:

```bash
curl -X PURGE "http://localhost:8080/stream?url=https://example.com/video.mp4"
```

This removes both the cached file and the internal URL mapping. The next request for the same URL will be a full cache miss.

### Metrics

Varc exposes cache performance metrics as a JSON snapshot via the Go library:

```go
stats := handler.Metrics().Snapshot()
// map[string]int64{
//   "requests":            42,
//   "hits":                30,
//   "misses":              12,
//   "bytes_served":        1048576,
//   "bytes_from_upstream": 2097152,
//   "purges":              1,
// }
```

Cache engine stats (items count, bytes used, cache root, metadata root) are merged into the same snapshot.

In the Caddy module, configure a metrics endpoint with the `metrics` subdirective:

```caddyfile
varc https://upstream.example.com {
    metrics /varc/stats
}
```

Then `curl http://localhost:8080/varc/stats` returns the same JSON snapshot.

### Stale-Serve

When the upstream server is unreachable or returns an error, varc automatically serves any cached data it has for the requested URL. Responses served from stale cache include an `X-Cache: STALE` header so clients can distinguish stale from fresh.

### Access Logging

If a `Logger` is configured, each request is logged with:

```
[proxy] GET /stream?url=https://example.com/video.mp4 200 1048576 1.234s
```

Format: `[proxy] METHOD URL STATUS SIZE DURATION`

In standalone mode, this uses `zap` structured logging (production quality).

## Performance

- Parallel chunked download with configurable stream count
- Sparse file support — no wasted disk for uncached ranges
- Concurrent readers don't block each other
- Metadata is persisted as JSON alongside cached data

## Development

```bash
# Build
go build -o varc ./cmd/varc

# Run tests
go test ./...

# Cross-compile
GOOS=linux GOARCH=amd64 go build -o varc-linux-amd64 ./cmd/varc
