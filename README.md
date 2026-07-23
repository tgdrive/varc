# varc

`varc` is a sparse read-through cache for remote byte ranges.

It can be used as:

- a Caddy HTTP handler for remote media and large files;
- a Go library over any `io.ReaderAt` or `RangeSource`.

Cached ranges are served from disk. Missing ranges are fetched from the source and written into a sparse file.

## How it works

- One logical chunk maps to one source range request.
- Readers can consume bytes while a chunk is still downloading.
- Concurrent readers share the same active download.
- Blocking reads and seeks run before queued preload work.
- Only one source transfer is active per cached object; independent objects download concurrently.
- Closing the final reader cancels unused active and queued work.
- Interrupted downloads keep committed bytes and resume from the first missing offset.

## Chunk settings

```caddyfile
chunk_size 32MiB
chunk_size_limit 128MiB
preload_chunks 1
```

`chunk_size` is the starting request size.

`chunk_size_limit` is the largest request size used after sustained sequential reads.

`preload_chunks` is the number of future chunks queued behind the current blocking read.

Sequential readers grow like this:

```text
32 MiB -> 64 MiB -> 128 MiB
```

A seek or random read resets the reader to `chunk_size`.

For fixed 128 MiB chunks:

```caddyfile
chunk_size 128MiB
chunk_size_limit 128MiB
preload_chunks 1
```

## Build with xcaddy

From the repository root:

```bash
xcaddy build \
  --with github.com/tgdrive/varc=.
```

Build directly from GitHub:

```bash
xcaddy build \
  --with github.com/tgdrive/varc@main
```

Verify the module:

```bash
./caddy list-modules | grep varc
```

Expected output:

```text
http.handlers.varc
```

## Caddyfile example

```caddyfile
:8080 {
    route /media/* {
        varc https://origin.example.com {
            cache_dir /var/cache/caddy/varc
            key {host}:{uri}

            append_uri on

            chunk_size 128MiB
            chunk_size_limit 128MiB
            preload_chunks 1

            max_size 500GiB
            min_free_space 20GiB
            max_age 168h
            poll_interval 1m

            probe_timeout 15s
            revalidate_interval 5m
            dial_timeout 10s
            response_header_timeout 30s

            debug_headers on
            admin_path /_varc
        }
    }
}
```

Validate it:

```bash
./caddy adapt \
  --config examples/Caddyfile \
  --adapter caddyfile \
  --pretty
```

Run it:

```bash
./caddy run \
  --config examples/Caddyfile \
  --adapter caddyfile
```

## JSON example

```json
{
  "handler": "varc",
  "upstream": "https://origin.example.com",
  "cache_dir": "/var/cache/caddy/varc",
  "key": "{http.request.host}:{http.request.uri}",
  "chunk_size": 134217728,
  "chunk_size_limit": 134217728,
  "preload_chunks": 1,
  "cache_max_size": 536870912000,
  "debug_headers": true
}
```

## Cache-only mode

```caddyfile
varc https://origin.example.com {
    cache_dir /var/cache/caddy/varc
    cache_only on
    pass_thru off
}
```

A cache miss returns an error unless `pass_thru on` is enabled.

## Origin requirements

The HTTP origin must support single byte ranges.

`varc` expects:

- `206 Partial Content`;
- a valid `Content-Range` matching the requested range;
- a stable object size;
- identity encoding.

`varc` rejects malformed ranges, wrong totals, wrong content lengths, unexpected `200` responses, and truncated bodies.

Streaming range bodies do not use a fixed total timeout. `timeout` is a read-idle timeout that resets whenever origin bytes arrive. Dial, TLS, probe, and response-header timeouts still apply.

Cached objects are treated as immutable by default. Set `revalidate_interval` to periodically probe mutable origins and invalidate cached ranges when their ETag, modification time, or size changes. `stale_if_error` may serve the last successfully validated copy only for its configured duration.

## Authentication and private responses

Requests with `Authorization` bypass the cache by default.

Responses with any of these also bypass by default:

- `Set-Cookie`;
- `Cache-Control: private`;
- `Cache-Control: no-store`.

Keep those defaults unless the cache key separates users or every user receives identical bytes.

Static origin credentials:

```caddyfile
header Authorization "Bearer {$ORIGIN_TOKEN}"
```

Forward a request header:

```caddyfile
forward_header Authorization
```

## Debug headers

With `debug_headers on`, responses can include:

- `X-Varc-Cache`
- `X-Varc-Key`
- `X-Varc-Source`
- `X-Varc-Range`
- `X-Varc-Bypass`

Do not expose them publicly if keys or source URLs contain sensitive data.

## Admin endpoint

Configure:

```caddyfile
admin_path /_varc
```

The endpoint is loopback-only by default.

Useful calls:

```bash
curl http://localhost:8080/_varc
curl http://localhost:8080/_varc/metrics
curl -X POST 'http://localhost:8080/_varc?action=prune'
curl -X POST 'http://localhost:8080/_varc?action=purge&key=movie.mkv'
curl -X POST 'http://localhost:8080/_varc?action=pin&key=movie.mkv'
curl -X POST 'http://localhost:8080/_varc?action=unpin&key=movie.mkv'
curl -X POST 'http://localhost:8080/_varc?action=repair&dry_run=true'
```

For remote access, set `admin_token` and put the endpoint behind proper authentication.

## Go library

```go
package main

import (
    "bytes"
    "context"
    "log"

    "github.com/tgdrive/varc/varc"
)

func main() {
    ctx := context.Background()
    source := bytes.NewReader([]byte("example remote data"))

    cache, err := varc.New(ctx, varc.Options{
        CacheDir:       "./.cache/varc",
        ChunkSize:      32 << 20,
        ChunkSizeLimit: 128 << 20,
        PreloadChunks:  1,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    reader, err := cache.Open(ctx, "object", source.Size(), source)
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    buf := make([]byte, 7)
    if _, err := reader.ReadAt(buf, 0); err != nil {
        log.Fatal(err)
    }

    log.Printf("read: %q", buf)
}
```

See [API_USAGE.md](API_USAGE.md) for the full API.

## Existing cache data

You do not need to delete the old cache after upgrading.

Existing data is reused when the cache key, object size, and fingerprint still match.

Remove the cache only when:

- cache keys changed;
- the origin data is stale;
- metadata is corrupt;
- a clean re-download is required.

## Operations

The core package supports:

- range planning;
- pin and unpin;
- batch warming;
- manifest import and export;
- metadata repair;
- optional checksums;
- health and metrics snapshots;
- age, size, and free-space pruning.

## Failure behavior

- Short or broken streams retry from the first uncommitted offset.
- Reader and cache cancellation propagate to the source context.
- The last reader cancels orphaned downloads.
- Failed cache writes do not claim durable coverage.
- After restart, only committed ranges are trusted.
- Changed size or fingerprint invalidates incompatible cached data.

## Filesystem notes

Use one cache owner per directory when possible.

`varc` does not provide distributed locking between separate processes or machines.

A local filesystem with sparse-file support is recommended. NFS, SMB, and FUSE may behave differently around sparse allocation, write ordering, locking, and rename operations.

Enable `sync_writes` only when stronger crash durability is worth the performance cost.

## Tests

Run everything:

```bash
go test -count=1 -timeout=180s ./...
```

Race detector:

```bash
go test -race -count=1 -timeout=240s ./...
```

Static checks:

```bash
go vet ./...
```

The test suite covers chunk growth, fixed chunks, seeks, preload priority, shared readers, truncated streams, retries, malformed HTTP responses, cancellation, restart, write failures, multi-terabyte sparse offsets, hundreds of readers, scheduler cleanup, and slow backing stores.

Real power loss, network filesystem behavior, disk exhaustion, and long production transfers still need testing in the deployment environment.
