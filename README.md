# varc

`varc` is a sparse, read-through byte-range cache for Go, with a Caddy v2 HTTP handler for serving remote objects efficiently.

It is designed for media servers, object gateways, large-file delivery, and any workload where clients repeatedly read small or scattered ranges from a slower source.

The cache is **range-addressed**, **progressively readable**, and **cache-first**:

1. A request is mapped to a stable cache key.
2. Existing metadata is checked locally.
3. Cached bytes are served directly from the sparse cache file.
4. Only missing ranges open the source.
5. Overlapping readers share the same in-flight download.
6. Bytes become readable while the source stream is still active.

A fully cached range does not create an upstream request.

## Why varc

Generic filesystem and object caches usually expose low-level controls such as stream counts, raw read-ahead bytes, and global in-flight limits. Those controls are difficult to tune for media workloads and often make seeks compete with speculative downloads.

`varc` exposes three cache-window settings instead:

```json
{
  "chunk_size": 33554432,
  "chunk_size_limit": 134217728,
  "preload_chunks": 1
}
```

- `chunk_size` is the initial fetch window for a reader.
- `chunk_size_limit` is the largest window reached during stable sequential access.
- `preload_chunks` is the number of future windows queued behind blocking reads.

The scheduler derives concurrency, priorities, and in-flight limits internally.

## Core behavior

### Sparse range cache

Each object is represented by:

- a sparse data file containing cached bytes;
- a metadata sidecar containing object identity, size, cached ranges, attributes, and optional checksums.

Cached coverage is tracked as byte ranges, not permanent numbered chunks. This allows readers to use different adaptive window sizes without invalidating previously cached data.

### Progressive visibility

A source request does not need to finish before readers can consume data.

Writes begin with small buffers and grow from 4 KiB up to 1 MiB. Every successfully written range becomes immediately readable by waiting readers. After the source request completes, the range is committed to durable metadata.

This provides lower perceived startup latency while preserving restart-safe coverage metadata.

### Adaptive sequential windows

Each reader owns independent access-pattern state.

With the default configuration:

```text
32 MiB -> 64 MiB -> 128 MiB
```

- The first read uses `chunk_size`.
- Sustained sequential reads grow the window.
- Growth stops at `chunk_size_limit`.
- A seek, backward read, or unrelated `ReadAt` resets the reader to `chunk_size`.
- One reader's sequential behavior does not alter another reader's window size.

To use a fixed 128 MiB window:

```caddyfile
chunk_size 128MiB
chunk_size_limit 128MiB
preload_chunks 1
```

### Priority scheduler

Only one origin transfer is active at a time. This is intentional: a blocking playback read should not share a constrained connection with speculative preloads.

Tasks are ordered as:

1. blocking reads and seeks;
2. immediate preload;
3. speculative preload.

A seek can therefore jump ahead of queued preload work. If a preload already covers the requested range, it is promoted instead of duplicated.

### Shared downloads

Before creating a source request, varc considers:

- durable cached ranges;
- progressively written volatile ranges;
- active task ranges.

Concurrent readers requesting the same missing bytes reuse one source task. Repeated wake-ups cannot create duplicate requests for ranges already owned by an active task.

### Cancellation and lifecycle

- `Seek` updates the cursor immediately and does not wait for an old fetch.
- `Close` is idempotent and unblocks waiting reads.
- Closing the final reader for an entry cancels its remaining origin tasks.
- Shared tasks remain active while another reader for the entry still exists.
- `Cache.Close` cancels active and queued work and waits for cleanup.

## Repository layout

```text
handler.go                  Caddy HTTP handler
caddyfile.go                Caddyfile parser
source.go                   validated HTTP range source
range.go                    HTTP Range parsing and validators
logger.go                   logging adapter
realworld_http_test.go      HTTP transport failure scenarios

varc/
  varc.go                   cache, reader, metadata, pruning, lifecycle
  adaptive.go               per-reader access detection and window growth
  scheduler.go              priority queue and download pipeline
  ops.go                    warming, repair, pinning, manifests, health
  adaptive_test.go          deterministic adaptive scheduler tests
  realworld_test.go         failure injection and stress scenarios
  slow_remote_test.go       slow backing-object simulation
  stream_test.go            progressive streaming and retry behavior
```

## Build with Caddy

Build a Caddy binary containing this module:

```bash
xcaddy build \
  --with github.com/tgdrive/varc=.
```

Run the example configuration:

```bash
./caddy run --config examples/Caddyfile
```

Validate the Caddyfile before deployment:

```bash
./caddy adapt --config examples/Caddyfile --pretty
```

## Caddyfile example

```caddyfile
:8080 {
    route /media/* {
        varc https://origin.example.com {
            cache_dir /var/cache/caddy/varc
            key {host}:{uri}

            # Append the incoming URI to the configured upstream.
            append_uri on

            # Normalize keys without accidentally merging distinct objects.
            ignore_query off
            strip_query utm_source utm_medium fbclid gclid
            sort_query on
            lowercase_host on
            vary_header Accept-Language

            # Media cache windows.
            chunk_size 32MiB
            chunk_size_limit 128MiB
            preload_chunks 1

            # Cache retention.
            max_size 500GiB
            max_age 168h
            min_free_space 20GiB
            poll_interval 1m
            shard_level 2

            # Shared-cache safety defaults.
            bypass_header X-No-Cache
            bypass_cookie session
            bypass_query nocache
            cache_authorization off
            cache_set_cookie off
            cache_private off
            cache_no_store off
            stale_if_error 1h

            # Origin transport.
            timeout 60s
            probe_timeout 15s
            dial_timeout 10s
            response_header_timeout 30s
            idle_conn_timeout 90s
            max_idle_conns 256
            read_retry_count 2
            read_retry_delay 100ms

            # Optional origin authentication.
            # header Authorization "Bearer {$ORIGIN_TOKEN}"
            # forward_header Authorization

            # Debug and administration.
            debug_headers on
            admin_path /_varc
            # admin_token {$VARC_ADMIN_TOKEN}
        }
    }
}
```

## Recommended media settings

### Balanced adaptive mode

```caddyfile
chunk_size 32MiB
chunk_size_limit 128MiB
preload_chunks 1
```

Use this for normal playback with occasional seeks.

### Fixed 128 MiB mode

```caddyfile
chunk_size 128MiB
chunk_size_limit 128MiB
preload_chunks 1
```

Use this when the backing system naturally works in 128 MiB objects and request overhead is more important than minimizing logical fetch size.

### Seek-heavy workloads

```caddyfile
chunk_size 16MiB
chunk_size_limit 32MiB
preload_chunks 0
```

Use this for thumbnail extraction, probing, or highly random reads.

## Cache-only mode

Cache-only mode never opens the upstream:

```caddyfile
:8080 {
    route /media/* {
        varc https://origin.example.com {
            cache_dir /var/cache/caddy/varc
            cache_only on
            pass_thru off
        }
    }
}
```

A cached range is served locally. A missing range returns a cache miss unless `pass_thru on` is enabled.

## JSON configuration

```json
{
  "apps": {
    "http": {
      "servers": {
        "srv0": {
          "listen": [":8080"],
          "routes": [
            {
              "match": [{"path": ["/media/*"]}],
              "handle": [
                {
                  "handler": "varc",
                  "upstream": "https://origin.example.com",
                  "cache_dir": "/var/cache/caddy/varc",
                  "key": "{http.request.host}:{http.request.uri}",
                  "chunk_size": 33554432,
                  "chunk_size_limit": 134217728,
                  "preload_chunks": 1,
                  "cache_max_size": 536870912000,
                  "cache_min_free_space": 21474836480,
                  "debug_headers": true
                }
              ]
            }
          ]
        }
      }
    }
  }
}
```

## HTTP requirements

The HTTP adapter expects a byte-addressable origin.

- Range responses must use `206 Partial Content`.
- `Content-Range` must exactly match the requested range.
- The reported total size must match the known object size.
- A declared `Content-Length` must match the requested byte count.
- `Accept-Encoding: identity` is forced so byte offsets remain stable.
- Multi-range client requests are rejected with `416`.

Unexpected `200` responses, malformed ranges, missing ranges, wrong totals, truncated bodies, and incorrect content lengths are treated as errors.

## Authentication and shared-cache safety

Requests containing `Authorization` bypass the cache by default. Enable `cache_authorization` only when either:

- every authorized user receives identical bytes; or
- the cache key includes the authorization scope or tenant identity.

Responses containing any of the following bypass by default:

- `Set-Cookie`;
- `Cache-Control: private`;
- `Cache-Control: no-store`.

Do not disable these protections for an uncontrolled shared origin.

Origin credentials can be configured statically:

```caddyfile
header Authorization "Bearer {$ORIGIN_TOKEN}"
```

or forwarded from the request:

```caddyfile
forward_header Authorization
```

The HTTP source retries transient failures according to `read_retry_count` and `read_retry_delay`. Credential refresh itself must be handled by the configured client, middleware, or credential provider.

## Response headers

Normal responses include the appropriate standard headers:

- `Accept-Ranges: bytes`
- `Content-Length`
- `Content-Range` for `206 Partial Content`
- `ETag` when supplied by the origin
- `Last-Modified` when supplied by the origin

With `debug_headers on`, responses can also include:

- `X-Varc-Cache: HIT|MISS|STALE|BYPASS`
- `X-Varc-Key`
- `X-Varc-Source`
- `X-Varc-Range`
- `X-Varc-Bypass`

Do not expose debug headers publicly when keys or source URLs contain sensitive information.

## Admin endpoint

When `admin_path /_varc` is configured, the endpoint is restricted to loopback by default.

Set `admin_token` to require either:

```text
Authorization: Bearer <token>
```

or:

```text
X-Varc-Admin-Token: <token>
```

Only enable `admin_allow_remote on` behind authentication, mTLS, or another trusted access-control layer.

Examples:

```bash
curl http://localhost:8080/_varc
curl http://localhost:8080/_varc/metrics
curl 'http://localhost:8080/_varc/object?key=https://origin.example.com/media/a.mp4'
curl -X POST 'http://localhost:8080/_varc?action=prune'
curl -X POST 'http://localhost:8080/_varc?action=purge&key=https://origin.example.com/media/a.mp4'
curl -X POST 'http://localhost:8080/_varc?action=pin&key=https://origin.example.com/media/a.mp4'
curl -X POST 'http://localhost:8080/_varc?action=unpin&key=https://origin.example.com/media/a.mp4'
curl -X POST 'http://localhost:8080/_varc?action=repair&dry_run=true'
curl -X POST 'http://localhost:8080/_varc?action=warm&url=https://origin.example.com/media/a.mp4&range=0-8388607'
```

## Go library

The core package can be used without Caddy.

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
    data := bytes.Repeat([]byte("media-data-"), 1024)

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

    source := bytes.NewReader(data)
    reader, err := cache.Open(ctx, "movie.bin", int64(len(data)), source)
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    buffer := make([]byte, 64<<10)
    if _, err := reader.ReadAt(buffer, 0); err != nil {
        log.Fatal(err)
    }
}
```

Sources may implement only `io.ReaderAt`. Implementing `RangeSource` allows varc to stream each logical range through one source request.

See [API_USAGE.md](API_USAGE.md) for the full library API.

## Operational controls

The core cache includes:

- **Range planning** — inspect cached and missing segments without contacting the source.
- **Pinning** — protect expensive entries from age, size, and free-space pruning.
- **Batch warming** — prefetch objects or selected ranges with bounded job concurrency.
- **Manifest export/import** — move or pre-seed cache metadata.
- **Repair** — remove corrupt metadata, invalid ranges, and orphaned state.
- **Checksums** — optionally verify cached blocks.
- **Health snapshots** — inspect writability, free space, active readers, active fetches, and coverage.
- **Metrics** — observe reads, misses, source traffic, errors, evictions, and metadata writes.

## Failure behavior

### Source interruption

If a stream ends early, successfully written bytes remain available. Retry resumes from the first uncommitted offset rather than restarting the entire logical window.

### Stalled source

Reader or cache cancellation propagates to the source context. Closing the final reader cancels orphaned active and queued work.

### Cache write failure

Directory creation, file opening, writing, syncing, or metadata persistence errors are returned to the caller. Failed writes do not claim durable cached coverage.

### Restart

Only committed metadata ranges are trusted after restart. Progressive volatile coverage that was never committed is not advertised as durable.

### Stale objects

Use stable keys and fingerprints such as ETags, modification timestamps, versioned URLs, or content-generation identifiers. A changed fingerprint or size invalidates incompatible cached state.

Cached hits intentionally do not contact the upstream for validation.

## Multi-process and filesystem considerations

Multiple cache instances can read and write the same directory in basic scenarios, but varc does not provide a distributed lock manager or transactional coordination between independent processes.

For production, prefer one cache owner per directory. Treat concurrent writers from separate processes as an advanced deployment requiring external coordination.

Local filesystems with sparse-file support are recommended. NFS, SMB, FUSE, and network filesystems may have different locking, write-ordering, sparse-allocation, and rename semantics.

Enable `sync_writes` when crash durability matters more than throughput, but no application-level test can perfectly reproduce sudden power loss or storage-controller behavior.

## Testing

The repository includes deterministic tests for:

- adaptive growth and seek reset;
- fixed-size chunk operation;
- blocking-read priority over queued preloads;
- shared-reader task deduplication;
- progressive visibility;
- truncated streams and exact retry offsets;
- malformed HTTP range responses;
- transient authorization failure;
- stalled-source cancellation;
- removal during active download;
- restart durability;
- cache write failure;
- metadata corruption and repair;
- checksums and pruning;
- multi-terabyte sparse offsets;
- hundreds of simultaneous readers;
- repeated cancellation and scheduler cleanup;
- two cache instances sharing one directory;
- slow backing stores that internally fetch larger physical objects.

Run the standard suite:

```bash
go test -count=1 -timeout=180s ./...
```

Repeat concurrency-sensitive tests:

```bash
go test -count=5 -timeout=240s ./...
```

Run with the race detector:

```bash
go test -race -count=1 -timeout=240s ./...
```

Run static checks:

```bash
go vet ./...
```

## Known external validation limits

The test suite cannot completely reproduce:

- sudden machine power loss during a physical disk write;
- kernel-specific NFS, SMB, or FUSE consistency behavior;
- real block-device or inode exhaustion;
- distributed multi-process locking across machines;
- multi-hour transfers over a particular remote provider;
- browser, FFmpeg, mpv, HLS, or DASH behavior for every codec and bitrate;
- production load distributed across many hosts.

Those scenarios should be validated in the deployment environment with real storage, networking, players, and monitoring.

## Production checklist

Before deployment:

- use a stable cache key that includes every dimension affecting object bytes;
- keep authorization and private-response bypass protections enabled unless isolation is guaranteed;
- reserve enough local free space for sparse files and metadata;
- set `max_size`, `min_free_space`, and `max_age`;
- use one cache owner per directory where possible;
- validate that the origin returns correct single-range `206` responses;
- choose fixed or adaptive chunk settings based on seek frequency and source request cost;
- expose admin endpoints only through trusted access controls;
- monitor cache misses, source errors, active fetches, free space, and incomplete entries;
- run integration tests with the actual origin and media clients.
