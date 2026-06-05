# varc

`varc` is a Caddy v2 HTTP handler module for serving remote HTTP objects through the `varc` sparse read-through range cache.

The important behavior is **cache-first range serving**:

1. Build the varc key from the request.
2. Try `varc.Open(..., nil, varc.WithCacheOnly())`.
3. Parse the client `Range` header using cached metadata.
4. If the byte range is already present, serve from local disk immediately.
5. Only on miss, probe/fetch the upstream with HTTP `Range` requests.

That means already-cached video/media ranges do **not** create an upstream client/request.

## Layout

```text
varc/
  go.mod
  handler.go       # Caddy HTTP handler module
  caddyfile.go     # Caddyfile parser
  source.go        # HTTP Range -> io.ReaderAt adapter
  range.go         # Range / validator handling
  logger.go
  varc/varc.go     # embedded varc package from your implementation
  examples/Caddyfile
```

## Build with xcaddy

From this directory:

```bash
xcaddy build \
  --with github.com/tgdrive/varc=.
```

Then run:

```bash
./caddy run --config examples/Caddyfile
```

## Caddyfile

```caddyfile
:8080 {
    route /media/* {
        varc https://origin.example.com {
            cache_dir /var/cache/caddy/varc
            key {host}:{uri}

            # Request URL/key behavior. With append_uri on, /media/a.mp4 is
            # fetched from https://origin.example.com/media/a.mp4.
            append_uri on
            ignore_query off
            strip_query utm_source utm_medium fbclid gclid
            sort_query on
            lowercase_host on
            vary_header Accept-Language

            # Safe shared-cache defaults. Authorization bypasses unless
            # cache_authorization is explicitly enabled. Set-Cookie, private,
            # and no-store responses bypass unless explicitly enabled.
            bypass_header X-No-Cache
            bypass_cookie session
            bypass_query nocache
            cache_authorization off
            cache_set_cookie off
            cache_private off
            cache_no_store off
            stale_if_error 1h

            # Cache tuning.
            block_size 1MiB
            chunk_size 128MiB
            chunk_streams 4
            max_inflight_bytes 512MiB
            read_ahead 16MiB
            max_size 500GiB
            max_age 168h
            poll_interval 1m
            shard_level 2

            # Origin HTTP tuning.
            timeout 60s
            probe_timeout 15s
            dial_timeout 10s
            response_header_timeout 30s
            max_idle_conns 256

            # Optional origin auth / tenancy.
            # header Authorization "Bearer {$ORIGIN_TOKEN}"
            # forward_header Authorization

            # Debug/admin. Admin is loopback-only by default; use a token if
            # exposing it through authenticated Caddy routes.
            debug_headers on
            admin_path /_varc
            # admin_token {$VARC_ADMIN_TOKEN}
        }
    }
}
```

## Cache-only mode

Use this when Caddy must only serve data that is already present in varc:

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

On cache hit, no upstream HTTP request is made. On miss, the handler returns a miss/error unless `pass_thru on` is enabled.

## JSON config

```json
{
  "apps": {
    "http": {
      "servers": {
        "srv0": {
          "listen": [":8080"],
          "routes": [{
            "match": [{"path": ["/media/*"]}],
            "handle": [{
              "handler": "varc",
              "upstream": "https://origin.example.com",
              "cache_dir": "/var/cache/caddy/varc",
              "key": "{http.request.host}:{http.request.uri}",
              "chunk_size": 134217728,
              "block_size": 1048576,
              "chunk_streams": 4,
              "read_ahead": 16777216,
              "cache_max_size": 536870912000,
              "debug_headers": true
            }]
          }]
        }
      }
    }
  }
}
```

## Headers

The module always sets:

- `Accept-Ranges: bytes`
- `Content-Length`
- `Content-Range` for `206 Partial Content`
- `ETag` when the origin provides one
- `Last-Modified` when the origin provides one

With `debug_headers on`, it also sets:

- `X-Varc-Cache: HIT|MISS|STALE|BYPASS`
- `X-Varc-Key`
- `X-Varc-Source`
- `X-Varc-Range`
- `X-Varc-Bypass` for bypassed requests

## Admin endpoint

If `admin_path /_varc` is configured, the endpoint is loopback-only by default.
Set `admin_token` to require `Authorization: Bearer <token>` or
`X-Varc-Admin-Token`, and only use `admin_allow_remote on` behind Caddy auth/mTLS.

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

## Important production notes

- The handler assumes the upstream object is byte-addressable with HTTP `Range` support.
- It forces `Accept-Encoding: identity` so cached byte offsets map to real upstream bytes.
- Cached hits intentionally skip upstream validation. This is what avoids client initialization/network calls for already cached ranges. Use stable keys, ETags/fingerprints, TTL pruning, or versioned URLs to avoid serving stale content.
- Multi-range responses are rejected with `416`. Most media players issue single ranges.
- If your origin requires authorization, either add a static `header` or list request headers with `forward_header`. Requests with `Authorization` bypass by default. Only turn `cache_authorization on` when the cache key includes the auth scope or every authorized user receives identical bytes.
- `Set-Cookie`, `Cache-Control: private`, and `Cache-Control: no-store` bypass by default to avoid shared-cache poisoning. Enable the corresponding `cache_*` options only for controlled origins.
- Use `stale_if_error` to serve already-cached ranges when origin probing/opening fails.

For Caddy adapter validation:

```bash
./caddy adapt --config examples/Caddyfile --pretty
```

## Go library

The `varc` package is a standalone sparse read-through range cache for Go programs — media servers, object-storage gateways, or any workload that reads byte ranges from a slower `io.ReaderAt` source.

Minimal usage:

```go
cache, _ := varc.New(ctx, varc.Options{CacheDir: "./.cache"})
defer cache.Close()

// Anything that implements io.ReaderAt can be a source.
r, _ := cache.Open(ctx, key, size, src, varc.WithFingerprint(etag))
defer r.Close()

buf := make([]byte, 64<<10)
n, _ := r.ReadAt(buf, offset)
```

See [API_USAGE.md](API_USAGE.md) for the full Go library reference: cache keys, fingerprints, readers, warming, metrics, pruning, HTTP range source adapter, error handling, and production tuning.

## Operational controls

The cache includes additional operations for long-running deployments:

- **Range planning**: `Cache.Plan` and `Reader.PlanRange` return cached/missing segments for a request without contacting the origin. Use this to decide whether to serve cache-only, pre-open a backend client, or return a fast miss.
- **Pinned entries**: `Cache.Pin`, `Cache.Unpin`, and `Cache.IsPinned` protect hot/expensive objects from age, size, and free-space pruning while still allowing explicit `Remove`.
- **Batch warming**: `Cache.WarmBatch` prefetches many files or selected byte ranges with bounded worker concurrency and context cancellation.
- **Manifest export/import**: `ExportManifest` and `ImportManifest` snapshot/restore metadata sidecars for cache moves, disaster recovery, and pre-seeding.
- **Repair/scrub**: `Repair` can remove orphan/corrupt metadata, drop invalid ranges/checksums, and normalize sidecars after crashes or manual file operations.
- **Health snapshot**: `Health` reports writability, disk free space, pinned/complete/incomplete counts, open readers, active fetches, and inflight bytes.
- **Operator-grade Caddy handler**: safe bypass defaults, normalized keys, stale-if-error serving, request probe coalescing, admin purge/pin/unpin/warm/repair/object endpoints, Prometheus text metrics, and heavier handler tests.

See [API_USAGE.md](API_USAGE.md) for usage examples.
