package caddycache

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestModuleID(t *testing.T) {
	if got := (Handler{}).CaddyModule().ID; got != "http.handlers.varc" {
		t.Fatalf("module ID = %q", got)
	}
}

func TestParseBytes(t *testing.T) {
	tests := map[string]int64{
		"0":     0,
		"123":   123,
		"1KiB":  1 << 10,
		"2MiB":  2 << 20,
		"3GiB":  3 << 30,
		"4KB":   4_000,
		"5MB":   5_000_000,
		"6GB":   6_000_000_000,
		"1 TiB": 1 << 40,
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			got, err := parseBytes(input)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("parseBytes(%q) = %d, want %d", input, got, want)
			}
		})
	}
	for _, input := range []string{"", "-1", "1.5GiB", "garbage"} {
		t.Run("invalid_"+input, func(t *testing.T) {
			if _, err := parseBytes(input); err == nil {
				t.Fatalf("parseBytes(%q) unexpectedly succeeded", input)
			}
		})
	}
}

func TestUnmarshalCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser(`varc https://origin.example/base {
	cache_dir /var/cache/varc
	key {host}:{uri}
	max_size 10GiB
	min_free_space 1GiB
	shard_depth 2
	max_age 24h
	poll_interval 30s
	chunk_size 64MiB
	chunk_size_limit 1GiB
	chunk_streams 3
	read_ahead 8MiB
	buffer_size 32MiB
	handle_caching 2s
	retries 4
	header_up Authorization "Bearer test-token"
}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if h.Upstream != "https://origin.example/base" || h.CacheDir != "/var/cache/varc" {
		t.Fatalf("unexpected base config: %+v", h)
	}
	if h.Key != "{host}:{uri}" {
		t.Fatalf("key template = %q", h.Key)
	}
	if h.CacheMaxSize == nil || *h.CacheMaxSize != 10<<30 {
		t.Fatalf("max size = %v", h.CacheMaxSize)
	}
	if h.CacheMinFreeSpace == nil || *h.CacheMinFreeSpace != 1<<30 {
		t.Fatalf("min free space = %v", h.CacheMinFreeSpace)
	}
	if h.CacheShardDepth == nil || *h.CacheShardDepth != 2 {
		t.Fatalf("shard depth = %v", h.CacheShardDepth)
	}
	if h.ChunkSize == nil || *h.ChunkSize != 64<<20 {
		t.Fatalf("chunk size = %v", h.ChunkSize)
	}
	if h.ChunkSizeLimit == nil || *h.ChunkSizeLimit != 1<<30 {
		t.Fatalf("chunk size limit = %v", h.ChunkSizeLimit)
	}
	if h.ChunkStreams == nil || *h.ChunkStreams != 3 {
		t.Fatalf("chunk streams = %v", h.ChunkStreams)
	}
	if h.CacheMaxAge == nil || time.Duration(*h.CacheMaxAge) != 24*time.Hour {
		t.Fatalf("max age = %v", h.CacheMaxAge)
	}
	if h.CachePollInterval == nil || time.Duration(*h.CachePollInterval) != 30*time.Second {
		t.Fatalf("poll interval = %v", h.CachePollInterval)
	}
	if h.ReadAhead == nil || *h.ReadAhead != 8<<20 {
		t.Fatalf("read ahead = %v", h.ReadAhead)
	}
	if h.BufferSize == nil || *h.BufferSize != 32<<20 {
		t.Fatalf("buffer size = %v", h.BufferSize)
	}
	if h.HandleCaching == nil || time.Duration(*h.HandleCaching) != 2*time.Second {
		t.Fatalf("handle caching = %v", h.HandleCaching)
	}
	if h.LowLevelRetries == nil || *h.LowLevelRetries != 4 {
		t.Fatalf("retries = %v", h.LowLevelRetries)
	}
	if got := h.Headers.Get("Authorization"); got != "Bearer test-token" {
		t.Fatalf("Authorization = %q", got)
	}
}

func TestCacheKeyTemplate(t *testing.T) {
	keyFunc := newCacheKeyFunc("{host}:{uri}")
	req := httptest.NewRequest(http.MethodGet, "http://media.example/movie.bin?token=abc", nil)
	got, err := keyFunc(req)
	if err != nil {
		t.Fatal(err)
	}
	if got != "media.example:/movie.bin?token=abc" {
		t.Fatalf("cache key = %q", got)
	}

	keyFunc = newCacheKeyFunc("{http.request.header.X-Tenant}:{http.request.uri.path}")
	req = httptest.NewRequest(http.MethodGet, "http://media.example/movie.bin?token=abc", nil)
	req.Header.Set("X-Tenant", "acme")
	got, err = keyFunc(req)
	if err != nil {
		t.Fatal(err)
	}
	if got != "acme:/movie.bin" {
		t.Fatalf("full-placeholder cache key = %q", got)
	}

	keyFunc = newCacheKeyFunc("{does.not.exist}")
	if _, err := keyFunc(req); err == nil {
		t.Fatal("unknown placeholder unexpectedly succeeded")
	}
}

func TestDynamicUpstreamTemplate(t *testing.T) {
	data := []byte("0123456789")
	var wantHost string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != wantHost {
			http.Error(w, "unexpected origin host", http.StatusBadRequest)
			return
		}
		if r.URL.Path != "/movie.bin" || r.URL.Query().Get("token") != "abc" {
			http.Error(w, "unexpected origin URL", http.StatusBadRequest)
			return
		}
		http.ServeContent(w, r, "movie.bin", time.Unix(1, 0), bytes.NewReader(data))
	}))
	defer origin.Close()
	originURL, err := url.Parse(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	wantHost = originURL.Host

	h := &Handler{
		Upstream: "{http.request.uri.query.target}",
		CacheDir: t.TempDir(),
	}
	zero := caddy.Duration(0)
	h.CachePollInterval = &zero
	if err := h.provision(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer h.Cleanup()

	target := origin.URL + "/movie.bin?token=abc"
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8888/?target="+url.QueryEscape(target), nil)
	cacheKey, err := h.handler.CacheKey(req)
	if err != nil {
		t.Fatal(err)
	}
	if cacheKey != "movie.bin" {
		t.Fatalf("dynamic cache key = %q, want %q", cacheKey, "movie.bin")
	}
	req.Header.Set("Range", "bytes=2-5")
	rr := httptest.NewRecorder()
	next := caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
		t.Fatal("GET unexpectedly continued to next handler")
		return nil
	})
	if err := h.ServeHTTP(rr, req, next); err != nil {
		t.Fatal(err)
	}
	if rr.Code != http.StatusPartialContent || rr.Body.String() != "2345" {
		t.Fatalf("dynamic upstream response = %d %q", rr.Code, rr.Body.String())
	}
}

func TestProvisionServeAndCleanup(t *testing.T) {
	data := []byte("0123456789abcdef")
	modTime := time.Date(2026, time.August, 9, 10, 0, 0, 0, time.UTC)
	var heads atomic.Int64
	var gets atomic.Int64

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Origin-Token"); got != "secret" {
			http.Error(w, "missing origin token", http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/objects/movie.bin" {
			http.Error(w, "unexpected origin path", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodHead:
			heads.Add(1)
		case http.MethodGet:
			gets.Add(1)
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "movie.bin", modTime, bytes.NewReader(data))
	}))
	defer origin.Close()

	h := &Handler{
		Upstream: origin.URL + "/objects",
		CacheDir: t.TempDir(),
		Headers:  http.Header{"X-Origin-Token": []string{"secret"}},
		Key:      "{host}:{uri}",
	}
	zero := caddy.Duration(0)
	h.CachePollInterval = &zero
	if err := h.provision(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	defer h.Cleanup()

	nextCalled := atomic.Bool{}
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		nextCalled.Store(true)
		w.WriteHeader(http.StatusNoContent)
		return nil
	})

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://cache.example/movie.bin", nil)
		req.Header.Set("Range", "bytes=2-5")
		rr := httptest.NewRecorder()
		if err := h.ServeHTTP(rr, req, next); err != nil {
			t.Fatal(err)
		}
		if rr.Code != http.StatusPartialContent || rr.Body.String() != "2345" {
			t.Fatalf("response = %d %q", rr.Code, rr.Body.String())
		}
	}

	otherHostReq := httptest.NewRequest(http.MethodGet, "http://cache-two.example/movie.bin", nil)
	otherHostReq.Header.Set("Range", "bytes=2-5")
	otherHostRR := httptest.NewRecorder()
	if err := h.ServeHTTP(otherHostRR, otherHostReq, next); err != nil {
		t.Fatal(err)
	}
	if otherHostRR.Code != http.StatusPartialContent || otherHostRR.Body.String() != "2345" {
		t.Fatalf("different-host response = %d %q", otherHostRR.Code, otherHostRR.Body.String())
	}
	if nextCalled.Load() {
		t.Fatal("GET unexpectedly continued to next handler")
	}
	if gets.Load() != 2 {
		t.Fatalf("origin GETs = %d, want 2 cache fills for two host keys", gets.Load())
	}
	if heads.Load() != 3 {
		t.Fatalf("origin HEADs = %d, want 3 metadata validations", heads.Load())
	}

	post := httptest.NewRequest(http.MethodPost, "http://cache.example/movie.bin", nil)
	postRR := httptest.NewRecorder()
	if err := h.ServeHTTP(postRR, post, next); err != nil {
		t.Fatal(err)
	}
	if !nextCalled.Load() || postRR.Code != http.StatusNoContent {
		t.Fatalf("POST did not continue to next handler: called=%v code=%d", nextCalled.Load(), postRR.Code)
	}

	if err := h.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := h.Cleanup(); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
}
