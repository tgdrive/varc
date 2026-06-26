package varc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

func TestHandlerRangeHeadConditionalsAndUnsatisfiable(t *testing.T) {
	body := []byte(strings.Repeat("abcdef", 40))
	origin := newRangeOrigin(t, body, nil, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true

	// Initial fill through origin.
	first := httptest.NewRecorder()
	if err := h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "https://edge.test/object.bin", nil), failNext(t)); err != nil {
		t.Fatal(err)
	}
	if first.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), body) {
		t.Fatalf("first status=%d len=%d", first.Code, first.Body.Len())
	}

	// HEAD should use cached metadata and never write a body.
	head := httptest.NewRecorder()
	headReq := httptest.NewRequest(http.MethodHead, "https://edge.test/object.bin", nil)
	if err := h.ServeHTTP(head, headReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(len(body)) {
		t.Fatalf("head code=%d len=%d cl=%q", head.Code, head.Body.Len(), head.Header().Get("Content-Length"))
	}
	if got := head.Header().Get("X-Varc-Cache"); got != "HIT" {
		t.Fatalf("head cache=%q", got)
	}

	// Conditional GET should return 304 from cached ETag metadata.
	cond := httptest.NewRecorder()
	condReq := httptest.NewRequest(http.MethodGet, "https://edge.test/object.bin", nil)
	condReq.Header.Set("If-None-Match", `"test-etag"`)
	if err := h.ServeHTTP(cond, condReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if cond.Code != http.StatusNotModified || cond.Body.Len() != 0 {
		t.Fatalf("conditional code=%d body=%q", cond.Code, cond.Body.String())
	}

	// Range read should be 206 with precise content-range.
	ranged := httptest.NewRecorder()
	rangeReq := httptest.NewRequest(http.MethodGet, "https://edge.test/object.bin", nil)
	rangeReq.Header.Set("Range", "bytes=5-14")
	if err := h.ServeHTTP(ranged, rangeReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if ranged.Code != http.StatusPartialContent || ranged.Header().Get("Content-Range") != fmt.Sprintf("bytes 5-14/%d", len(body)) {
		t.Fatalf("range code=%d cr=%q", ranged.Code, ranged.Header().Get("Content-Range"))
	}
	if !bytes.Equal(ranged.Body.Bytes(), body[5:15]) {
		t.Fatalf("range body mismatch %q", ranged.Body.String())
	}

	bad := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodGet, "https://edge.test/object.bin", nil)
	badReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", len(body)+10, len(body)+20))
	if err := h.ServeHTTP(bad, badReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if bad.Code != http.StatusRequestedRangeNotSatisfiable || bad.Header().Get("Content-Range") != fmt.Sprintf("bytes */%d", len(body)) {
		t.Fatalf("bad range code=%d cr=%q", bad.Code, bad.Header().Get("Content-Range"))
	}
}

func TestHandlerPassThruAndMethodHandling(t *testing.T) {
	h := newUnitHandler(t, "https://origin.invalid")
	post := httptest.NewRequest(http.MethodPost, "https://edge.test/upload", nil)
	if err := h.ServeHTTP(httptest.NewRecorder(), post, failNext(t)); err == nil {
		t.Fatal("expected method-not-allowed error when pass_thru is disabled")
	}

	h.PassThru = true
	var nextCalled atomic.Bool
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		nextCalled.Store(true)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("next"))
		return nil
	})
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, post, next); err != nil {
		t.Fatal(err)
	}
	if !nextCalled.Load() || rr.Code != http.StatusAccepted || rr.Body.String() != "next" {
		t.Fatalf("pass thru failed called=%v code=%d body=%q", nextCalled.Load(), rr.Code, rr.Body.String())
	}
}

func TestHandlerCacheOnlyMissPassThruAndError(t *testing.T) {
	h := newUnitHandler(t, "https://origin.invalid")
	h.CacheOnly = true
	if err := h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "https://edge.test/miss", nil), failNext(t)); err == nil {
		t.Fatal("expected cache-only miss error")
	}

	h.PassThru = true
	var nextCalled atomic.Bool
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		nextCalled.Store(true)
		w.WriteHeader(http.StatusTeapot)
		return nil
	})
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "https://edge.test/miss", nil), next); err != nil {
		t.Fatal(err)
	}
	if !nextCalled.Load() || rr.Code != http.StatusTeapot {
		t.Fatalf("expected pass-through cache-only miss called=%v code=%d", nextCalled.Load(), rr.Code)
	}
}

func TestHandlerProbeFallbackToRange(t *testing.T) {
	body := []byte(strings.Repeat("range-probe", 200))
	var heads atomic.Int64
	var rangeProbes atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", "fallback")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if r.Method == http.MethodHead {
			heads.Add(1)
			time.Sleep(30 * time.Millisecond)
			// Deliberately omit Content-Length so probeRemote must try Range.
			return
		}
		span, err := parseSingleRange(r.Header.Get("Range"), int64(len(body)))
		if err != nil {
			writeRangeNotSatisfiable(w, int64(len(body)))
			return
		}
		if span.Partial {
			if span.Start == 0 && span.End == 1 {
				rangeProbes.Add(1)
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(body)))
			w.Header().Set("Content-Length", fmt.Sprint(span.Length()))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[span.Start:span.End])
			return
		}
		_, _ = w.Write(body)
	}))
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)

	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "https://edge.test/fallback.txt", nil), failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Code != http.StatusOK || rr.Body.Len() != len(body) {
		t.Fatalf("fallback status=%d len=%d body=%q", rr.Code, rr.Body.Len(), rr.Body.String())
	}
	if heads.Load() != 1 || rangeProbes.Load() != 1 {
		t.Fatalf("fallback probe counts wrong: heads=%d rangeProbes=%d", heads.Load(), rangeProbes.Load())
	}
}

func TestHandlerProbeSingleflightCollapsesConcurrentHEADs(t *testing.T) {
	body := []byte(strings.Repeat("singleflight", 100))
	var heads atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Header().Set("ETag", "sf")
		if r.Method == http.MethodHead {
			heads.Add(1)
			time.Sleep(30 * time.Millisecond)
			return
		}
		span, err := parseSingleRange(r.Header.Get("Range"), int64(len(body)))
		if err != nil {
			writeRangeNotSatisfiable(w, int64(len(body)))
			return
		}
		if span.Partial {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[span.Start:span.End])
			return
		}
		_, _ = w.Write(body)
	}))
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)

	var wg sync.WaitGroup
	var failed atomic.Bool
	req := httptest.NewRequest(http.MethodGet, "https://edge.test/sf.bin", nil)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			remote, err := h.probeRemoteSingleflight(req.Context(), req, "sf-key", origin.URL+"/sf.bin")
			if err != nil || remote.Size != int64(len(body)) {
				failed.Store(true)
			}
		}()
	}
	wg.Wait()
	if failed.Load() {
		t.Fatal("concurrent singleflight probe failed")
	}
	if heads.Load() != 1 {
		t.Fatalf("probe singleflight broken: heads=%d", heads.Load())
	}
}

func TestHandlerRequestAndResponseBypassPolicies(t *testing.T) {
	body := []byte("private payload")
	originHeaders := http.Header{}
	originHeaders.Set("Cache-Control", "private, max-age=60")
	origin := newRangeOrigin(t, body, originHeaders, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true
	h.BypassHeaders = []string{"X-No-Cache"}
	h.BypassCookies = []string{"sid"}
	h.BypassQuery = []string{"nocache"}

	cases := []struct {
		name   string
		mutate func(*http.Request)
		want   string
	}{
		{"header", func(r *http.Request) { r.Header.Set("X-No-Cache", "1") }, "header:X-No-Cache"},
		{"cookie", func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "sid", Value: "abc"}) }, "cookie:sid"},
		{"query", func(r *http.Request) { r.URL.RawQuery = "nocache=1" }, "query:nocache"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://edge.test/policy.bin", nil)
			tc.mutate(req)
			rr := httptest.NewRecorder()
			if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
				t.Fatal(err)
			}
			if rr.Header().Get("X-Varc-Cache") != "BYPASS" || rr.Header().Get("X-Varc-Bypass") != tc.want {
				t.Fatalf("headers cache=%q reason=%q", rr.Header().Get("X-Varc-Cache"), rr.Header().Get("X-Varc-Bypass"))
			}
		})
	}

	// Response Cache-Control: private bypasses by default.
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "https://edge.test/private.bin", nil), failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Header().Get("X-Varc-Bypass") != "cache-control:private" {
		t.Fatalf("private bypass reason=%q", rr.Header().Get("X-Varc-Bypass"))
	}
	if entries, _ := h.cache.ListEntries(context.Background()); len(entries) != 0 {
		t.Fatalf("private response populated cache: %d entries", len(entries))
	}

	// Explicitly allowing private should cache and become a hit on second request.
	h.CachePrivate = true
	if err := h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "https://edge.test/cache-private.bin", nil), failNext(t)); err != nil {
		t.Fatal(err)
	}
	second := httptest.NewRecorder()
	if err := h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "https://edge.test/cache-private.bin", nil), failNext(t)); err != nil {
		t.Fatal(err)
	}
	if got := second.Header().Get("X-Varc-Cache"); got != "HIT" {
		t.Fatalf("expected HIT after cache_private on, got %q", got)
	}
}

func TestHandlerAdminAuthStatusHealthWarmPruneRepairAndErrors(t *testing.T) {
	body := []byte(strings.Repeat("warm", 80))
	origin := newRangeOrigin(t, body, nil, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.AdminPath = "/_varc"
	h.AdminToken = "secret"
	h.AdminAllowRemote = true

	// Missing token must be rejected even from loopback.
	badReq := httptest.NewRequest(http.MethodGet, "https://edge.test/_varc", nil)
	badReq.RemoteAddr = "127.0.0.1:12345"
	if err := h.ServeHTTP(httptest.NewRecorder(), badReq, failNext(t)); err == nil {
		t.Fatal("expected admin auth failure")
	}

	admin := func(method, target string) string {
		t.Helper()
		req := httptest.NewRequest(method, "https://edge.test"+target, nil)
		req.RemoteAddr = "203.0.113.9:12345"
		req.Header.Set("X-Varc-Admin-Token", "secret")
		rr := httptest.NewRecorder()
		if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
			t.Fatalf("admin %s %s: %v body=%s", method, target, err, rr.Body.String())
		}
		if rr.Code < 200 || rr.Code >= 300 {
			t.Fatalf("admin status=%d body=%s", rr.Code, rr.Body.String())
		}
		return rr.Body.String()
	}

	status := admin(http.MethodGet, "/_varc")
	if !strings.Contains(status, "cache_dir") || !strings.Contains(status, "handler_metrics") {
		t.Fatalf("bad status payload: %s", status)
	}
	health := admin(http.MethodGet, "/_varc/health")
	if !strings.Contains(health, "writable") {
		t.Fatalf("bad health payload: %s", health)
	}
	warm := admin(http.MethodPost, "/_varc?action=warm&url="+urlQueryEscape(origin.URL+"/warm.bin")+"&range=0-31")
	if !strings.Contains(warm, "results") || !strings.Contains(warm, "warm.bin") {
		t.Fatalf("bad warm payload: %s", warm)
	}
	plan := admin(http.MethodGet, "/_varc/plan?url="+urlQueryEscape(origin.URL+"/warm.bin")+"&start=0&end=32")
	if !strings.Contains(plan, "CachedBytes") && !strings.Contains(plan, "cached_bytes") {
		t.Fatalf("bad plan payload: %s", plan)
	}
	prune := admin(http.MethodPost, "/_varc/prune")
	if !strings.Contains(prune, "prune") {
		t.Fatalf("bad prune payload: %s", prune)
	}
	repair := admin(http.MethodPost, "/_varc/repair?dry_run=true&drop_bad_ranges=off&remove_missing_data=no")
	if !strings.Contains(repair, "repair") || !strings.Contains(repair, "DryRun") {
		t.Fatalf("bad repair payload: %s", repair)
	}
	metrics := admin(http.MethodGet, "/_varc/metrics")
	if !strings.Contains(metrics, "varc_handler_warms") {
		t.Fatalf("metrics missing warm counter: %s", metrics)
	}

	getNeedsPost := httptest.NewRequest(http.MethodGet, "https://edge.test/_varc/purge?key=x", nil)
	getNeedsPost.RemoteAddr = "127.0.0.1:1"
	getNeedsPost.Header.Set("Authorization", "Bearer secret")
	if err := h.ServeHTTP(httptest.NewRecorder(), getNeedsPost, failNext(t)); err == nil {
		t.Fatal("expected GET purge to require POST")
	}
	unknown := httptest.NewRequest(http.MethodPost, "https://edge.test/_varc?action=bogus", nil)
	unknown.RemoteAddr = "127.0.0.1:1"
	unknown.Header.Set("Authorization", "Bearer secret")
	if err := h.ServeHTTP(httptest.NewRecorder(), unknown, failNext(t)); err == nil {
		t.Fatal("expected unknown action error")
	}
}

func TestHandlerOriginHeadersAndProxyBypassHeaderCopy(t *testing.T) {
	body := []byte("hello forwarded headers")
	var sawForward atomic.Bool
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forward-Me") == "yes" && r.Header.Get("X-Static") == "edge.test" && r.Header.Get("Accept-Encoding") == "identity" {
			sawForward.Store(true)
		}
		w.Header().Set("Connection", "close")
		w.Header().Set("X-Origin", "kept")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		span, _ := parseSingleRange(r.Header.Get("Range"), int64(len(body)))
		if span.Partial {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[span.Start:span.End])
			return
		}
		_, _ = w.Write(body)
	}))
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true
	h.StaticHeaders = http.Header{"X-Static": []string{"{host}"}}
	h.ForwardHeaders = []string{"X-Forward-Me"}
	h.BypassHeaders = []string{"X-Bypass"}

	req := httptest.NewRequest(http.MethodGet, "https://edge.test/h.txt", nil)
	req.Header.Set("X-Forward-Me", "yes")
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if !sawForward.Load() {
		t.Fatal("origin did not receive forwarded/static headers")
	}

	bypass := httptest.NewRecorder()
	bypassReq := httptest.NewRequest(http.MethodGet, "https://edge.test/h.txt", nil)
	bypassReq.Header.Set("X-Bypass", "1")
	if err := h.ServeHTTP(bypass, bypassReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if bypass.Header().Get("X-Origin") != "kept" || bypass.Header().Get("Connection") != "" {
		t.Fatalf("bad proxied headers kept=%q connection=%q", bypass.Header().Get("X-Origin"), bypass.Header().Get("Connection"))
	}
}

func TestCaddyfileParsersAndValidationProvisionCleanup(t *testing.T) {
	if got, err := parseBytes("1.5MiB"); err != nil || got != 1572864 {
		t.Fatalf("parseBytes MiB got=%d err=%v", got, err)
	}
	if got, err := parseBytes("2kb"); err != nil || got != 2000 {
		t.Fatalf("parseBytes kb got=%d err=%v", got, err)
	}
	if _, err := parseBytes("-1"); err == nil {
		t.Fatal("expected negative parseBytes error")
	}
	if _, err := parseBytes("wat"); err == nil {
		t.Fatal("expected invalid parseBytes error")
	}

	badBool := caddyfileTestDispenser(`varc https://origin { debug_headers maybe }`)
	if err := new(Handler).UnmarshalCaddyfile(badBool); err == nil {
		t.Fatal("expected bad boolean parse error")
	}
	badUnknown := caddyfileTestDispenser(`varc https://origin { unknown_option yes }`)
	if err := new(Handler).UnmarshalCaddyfile(badUnknown); err == nil {
		t.Fatal("expected unknown directive error")
	}
	badArgs := caddyfileTestDispenser(`varc https://one https://two`)
	if err := new(Handler).UnmarshalCaddyfile(badArgs); err == nil {
		t.Fatal("expected top-level arg error")
	}

	if err := (&Handler{}).Validate(); err == nil {
		t.Fatal("expected missing upstream validation error")
	}
	if err := (&Handler{Upstream: "://bad"}).Validate(); err == nil {
		t.Fatal("expected invalid upstream validation error")
	}
	if err := (&Handler{Upstream: "https://origin", AdminPath: "relative"}).Validate(); err == nil {
		t.Fatal("expected admin path validation error")
	}
	if err := (&Handler{CacheOnly: true}).Validate(); err != nil {
		t.Fatalf("cache-only validate: %v", err)
	}
	if defaultInt(0, 7) != 7 || defaultInt(3, 7) != 3 || defaultDuration(0, time.Second) != time.Second || defaultDuration(time.Minute, time.Second) != time.Minute {
		t.Fatal("default helpers failed")
	}
	if joinURLPath("/base/", "/file/") != "/base/file/" || joinURLPath("", "") != "/" {
		t.Fatal("joinURLPath edge failed")
	}

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	h := &Handler{Upstream: "https://origin.example", CacheDir: t.TempDir(), CachePollInterval: caddy.Duration(-1), BlockSize: 32, ChunkSize: 64}
	if err := h.Provision(ctx); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if h.cache == nil || h.client == nil || h.flights == nil {
		t.Fatal("Provision did not initialize runtime")
	}
	if err := h.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if err := h.cache.Close(); err != nil { // idempotent close after cleanup
		t.Fatalf("second close: %v", err)
	}
}

func TestHTTPRangeSourceAndRemoteParsingErrors(t *testing.T) {
	if fp := (RemoteObject{ETag: "abc", Size: 9}).Fingerprint(); fp != "abc" {
		t.Fatalf("etag fingerprint=%q", fp)
	}
	lm := time.Unix(123, 0).UTC()
	if fp := (RemoteObject{LastModified: lm, Size: 9}).Fingerprint(); !strings.Contains(fp, ":9") {
		t.Fatalf("last-mod fingerprint=%q", fp)
	}
	if fp := (RemoteObject{Size: 9}).Fingerprint(); fp != "size:9" {
		t.Fatalf("size fingerprint=%q", fp)
	}
	if normalizeETag("raw") != `"raw"` || normalizeETag(`W/"raw"`) != `W/"raw"` || formatHTTPTime(time.Time{}) != "" {
		t.Fatal("etag/time helpers failed")
	}

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bad-status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not partial"))
		case "/bad-range":
			w.Header().Set("Content-Range", "bytes 3-4/10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("xx"))
		case "/missing-range":
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abcde"))
		case "/bad-total":
			w.Header().Set("Content-Range", "bytes 0-4/11")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abcde"))
		case "/short":
			w.Header().Set("Content-Range", "bytes 0-4/10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("xy"))
		case "/small":
			w.Header().Set("Content-Range", "bytes 0-2/3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abc"))
		default:
			body := []byte("abcdefghij")
			span, err := parseSingleRange(r.Header.Get("Range"), int64(len(body)))
			if err != nil {
				writeRangeNotSatisfiable(w, int64(len(body)))
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[span.Start:span.End])
		}
	}))
	defer origin.Close()

	for _, path := range []string{"/bad-status", "/bad-range", "/missing-range", "/bad-total", "/short"} {
		t.Run(path, func(t *testing.T) {
			src := &HTTPRangeSource{Client: http.DefaultClient, URL: origin.URL + path, ValidateSize: 10}
			_, err := src.ReadAt(make([]byte, 5), 0)
			if err == nil {
				t.Fatal("expected ReadAt error")
			}
		})
	}
	src := &HTTPRangeSource{Client: http.DefaultClient, URL: origin.URL + "/small", ValidateSize: 3}
	buf := make([]byte, 5)
	n, err := src.ReadAt(buf, 0)
	if err != nil || n != 3 || string(buf[:3]) != "abc" {
		t.Fatalf("clamped read n=%d err=%v data=%q", n, err, string(buf[:3]))
	}
	if n, err := src.ReadAt(nil, 0); n != 0 || err != nil {
		t.Fatalf("zero read n=%d err=%v", n, err)
	}
	if _, err := src.ReadAt(make([]byte, 1), -1); err == nil {
		t.Fatal("expected negative offset error")
	}
	if n, err := src.ReadAt(make([]byte, 1), 99); n != 0 || err != io.EOF {
		t.Fatalf("eof n=%d err=%v", n, err)
	}
}

func TestCaddyfileHelperParsersAndLoggerAndFlightGroupEdges(t *testing.T) {
	pairDisp := caddyfileTestDispenser(`varc https://origin {
	header X-Test value
}`)
	for pairDisp.Next() {
		for nesting := pairDisp.Nesting(); pairDisp.NextBlock(nesting); {
			if pairDisp.Val() == "header" {
				name, value, err := nextPair(pairDisp)
				if err != nil || name != "X-Test" || value != "value" {
					t.Fatalf("nextPair name=%q value=%q err=%v", name, value, err)
				}
			}
		}
	}
	badPair := caddyfileTestDispenser(`varc https://origin {
	header only
}`)
	for badPair.Next() {
		for nesting := badPair.Nesting(); badPair.NextBlock(nesting); {
			if _, _, err := nextPair(badPair); err == nil {
				t.Fatal("expected nextPair error")
			}
		}
	}
	intDisp := caddyfileTestDispenser(`varc https://origin {
	chunk_streams 12
}`)
	for intDisp.Next() {
		for nesting := intDisp.Nesting(); intDisp.NextBlock(nesting); {
			if got, err := nextInt(intDisp); err != nil || got != 12 {
				t.Fatalf("nextInt got=%d err=%v", got, err)
			}
		}
	}
	badInt := caddyfileTestDispenser(`varc https://origin {
	chunk_streams nope
}`)
	for badInt.Next() {
		for nesting := badInt.Nesting(); badInt.NextBlock(nesting); {
			if _, err := nextInt(badInt); err == nil {
				t.Fatal("expected nextInt error")
			}
		}
	}
	bytesDisp := caddyfileTestDispenser(`varc https://origin {
	block_size 64KiB
}`)
	for bytesDisp.Next() {
		for nesting := bytesDisp.Nesting(); bytesDisp.NextBlock(nesting); {
			if got, err := nextBytes(bytesDisp); err != nil || got != 64*1024 {
				t.Fatalf("nextBytes got=%d err=%v", got, err)
			}
		}
	}
	badBytes := caddyfileTestDispenser(`varc https://origin {
	block_size nope
}`)
	for badBytes.Next() {
		for nesting := badBytes.Nesting(); badBytes.NextBlock(nesting); {
			if _, err := nextBytes(badBytes); err == nil {
				t.Fatal("expected nextBytes error")
			}
		}
	}
	badDuration := caddyfileTestDispenser(`varc https://origin {
	stale_if_error nope
}`)
	if err := new(Handler).UnmarshalCaddyfile(badDuration); err == nil {
		t.Fatal("expected bad duration error")
	}

	parsed, err := parseCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfileTestDispenser(`varc https://origin.example {
	debug_headers on
}`)})
	if err != nil {
		t.Fatalf("parseCaddyfile: %v", err)
	}
	if _, ok := parsed.(*Handler); !ok {
		t.Fatalf("parseCaddyfile returned %T", parsed)
	}

	full := caddyfileTestDispenser(`varc https://origin.example/base {
	cache_dir /tmp/varc-test
	key {host}:{normalized_uri}
	append_uri off
	ignore_query on
	sync_writes on
	clean_on_start on
	verify_checksum on
	header X-Token static
	forward_header X-Forward
	timeout 5s
	probe_timeout 4s
	dial_timeout 3s
	tls_handshake_timeout 2s
	response_header_timeout 6s
	idle_conn_timeout 7s
	max_idle_conns 42
	block_size 64KiB
	chunk_size 1MiB
	chunk_size_limit 2MiB
	chunk_streams 3
	max_inflight_bytes 4MiB
	max_size 5MiB
	min_free_space 6MiB
	max_age 8s
	poll_interval 9s
	read_ahead 10KiB
	shard_level 3
	read_retry_count 2
	read_retry_delay 11ms
}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(full); err != nil {
		t.Fatalf("full caddyfile: %v", err)
	}
	if h.Upstream != "https://origin.example/base" || h.CacheDir != "/tmp/varc-test" || h.Key == "" || h.AppendURI == nil || *h.AppendURI || !h.IgnoreQuery || !h.SyncWrites || !h.CleanOnStart || !h.VerifyChecksum {
		t.Fatalf("basic full parse failed: upstream=%q cache_dir=%q key=%q", h.Upstream, h.CacheDir, h.Key)
	}
	if h.StaticHeaders.Get("X-Token") != "static" || len(h.ForwardHeaders) != 1 || h.MaxIdleConns != 42 || h.BlockSize != 64*1024 || h.ChunkSize != 1024*1024 || h.CacheMaxSize != 5*1024*1024 || h.CacheMinFreeSpace != 6*1024*1024 || h.ShardLevel != 3 || h.ReadRetryCount != 2 {
		t.Fatalf("numeric/header full parse failed: headers=%+v max_idle=%d block=%d chunk=%d max_size=%d", h.StaticHeaders, h.MaxIdleConns, h.BlockSize, h.ChunkSize, h.CacheMaxSize)
	}
	if time.Duration(h.Timeout) != 5*time.Second || time.Duration(h.ProbeTimeout) != 4*time.Second || time.Duration(h.ReadRetryDelay) != 11*time.Millisecond {
		t.Fatalf("duration full parse failed timeout=%v probe=%v retry=%v", h.Timeout, h.ProbeTimeout, h.ReadRetryDelay)
	}

	zapPrintfLogger{}.Debugf("debug %d", 1)
	zapPrintfLogger{}.Infof("info %d", 1)
	zapPrintfLogger{}.Warnf("warn %d", 1)
	zapPrintfLogger{}.Errorf("error %d", 1)
	zapPrintfLogger{log: zap.NewNop()}.Debugf("debug %d", 1)
	zapPrintfLogger{log: zap.NewNop()}.Infof("info %d", 1)
	zapPrintfLogger{log: zap.NewNop()}.Warnf("warn %d", 1)
	zapPrintfLogger{log: zap.NewNop()}.Errorf("error %d", 1)

	if !(&Handler{StaleIfError: caddy.Duration(time.Second)}).canServeStale() || (&Handler{}).canServeStale() {
		t.Fatal("canServeStale failed")
	}
	if sanitizeMetricName("a-b.c") != "a_b_c" || sanitizeMetricName("!!!") != "___" || sanitizeMetricName("") != "unknown" {
		t.Fatal("sanitizeMetricName failed")
	}

	v, err, shared := (*flightGroup)(nil).do(context.Background(), "k", func() (any, error) { return "ok", nil })
	if err != nil || v != "ok" || shared {
		t.Fatalf("nil flight do v=%v err=%v shared=%v", v, err, shared)
	}
	g := newFlightGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _, _ = g.do(context.Background(), "k", func() (any, error) {
			close(started)
			<-release
			return "late", nil
		})
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err, shared := g.do(ctx, "k", func() (any, error) { return "never", nil }); !shared || !errors.Is(err, context.Canceled) {
		t.Fatalf("flight canceled shared=%v err=%v", shared, err)
	}
	close(release)
}

func TestHandlerAdminLoopbackBearerAndMethodEdges(t *testing.T) {
	h := newUnitHandler(t, "https://origin.example")
	h.AdminPath = "/_varc"
	remote := httptest.NewRequest(http.MethodGet, "https://edge.test/_varc", nil)
	remote.RemoteAddr = "203.0.113.5:1234"
	if err := h.ServeHTTP(httptest.NewRecorder(), remote, failNext(t)); err == nil {
		t.Fatal("expected remote admin without allow to fail")
	}
	h.AdminAllowRemote = true
	badMethod := httptest.NewRequest(http.MethodDelete, "https://edge.test/_varc", nil)
	badMethod.RemoteAddr = "127.0.0.1:1"
	if err := h.ServeHTTP(httptest.NewRecorder(), badMethod, failNext(t)); err == nil {
		t.Fatal("expected admin method error")
	}
	h.AdminToken = "secret"
	bearer := httptest.NewRequest(http.MethodGet, "https://edge.test/_varc?action=metrics", nil)
	bearer.RemoteAddr = "203.0.113.5:1234"
	bearer.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, bearer, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rr.Body.String(), "varc_handler_requests") {
		t.Fatalf("metrics body=%s", rr.Body.String())
	}
}
