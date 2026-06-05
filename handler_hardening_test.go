package varc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	corevarc "github.com/tgdrive/varc/varc"
	"go.uber.org/zap"
)

func TestHandlerBypassesAuthorizationByDefault(t *testing.T) {
	body := []byte("abcdefghijklmnopqrstuvwxyz")
	var gets atomic.Int64
	origin := newRangeOrigin(t, body, nil, &gets)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true

	req := httptest.NewRequest(http.MethodGet, "https://edge.test/file.txt", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("X-Varc-Cache"); got != "BYPASS" {
		t.Fatalf("X-Varc-Cache = %q", got)
	}
	if rr.Body.String() != string(body) {
		t.Fatalf("body = %q", rr.Body.String())
	}
	entries, err := h.cache.ListEntries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("authorization bypass should not populate cache, entries=%d", len(entries))
	}
	if gets.Load() != 1 {
		t.Fatalf("origin gets = %d", gets.Load())
	}
}

func TestHandlerSetCookieAndNoStoreBypass(t *testing.T) {
	body := []byte("payload")
	originHeaders := http.Header{}
	originHeaders.Set("Set-Cookie", "sid=private; Path=/")
	originHeaders.Set("Cache-Control", "no-store")
	origin := newRangeOrigin(t, body, originHeaders, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true

	req := httptest.NewRequest(http.MethodGet, "https://edge.test/private.bin", nil)
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if got := rr.Header().Get("X-Varc-Cache"); got != "BYPASS" {
		t.Fatalf("cache = %q", got)
	}
	entries, _ := h.cache.ListEntries(context.Background())
	if len(entries) != 0 {
		t.Fatalf("unsafe response should not populate cache, entries=%d", len(entries))
	}
}

func TestHandlerStaleIfError(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 64))
	origin := newRangeOrigin(t, body, nil, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.DebugHeaders = true
	h.StaleIfError = caddyDuration(time.Hour)

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "https://edge.test/movie.mp4", nil)
	if err := h.ServeHTTP(first, firstReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if first.Code != http.StatusOK || first.Body.Len() != len(body) {
		t.Fatalf("initial fill status=%d len=%d", first.Code, first.Body.Len())
	}

	h.client = &http.Client{Transport: errRoundTripper{err: fmt.Errorf("origin down")}}
	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "https://edge.test/movie.mp4", nil)
	if err := h.ServeHTTP(second, secondReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if second.Code != http.StatusOK {
		t.Fatalf("stale status = %d body=%q", second.Code, second.Body.String())
	}
	if got := second.Header().Get("X-Varc-Cache"); got != "HIT" && got != "STALE" {
		t.Fatalf("cache status = %q", got)
	}
	if !bytes.Equal(second.Body.Bytes(), body) {
		t.Fatal("stale body mismatch")
	}
}

func TestHandlerAdminPinObjectPurgeAndMetrics(t *testing.T) {
	body := []byte(strings.Repeat("x", 256))
	origin := newRangeOrigin(t, body, nil, nil)
	defer origin.Close()
	h := newUnitHandler(t, origin.URL)
	h.AdminPath = "/_varc"
	h.AdminAllowRemote = true
	h.DebugHeaders = true

	fill := httptest.NewRecorder()
	fillReq := httptest.NewRequest(http.MethodGet, "https://edge.test/a.bin", nil)
	if err := h.ServeHTTP(fill, fillReq, failNext(t)); err != nil {
		t.Fatal(err)
	}
	key := origin.URL + "/a.bin"

	postAdmin(t, h, "/_varc?action=pin&key="+urlQueryEscape(key))
	pinned, err := h.cache.IsPinned(key)
	if err != nil || !pinned {
		t.Fatalf("pinned=%v err=%v", pinned, err)
	}
	obj := getAdmin(t, h, "/_varc/object?key="+urlQueryEscape(key))
	if !strings.Contains(obj, "coverage") || !strings.Contains(obj, "plan") {
		t.Fatalf("object payload missing plan/coverage: %s", obj)
	}
	metrics := getAdmin(t, h, "/_varc/metrics")
	if !strings.Contains(metrics, "varc_handler_requests") {
		t.Fatalf("metrics missing handler counter: %s", metrics)
	}
	postAdmin(t, h, "/_varc?action=unpin&key="+urlQueryEscape(key))
	pinned, _ = h.cache.IsPinned(key)
	if pinned {
		t.Fatal("expected unpinned")
	}
	postAdmin(t, h, "/_varc?action=purge&key="+urlQueryEscape(key))
	if h.cache.Exists(key) {
		t.Fatal("expected purged entry")
	}
}

func TestHandlerKeyNormalizationAndVaryHeaders(t *testing.T) {
	h := &Handler{
		Upstream:      "https://ORIGIN.example/base",
		StripQuery:    []string{"utm_source"},
		SortQuery:     true,
		LowercaseHost: true,
		VaryHeaders:   []string{"Accept-Language"},
	}
	req := httptest.NewRequest(http.MethodGet, "https://edge.test/video.mp4?b=2&utm_source=x&a=1", nil)
	req.Header.Set("Accept-Language", "en-IN")
	repl := replacerFromRequest(req)
	source, err := h.resolveSourceURL(repl, req)
	if err != nil {
		t.Fatal(err)
	}
	if source != "https://origin.example/base/video.mp4?a=1&b=2" {
		t.Fatalf("normalized source = %q", source)
	}
	key := h.cacheKey(repl, req, source)
	if !strings.Contains(key, "Accept-Language=en-IN") {
		t.Fatalf("vary header missing from key: %q", key)
	}
}

func TestHandlerCaddyfileNewOptions(t *testing.T) {
	d := caddyfileTestDispenser(`varc https://origin.example {
	strip_query utm_source fbclid
	sort_query on
	lowercase_host on
	vary_header Accept-Language
	bypass_header X-No-Cache
	bypass_cookie session
	bypass_query nocache
	cache_authorization on
	cache_set_cookie on
	cache_private on
	cache_no_store on
	stale_if_error 30m
	admin_path /_varc
	admin_token secret
	admin_allow_remote on
}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if !h.SortQuery || !h.LowercaseHost || !h.CacheAuthorization || !h.CacheSetCookie || !h.CachePrivate || !h.CacheNoStore || !h.AdminAllowRemote {
		t.Fatalf("bool options not parsed")
	}
	if len(h.StripQuery) != 2 || h.VaryHeaders[0] != "Accept-Language" || h.BypassHeaders[0] != "X-No-Cache" || h.BypassCookies[0] != "session" || h.BypassQuery[0] != "nocache" {
		t.Fatalf("list options not parsed")
	}
	if h.AdminToken != "secret" || h.AdminPath != "/_varc" || time.Duration(h.StaleIfError) != 30*time.Minute {
		t.Fatalf("admin/stale options not parsed")
	}
}

func newUnitHandler(t *testing.T, upstream string) *Handler {
	t.Helper()
	opt := corevarc.DefaultOptions()
	opt.CacheDir = t.TempDir()
	opt.BlockSize = 32
	opt.ChunkSize = 64
	cache, err := corevarc.New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return &Handler{
		Upstream:     upstream,
		CacheDir:     opt.CacheDir,
		ProbeTimeout: caddy.Duration(5 * time.Second),
		StaleIfError: 0,
		cache:        cache,
		client:       http.DefaultClient,
		logger:       zap.NewNop(),
		flights:      newFlightGroup(),
	}
}

func newRangeOrigin(t *testing.T, body []byte, extra http.Header, gets *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, vals := range extra {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if gets != nil {
			gets.Add(1)
		}
		span, err := parseSingleRange(r.Header.Get("Range"), int64(len(body)))
		if err != nil {
			writeRangeNotSatisfiable(w, int64(len(body)))
			return
		}
		if span.Partial {
			w.Header().Set("Content-Length", fmt.Sprint(span.Length()))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[span.Start:span.End])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body)
	}))
}

func failNext(t *testing.T) caddyhttp.Handler {
	t.Helper()
	return caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
		t.Fatal("next handler should not be called")
		return nil
	})
}

type errRoundTripper struct{ err error }

func (e errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return nil, e.err }

func postAdmin(t *testing.T, h *Handler, target string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "https://edge.test"+target, nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Code < 200 || rr.Code >= 300 {
		t.Fatalf("admin POST %s status=%d body=%s", target, rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func getAdmin(t *testing.T, h *Handler, target string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "https://edge.test"+target, nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rr := httptest.NewRecorder()
	if err := h.ServeHTTP(rr, req, failNext(t)); err != nil {
		t.Fatal(err)
	}
	if rr.Code < 200 || rr.Code >= 300 {
		t.Fatalf("admin GET %s status=%d body=%s", target, rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func urlQueryEscape(s string) string {
	r := strings.NewReplacer("%", "%25", " ", "%20", "?", "%3F", "&", "%26", "=", "%3D", "#", "%23")
	return r.Replace(s)
}

func caddyDuration(d time.Duration) caddy.Duration { return caddy.Duration(d) }

func caddyfileTestDispenser(input string) *caddyfile.Dispenser {
	return caddyfile.NewTestDispenser(input)
}
