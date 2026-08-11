package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tgdrive/varc/cache"
	httpsource "github.com/tgdrive/varc/source/http"
)

type upstreamFixture struct {
	mu        sync.Mutex
	data      string
	etag      string
	rangeGET  int
	head      int
	headAuth  []string
	rangeAuth []string
}

func (u *upstreamFixture) handler(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	data, etag := u.data, u.etag
	if r.Method == http.MethodHead {
		u.head++
		u.headAuth = append(u.headAuth, r.Header.Get("Authorization"))
	}
	u.mu.Unlock()

	if r.URL.Path == "/missing" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Last-Modified", time.Date(2026, time.August, 9, 9, 0, 0, 0, time.UTC).Format(http.TimeFormat))
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	rangeHeader := r.Header.Get("Range")
	var start, end int
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(data) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	u.mu.Lock()
	u.rangeGET++
	u.rangeAuth = append(u.rangeAuth, r.Header.Get("Authorization"))
	u.mu.Unlock()
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write([]byte(data[start : end+1]))
}

func (u *upstreamFixture) rangeCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.rangeGET
}

func (u *upstreamFixture) authHistory() (heads, ranges []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.headAuth...), append([]string(nil), u.rangeAuth...)
}

func newHTTPProxy(t *testing.T) (*Handler, *upstreamFixture) {
	t.Helper()
	fixture := &upstreamFixture{data: "0123456789", etag: `"v1"`}
	upstream := httptest.NewServer(http.HandlerFunc(fixture.handler))
	t.Cleanup(upstream.Close)

	src := httpsource.New(upstream.Client(), func(_ context.Context, key string) (string, error) {
		return upstream.URL + "/" + strings.TrimPrefix(key, "/"), nil
	})
	opt := cache.DefaultOptions()
	opt.CachePollInterval = 0
	opt.HandleCaching = 0
	c, err := cache.New(context.Background(), t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("cache Close: %v", err)
		}
	})
	return &Handler{Cache: c, Source: src}, fixture
}

func TestHandlerServesRangeAndThenCacheHitAcrossDifferentAuthHeaders(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	auth := []string{"Bearer first", "Bearer second"}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://proxy/movie", nil)
		req.Header.Set("Range", "bytes=2-5")
		req.Header.Set("Authorization", auth[i])
		req.Header.Set("Cookie", "session="+strconv.Itoa(i+1))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent {
			t.Fatalf("request %d status = %d, body=%q", i, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 2-5/10" {
			t.Fatalf("Content-Range = %q", got)
		}
		if got := rec.Header().Get("ETag"); got != `"v1"` {
			t.Fatalf("ETag = %q", got)
		}
		if got := rec.Body.String(); got != "2345" {
			t.Fatalf("body = %q", got)
		}
	}
	if got := upstream.rangeCount(); got != 1 {
		t.Fatalf("upstream range GETs = %d, want 1; auth headers must not vary the cache key", got)
	}
	heads, ranges := upstream.authHistory()
	if len(heads) != 2 || heads[0] != auth[0] || heads[1] != auth[1] {
		t.Fatalf("HEAD Authorization history = %v, want %v", heads, auth)
	}
	if len(ranges) != 1 || ranges[0] != auth[0] {
		t.Fatalf("range Authorization history = %v, want first request auth only", ranges)
	}
}

func TestHandlerServesFullGETThroughRangeSource(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodGet, "http://proxy/movie", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "0123456789" {
		t.Fatalf("body = %q", got)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("Accept-Ranges = %q", got)
	}
	if got := upstream.rangeCount(); got != 1 {
		t.Fatalf("upstream range GETs = %d, want 1", got)
	}
}

func TestHandlerHEADDoesNotFetchBody(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodHead, "http://proxy/movie", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body = %q", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != "10" {
		t.Fatalf("Content-Length = %q", got)
	}
	if got := upstream.rangeCount(); got != 0 {
		t.Fatalf("upstream range GETs = %d, want 0", got)
	}
}

func TestHandlerConditionalETagAvoidsBodyFetch(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodGet, "http://proxy/movie", nil)
	req.Header.Set("If-None-Match", `"v1"`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
	if got := upstream.rangeCount(); got != 0 {
		t.Fatalf("upstream range GETs = %d, want 0", got)
	}
}

func TestHandlerInvalidRangeReturns416WithoutFetch(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodGet, "http://proxy/movie", nil)
	req.Header.Set("Range", "bytes=99-100")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
	if got := upstream.rangeCount(); got != 0 {
		t.Fatalf("upstream range GETs = %d, want 0", got)
	}
}

func TestHandlerNotFound(t *testing.T) {
	h, _ := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodGet, "http://proxy/missing", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerRejectsMutationMethods(t *testing.T) {
	h, upstream := newHTTPProxy(t)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/movie", strings.NewReader("x"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow = %q", got)
	}
	if got := upstream.rangeCount(); got != 0 {
		t.Fatalf("upstream range GETs = %d, want 0", got)
	}
}
