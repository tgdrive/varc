package httpsource

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vfs-cache/source"
)

func resolverFor(server *httptest.Server) Resolver {
	return func(context.Context, string) (string, error) { return server.URL + "/object", nil }
}

func TestStatHEAD(t *testing.T) {
	modified := time.Date(2026, time.August, 9, 8, 30, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "123")
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := New(server.Client(), resolverFor(server))
	meta, err := s.Stat(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 123 || meta.ETag != `"v1"` || meta.ContentType != "video/mp4" || !meta.LastModified.Equal(modified) {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
}

func TestStatFallsBackToRangeGET(t *testing.T) {
	var gets int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case http.MethodGet:
			gets++
			if got := r.Header.Get("Range"); got != "bytes=0-0" {
				t.Fatalf("Range = %q, want bytes=0-0", got)
			}
			w.Header().Set("Content-Range", "bytes 0-0/5")
			w.Header().Set("Content-Length", "1")
			w.Header().Set("ETag", `"fallback"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("h"))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	s := New(server.Client(), resolverFor(server))
	meta, err := s.Stat(context.Background(), "movie")
	if err != nil {
		t.Fatal(err)
	}
	if gets != 1 || meta.Size != 5 || meta.ETag != `"fallback"` {
		t.Fatalf("gets=%d metadata=%+v", gets, meta)
	}
}

func TestStatEmptyObjectFrom416(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Range", "bytes */0")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()

	meta, err := New(server.Client(), resolverFor(server)).Stat(context.Background(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 0 {
		t.Fatalf("size = %d, want 0", meta.Size)
	}
}

func TestOpenRangeValidatesAndUsesIfRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=2-5" {
			t.Fatalf("Range = %q, want bytes=2-5", got)
		}
		if got := r.Header.Get("If-Range"); got != `"v1"` {
			t.Fatalf("If-Range = %q, want %q", got, `"v1"`)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Range", "bytes 2-5/10")
		w.Header().Set("Content-Length", "4")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("2345"))
	}))
	defer server.Close()

	s := New(server.Client(), resolverFor(server))
	s.Header = http.Header{"Authorization": {"Bearer secret"}}
	rc, err := s.OpenRange(context.Background(), "movie", 2, 6, source.Metadata{Size: 10, ETag: `"v1"`})
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "2345" {
		t.Fatalf("body = %q, want 2345", got)
	}
}

func TestOpenRangeUsesLastModifiedForWeakETag(t *testing.T) {
	modified := time.Date(2026, time.August, 9, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-Range"); got != modified.Format(http.TimeFormat) {
			t.Fatalf("If-Range = %q, want Last-Modified", got)
		}
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	}))
	defer server.Close()

	rc, err := New(server.Client(), resolverFor(server)).OpenRange(context.Background(), "x", 0, 1, source.Metadata{
		Size:         1,
		ETag:         `W/"weak"`,
		LastModified: modified,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
}

func TestOpenRangeDetectsObjectChange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer server.Close()

	_, err := New(server.Client(), resolverFor(server)).OpenRange(context.Background(), "movie", 2, 6, source.Metadata{Size: 10, ETag: `"old"`})
	if !errors.Is(err, source.ErrObjectChanged) {
		t.Fatalf("error = %v, want ErrObjectChanged", err)
	}
}

func TestOpenRangeRejectsBadContentRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 3-6/10")
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("3456"))
	}))
	defer server.Close()

	_, err := New(server.Client(), resolverFor(server)).OpenRange(context.Background(), "movie", 2, 6, source.Metadata{Size: 10})
	if err == nil {
		t.Fatal("expected Content-Range validation error")
	}
}

func TestOpenRangeReportsUnsupportedRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer server.Close()

	_, err := New(server.Client(), resolverFor(server)).OpenRange(context.Background(), "movie", 2, 6, source.Metadata{Size: 10})
	if !errors.Is(err, source.ErrRangeUnsupported) {
		t.Fatalf("error = %v, want ErrRangeUnsupported", err)
	}
}

func TestStatNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := New(server.Client(), resolverFor(server)).Stat(context.Background(), "missing")
	if !errors.Is(err, source.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestOpenRangeDetectsLastModifiedChange(t *testing.T) {
	expectedTime := time.Date(2026, time.August, 9, 9, 0, 0, 0, time.UTC)
	changedTime := expectedTime.Add(time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.Header().Set("Content-Length", "1")
		w.Header().Set("Last-Modified", changedTime.Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	}))
	defer server.Close()

	_, err := New(server.Client(), resolverFor(server)).OpenRange(context.Background(), "x", 0, 1, source.Metadata{
		Size:         1,
		LastModified: expectedTime,
	})
	if !errors.Is(err, source.ErrObjectChanged) {
		t.Fatalf("error = %v, want ErrObjectChanged", err)
	}
}

func TestRequestHeadersForwardedAndCacheRangeHeadersOverrideClientValues(t *testing.T) {
	var headAuth, headCookie, headHost, getAuth, getCookie, getTenant, getHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headAuth = r.Header.Get("Authorization")
			headCookie = r.Header.Get("Cookie")
			headHost = r.Host
			if got := r.Header.Get("Range"); got != "" {
				t.Fatalf("HEAD Range = %q, want stripped client range", got)
			}
			if got := r.Header.Get("If-Range"); got != "" {
				t.Fatalf("HEAD If-Range = %q, want stripped client validator", got)
			}
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Fatalf("HEAD If-None-Match = %q, want stripped client condition", got)
			}
			w.Header().Set("Content-Length", "10")
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getAuth = r.Header.Get("Authorization")
			getCookie = r.Header.Get("Cookie")
			getTenant = r.Header.Get("X-Tenant")
			getHost = r.Host
			if got := r.Header.Get("Range"); got != "bytes=2-5" {
				t.Fatalf("Range = %q, want cache-generated bytes=2-5", got)
			}
			if got := r.Header.Get("If-Range"); got != `"v1"` {
				t.Fatalf("If-Range = %q, want cache-generated validator", got)
			}
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Fatalf("GET If-None-Match = %q, want stripped client condition", got)
			}
			w.Header().Set("Content-Range", "bytes 2-5/10")
			w.Header().Set("Content-Length", "4")
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("2345"))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	ctx := source.WithRequestHeaders(context.Background(), http.Header{
		"Authorization": {"Bearer request-token"},
		"Cookie":        {"session=abc"},
		"X-Tenant":      {"tenant-a"},
		"Range":         {"bytes=99-100"},
		"If-Range":      {`"client-validator"`},
		"If-None-Match": {`"downstream-condition"`},
		"Host":          {"client.example"},
	})
	s := New(server.Client(), resolverFor(server))
	meta, err := s.Stat(ctx, "movie")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := s.OpenRange(ctx, "movie", 2, 6, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	if headAuth != "Bearer request-token" || headCookie != "session=abc" || headHost != "client.example" {
		t.Fatalf("HEAD forwarded auth/cookie/host = %q / %q / %q", headAuth, headCookie, headHost)
	}
	if getAuth != "Bearer request-token" || getCookie != "session=abc" || getTenant != "tenant-a" || getHost != "client.example" {
		t.Fatalf("GET forwarded headers auth=%q cookie=%q tenant=%q host=%q", getAuth, getCookie, getTenant, getHost)
	}
}
