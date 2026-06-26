package varc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/caddyserver/caddy/v2"
)

func TestResolveSourceURL(t *testing.T) {
	on := true
	h := &Handler{Upstream: "https://origin.example.com/base", AppendURI: &on}
	r := httptest.NewRequest(http.MethodGet, "https://edge.example.com/video/a.mp4?token=1", nil)
	repl := caddy.NewReplacer()
	ctx := context.WithValue(r.Context(), caddy.ReplacerCtxKey, repl)
	r = r.WithContext(ctx)
	got, err := h.resolveSourceURL(repl, r)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://origin.example.com/base/video/a.mp4?token=1"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestResolveSourceURLPreservesEscapedPath(t *testing.T) {
	on := true
	h := &Handler{Upstream: "https://origin.example.com/base%2Froot", AppendURI: &on}
	r := httptest.NewRequest(http.MethodGet, "https://edge.example.com/video/a%2F%2Fb%20c", nil)
	repl := replacerFromRequest(r)

	got, err := h.resolveSourceURL(repl, r)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://origin.example.com/base%2Froot/video/a%2F%2Fb%20c"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestCacheKeyDefaultAndTemplate(t *testing.T) {
	h := &Handler{}
	r := httptest.NewRequest(http.MethodGet, "https://edge.example.com/a?b=1", nil)
	repl := replacerFromRequest(r)
	if got := h.cacheKey(repl, r, "https://origin/a?b=1"); got != "https://origin/a?b=1" {
		t.Fatalf("default key = %q", got)
	}
	h.Key = "{host}:{uri}"
	if got := h.cacheKey(repl, r, "ignored"); got != "edge.example.com:/a?b=1" {
		t.Fatalf("template key = %q", got)
	}
}
