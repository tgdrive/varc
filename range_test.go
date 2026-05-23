package varc

import (
	"net/http"
	"testing"
	"time"
)

func TestParseSingleRange(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		size    int64
		start   int64
		end     int64
		partial bool
		wantErr bool
	}{
		{name: "full", size: 100, start: 0, end: 100},
		{name: "closed", raw: "bytes=10-19", size: 100, start: 10, end: 20, partial: true},
		{name: "open", raw: "bytes=10-", size: 100, start: 10, end: 100, partial: true},
		{name: "suffix", raw: "bytes=-10", size: 100, start: 90, end: 100, partial: true},
		{name: "suffix larger", raw: "bytes=-999", size: 100, start: 0, end: 100, partial: true},
		{name: "clamped", raw: "bytes=90-999", size: 100, start: 90, end: 100, partial: true},
		{name: "multi", raw: "bytes=1-2,3-4", size: 100, wantErr: true},
		{name: "bad unit", raw: "items=1-2", size: 100, wantErr: true},
		{name: "past eof", raw: "bytes=100-101", size: 100, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSingleRange(tt.raw, tt.size)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Start != tt.start || got.End != tt.end || got.Partial != tt.partial {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestParseContentRange(t *testing.T) {
	start, end, total, ok := parseContentRange("bytes 10-19/100")
	if !ok || start != 10 || end != 19 || total != 100 {
		t.Fatalf("bad parse: %d %d %d %v", start, end, total, ok)
	}
	_, _, total, ok = parseContentRange("bytes 0-0/*")
	if !ok || total != -1 {
		t.Fatalf("bad unknown total parse: %d %v", total, ok)
	}
}

func TestIfRange(t *testing.T) {
	mod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("If-Range", "\"abc\"")
	if !rangeAllowedByIfRange(r, "\"abc\"", mod) {
		t.Fatal("etag match should allow range")
	}
	if rangeAllowedByIfRange(r, "\"def\"", mod) {
		t.Fatal("etag mismatch should reject range")
	}
	r.Header.Set("If-Range", mod.UTC().Format(http.TimeFormat))
	if !rangeAllowedByIfRange(r, "", mod) {
		t.Fatal("date match should allow range")
	}
}

func TestIsNotModified(t *testing.T) {
	mod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("If-None-Match", "\"abc\"")
	if !isNotModified(r, "\"abc\"", mod) {
		t.Fatal("etag should be not modified")
	}
	r.Header.Del("If-None-Match")
	r.Header.Set("If-Modified-Since", mod.UTC().Format(http.TimeFormat))
	if !isNotModified(r, "", mod) {
		t.Fatal("date should be not modified")
	}
}
