package varc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corevarc "github.com/tgdrive/varc/varc"
)

func TestHTTPRangeTransientUnauthorizedRetries(t *testing.T) {
	data := bytes.Repeat([]byte("auth-retry-"), 64)
	var attempts atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("expired token"))
			return
		}
		span, err := parseSingleRange(r.Header.Get("Range"), int64(len(data)))
		if err != nil {
			writeRangeNotSatisfiable(w, int64(len(data)))
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[span.Start:span.End])
	}))
	defer origin.Close()

	opt := corevarc.DefaultOptions()
	opt.CacheDir = t.TempDir()
	opt.ChunkSize = int64(len(data))
	opt.ChunkSizeLimit = opt.ChunkSize
	opt.PreloadChunks = 0
	opt.NoBackground = true
	opt.ReadRetryCount = 1
	opt.ReadRetryDelay = time.Millisecond
	c, err := corevarc.New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := &HTTPRangeSource{URL: origin.URL, ValidateSize: int64(len(data))}
	r, err := c.Open(context.Background(), "auth-retry", int64(len(data)), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, len(data))
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data) || attempts.Load() != 2 {
		t.Fatalf("data/retry mismatch attempts=%d", attempts.Load())
	}
}
