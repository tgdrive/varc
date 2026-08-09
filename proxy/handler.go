// Package proxy adapts the cache package to net/http. It intentionally has no
// Caddy dependency so the same core can be embedded in any Go HTTP server.
package proxy

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"

	"vfs-cache/cache"
	"vfs-cache/source"
)

// KeyFunc maps an incoming request to a source/cache key.
type KeyFunc func(*http.Request) (string, error)

// Handler serves source objects through Cache.
type Handler struct {
	Cache *cache.Cache
	Key   KeyFunc
}

// PathKey uses the cleaned URL path without a leading slash as the cache key.
func PathKey(r *http.Request) (string, error) {
	key := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if key == "." || key == "" {
		return "", errors.New("empty object path")
	}
	return key, nil
}

// ServeHTTP serves GET and HEAD with standard library range and conditional
// request semantics. Source reads are performed lazily by http.ServeContent.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Cache == nil {
		http.Error(w, "cache proxy is not configured", http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keyFunc := h.Key
	if keyFunc == nil {
		keyFunc = PathKey
	}
	key, err := keyFunc(r)
	if err != nil {
		http.Error(w, "invalid object key", http.StatusBadRequest)
		return
	}

	headers := r.Header.Clone()
	if r.Host != "" {
		headers.Set("Host", r.Host)
	}
	ctx := source.WithRequestHeaders(r.Context(), headers)
	reader, err := h.Cache.Open(ctx, key)
	if err != nil {
		h.serveError(w, err)
		return
	}
	defer reader.Close()

	meta := reader.Metadata()
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	if meta.ETag != "" {
		w.Header().Set("ETag", meta.ETag)
	}

	name := path.Base(key)
	content := &readSeeker{reader: reader, size: reader.Size()}
	http.ServeContent(w, r, name, meta.LastModified, content)
}

func (h *Handler) serveError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, source.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, source.ErrRangeNotSatisfiable):
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
	case errors.Is(err, source.ErrObjectChanged):
		// A request which races an upstream mutation is retryable by the client;
		// do not risk serving bytes from mixed object versions.
		http.Error(w, "upstream object changed", http.StatusPreconditionFailed)
	case errors.Is(err, source.ErrRangeUnsupported):
		http.Error(w, "upstream does not support ranges", http.StatusBadGateway)
	default:
		http.Error(w, "upstream cache error", http.StatusBadGateway)
	}
}

type readSeeker struct {
	reader *cache.Reader
	size   int64

	mu  sync.Mutex
	off int64
}

func (r *readSeeker) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.off >= r.size {
		return 0, io.EOF
	}
	n, err := r.reader.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}

func (r *readSeeker) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.off + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, fmt.Errorf("cache proxy: invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, errors.New("cache proxy: negative seek offset")
	}
	r.off = next
	return next, nil
}

var _ http.Handler = (*Handler)(nil)
var _ io.ReadSeeker = (*readSeeker)(nil)
