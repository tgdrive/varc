package varc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func (h *Handler) requestBypassReason(r *http.Request) string {
	if !h.CacheAuthorization && strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		return "authorization"
	}
	for _, name := range h.BypassHeaders {
		if strings.TrimSpace(name) == "" {
			continue
		}
		if r.Header.Get(name) != "" {
			return "header:" + http.CanonicalHeaderKey(name)
		}
	}
	for _, name := range h.BypassCookies {
		if strings.TrimSpace(name) == "" {
			continue
		}
		if _, err := r.Cookie(name); err == nil {
			return "cookie:" + name
		}
	}
	q := r.URL.Query()
	for _, name := range h.BypassQuery {
		if strings.TrimSpace(name) == "" {
			continue
		}
		if _, ok := q[name]; ok {
			return "query:" + name
		}
	}
	return ""
}

func (h *Handler) responseBypassReason(remote RemoteObject) string {
	if remote.SetCookie && !h.CacheSetCookie {
		return "set-cookie"
	}
	cc := parseCacheControl(remote.CacheControl)
	if cc["no-store"] && !h.CacheNoStore {
		return "cache-control:no-store"
	}
	if cc["private"] && !h.CachePrivate {
		return "cache-control:private"
	}
	return ""
}

func (h *Handler) canServeStale() bool {
	return time.Duration(h.StaleIfError) > 0
}

func (h *Handler) shouldRevalidate(key string) bool {
	interval := time.Duration(h.RevalidateInterval)
	if interval <= 0 {
		return false
	}
	h.validationMu.Lock()
	last := h.validatedAt[key]
	h.validationMu.Unlock()
	return last.IsZero() || time.Since(last) >= interval
}

func (h *Handler) markValidated(key string) {
	h.validationMu.Lock()
	if h.validatedAt == nil {
		h.validatedAt = make(map[string]time.Time)
	}
	h.validatedAt[key] = time.Now()
	h.validationMu.Unlock()
}

func (h *Handler) canServeStaleKey(key string) bool {
	maxAge := time.Duration(h.StaleIfError)
	if maxAge <= 0 {
		return false
	}
	h.validationMu.Lock()
	last := h.validatedAt[key]
	h.validationMu.Unlock()
	return !last.IsZero() && time.Since(last) <= maxAge
}

func parseCacheControl(raw string) map[string]bool {
	out := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		if i := strings.IndexByte(part, '='); i >= 0 {
			part = strings.TrimSpace(part[:i])
		}
		out[part] = true
	}
	return out
}

func (h *Handler) proxyBypass(w http.ResponseWriter, r *http.Request, sourceURL, key, reason string) error {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, sourceURL, nil)
	if err != nil {
		h.metrics.errors.Add(1)
		return err
	}
	copyHeaders(req.Header, h.originHeaders(r))
	for _, name := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since"} {
		for _, value := range r.Header.Values(name) {
			req.Header.Add(name, value)
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.metrics.errors.Add(1)
		return err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	if h.DebugHeaders {
		w.Header().Set("X-Varc-Cache", "BYPASS")
		w.Header().Set("X-Varc-Bypass", reason)
		w.Header().Set("X-Varc-Key", key)
		w.Header().Set("X-Varc-Source", sourceURL)
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return nil
	}
	n, copyErr := copyWithIdleTimeout(ctx, cancel, w, resp.Body, time.Duration(h.Timeout))
	h.metrics.bytesServed.Add(n)
	h.metrics.bytesFromOrigin.Add(n)
	if copyErr != nil {
		h.metrics.errors.Add(1)
		return fmt.Errorf("varc bypass stream: %w", copyErr)
	}
	return nil
}

type idleProgressReader struct {
	reader   io.Reader
	progress chan<- struct{}
}

func (r idleProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		select {
		case r.progress <- struct{}{}:
		default:
		}
	}
	return n, err
}

func copyWithIdleTimeout(ctx context.Context, cancel context.CancelFunc, dst io.Writer, src io.Reader, timeout time.Duration) (int64, error) {
	if timeout <= 0 {
		return io.CopyBuffer(dst, src, make([]byte, defaultResponseBuffer))
	}
	progress := make(chan struct{}, 1)
	stopped := make(chan struct{})
	timedOut := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(stopped)
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-progress:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				close(timedOut)
				cancel()
				return
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	n, err := io.CopyBuffer(dst, idleProgressReader{reader: src, progress: progress}, make([]byte, defaultResponseBuffer))
	close(stop)
	<-stopped
	select {
	case <-timedOut:
		return n, fmt.Errorf("varc: bypass source read idle for %s: %w", timeout, context.DeadlineExceeded)
	default:
	}
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return n, ctx.Err()
	}
	return n, err
}

func copyResponseHeaders(dst, src http.Header) {
	for k, values := range src {
		if hopByHopHeader(k) {
			continue
		}
		dst.Del(k)
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func hopByHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}
