package varc

import (
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
	ctx := r.Context()
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
	n, copyErr := io.CopyBuffer(w, resp.Body, make([]byte, defaultResponseBuffer))
	h.metrics.bytesServed.Add(n)
	h.metrics.bytesFromOrigin.Add(n)
	if copyErr != nil {
		h.metrics.errors.Add(1)
		return fmt.Errorf("varc bypass stream: %w", copyErr)
	}
	return nil
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
