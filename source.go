package varc

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// RemoteObject describes one byte-addressable upstream object.
type RemoteObject struct {
	SourceURL    string
	Size         int64
	ContentType  string
	ETag         string
	LastModified time.Time
	AcceptRanges bool
	CacheControl string
	SetCookie    bool
}

// Fingerprint returns the varc content fingerprint used to invalidate stale
// ranges.  ETag is preferred because it normally changes when content changes.
func (r RemoteObject) Fingerprint() string {
	if strings.TrimSpace(r.ETag) != "" {
		return strings.TrimSpace(r.ETag)
	}
	if !r.LastModified.IsZero() {
		return r.LastModified.UTC().Format(time.RFC3339Nano) + ":" + strconv.FormatInt(r.Size, 10)
	}
	return "size:" + strconv.FormatInt(r.Size, 10)
}

// HTTPRangeSource adapts an HTTP object supporting Range requests to
// io.ReaderAt.  varc calls this only on cache misses.
type HTTPRangeSource struct {
	Context      context.Context
	Client       *http.Client
	URL          string
	Headers      http.Header
	Logger       *zap.Logger
	ValidateSize int64
}

// ReadAt fetches p from the upstream using an HTTP Range request.
func (s *HTTPRangeSource) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	if s.ValidateSize >= 0 && off >= s.ValidateSize {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if s.ValidateSize >= 0 && end >= s.ValidateSize {
		end = s.ValidateSize - 1
		p = p[:end-off+1]
	}

	ctx := s.Context
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return 0, err
	}
	copyHeaders(req.Header, s.Headers)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("upstream range fetch %s returned %d: %s", s.URL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	cr := resp.Header.Get("Content-Range")
	start, gotEnd, total, ok := parseContentRange(cr)
	if !ok || start != off || gotEnd != end || (s.ValidateSize >= 0 && total != s.ValidateSize) {
		return 0, fmt.Errorf("upstream returned unexpected Content-Range %q for bytes=%d-%d size=%d", cr, off, end, s.ValidateSize)
	}
	n, readErr := io.ReadFull(resp.Body, p)
	if readErr != nil {
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			return n, io.ErrUnexpectedEOF
		}
		return n, readErr
	}
	if int64(n) != end-off+1 {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func (h *Handler) probeRemote(ctx context.Context, r *http.Request, sourceURL string) (RemoteObject, error) {
	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(h.ProbeTimeout))
	defer cancel()

	headers := h.originHeaders(r)
	remote, err := h.probeHEAD(probeCtx, sourceURL, headers)
	if err == nil && remote.Size >= 0 {
		remote.SourceURL = sourceURL
		return remote, nil
	}
	h.logger.Debug("varc HEAD probe failed; falling back to range probe", zap.String("url", sourceURL), zap.Error(err))
	remote, err = h.probeRange(probeCtx, sourceURL, headers)
	if err != nil {
		return RemoteObject{}, err
	}
	remote.SourceURL = sourceURL
	return remote, nil
}

func (h *Handler) probeHEAD(ctx context.Context, sourceURL string, headers http.Header) (RemoteObject, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, sourceURL, nil)
	if err != nil {
		return RemoteObject{}, err
	}
	copyHeaders(req.Header, headers)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := h.client.Do(req)
	if err != nil {
		return RemoteObject{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RemoteObject{}, fmt.Errorf("upstream HEAD returned %d", resp.StatusCode)
	}
	size := resp.ContentLength
	if size < 0 {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if parsed, parseErr := strconv.ParseInt(cl, 10, 64); parseErr == nil {
				size = parsed
			}
		}
	}
	remote := remoteFromHeaders(resp.Header, size)
	return remote, nil
}

func (h *Handler) probeRange(ctx context.Context, sourceURL string, headers http.Header) (RemoteObject, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return RemoteObject{}, err
	}
	copyHeaders(req.Header, headers)
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := h.client.Do(req)
	if err != nil {
		return RemoteObject{}, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	if resp.StatusCode != http.StatusPartialContent {
		return RemoteObject{}, fmt.Errorf("upstream range probe returned %d", resp.StatusCode)
	}
	_, _, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok || total < 0 {
		return RemoteObject{}, fmt.Errorf("upstream range probe missing valid Content-Range")
	}
	remote := remoteFromHeaders(resp.Header, total)
	return remote, nil
}

func remoteFromHeaders(h http.Header, size int64) RemoteObject {
	ct := h.Get("Content-Type")
	if mediaType, _, err := mime.ParseMediaType(ct); err == nil {
		ct = mediaType
	}
	etag := strings.TrimSpace(h.Get("ETag"))
	lastMod := time.Time{}
	if raw := h.Get("Last-Modified"); raw != "" {
		if parsed, err := http.ParseTime(raw); err == nil {
			lastMod = parsed
		}
	}
	return RemoteObject{
		Size:         size,
		ContentType:  ct,
		ETag:         normalizeETag(etag),
		LastModified: lastMod,
		AcceptRanges: strings.Contains(strings.ToLower(h.Get("Accept-Ranges")), "bytes"),
		CacheControl: strings.TrimSpace(h.Get("Cache-Control")),
		SetCookie:    len(h.Values("Set-Cookie")) > 0,
	}
}

func copyHeaders(dst, src http.Header) {
	for k, values := range src {
		if strings.EqualFold(k, "Range") || strings.EqualFold(k, "If-Range") || strings.EqualFold(k, "Accept-Encoding") {
			continue
		}
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func normalizeETag(etag string) string {
	etag = strings.TrimSpace(etag)
	if etag == "" {
		return ""
	}
	if strings.HasPrefix(etag, "W/\"") || strings.HasPrefix(etag, "\"") {
		return etag
	}
	return strconv.Quote(etag)
}

func formatHTTPTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(http.TimeFormat)
}
