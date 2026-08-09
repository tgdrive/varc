// Package httpsource implements source.Source using HTTP range requests.
package httpsource

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tgdrive/varc/source"
)

// Resolver maps a cache key to an upstream URL.
type Resolver func(ctx context.Context, key string) (string, error)

// Source reads immutable object ranges over HTTP.
type Source struct {
	Client  *http.Client
	Resolve Resolver
	Header  http.Header
}

// New constructs an HTTP source. A nil client uses http.DefaultClient.
func New(client *http.Client, resolve Resolver) *Source {
	if client == nil {
		client = http.DefaultClient
	}
	return &Source{Client: client, Resolve: resolve}
}

func (s *Source) resolve(ctx context.Context, key string) (string, error) {
	if s.Resolve == nil {
		return "", fmt.Errorf("http source: nil resolver")
	}
	upstream, err := s.Resolve(ctx, key)
	if err != nil {
		return "", fmt.Errorf("http source: resolve %q: %w", key, err)
	}
	if upstream == "" {
		return "", fmt.Errorf("http source: resolve %q returned empty URL", key)
	}
	return upstream, nil
}

func (s *Source) request(ctx context.Context, method, upstream string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, upstream, nil)
	if err != nil {
		return nil, err
	}
	applyHeaders(req, source.RequestHeaders(ctx))
	applyHeaders(req, s.Header)
	return req, nil
}

func applyHeaders(req *http.Request, headers map[string][]string) {
	for key, values := range headers {
		if strings.EqualFold(key, "Host") {
			req.Host = ""
			if len(values) != 0 {
				req.Host = values[0]
			}
			continue
		}
		req.Header[key] = append([]string(nil), values...)
	}
}

func clearClientRepresentationConditions(req *http.Request) {
	for _, key := range []string{
		"Range",
		"If-Range",
		"If-Match",
		"If-None-Match",
		"If-Modified-Since",
		"If-Unmodified-Since",
	} {
		req.Header.Del(key)
	}
}

// Stat obtains size and validators for key. HEAD is preferred, with a
// bytes=0-0 GET fallback for servers which do not implement useful HEADs.
func (s *Source) Stat(ctx context.Context, key string) (source.Metadata, error) {
	upstream, err := s.resolve(ctx, key)
	if err != nil {
		return source.Metadata{}, err
	}

	meta, fallback, err := s.statHEAD(ctx, upstream)
	if err != nil {
		return source.Metadata{}, err
	}
	if !fallback {
		return meta, nil
	}
	return s.statRange(ctx, upstream)
}

func (s *Source) statHEAD(ctx context.Context, upstream string) (meta source.Metadata, fallback bool, err error) {
	req, err := s.request(ctx, http.MethodHead, upstream)
	if err != nil {
		return meta, false, fmt.Errorf("http source: create HEAD: %w", err)
	}
	clearClientRepresentationConditions(req)
	resp, err := s.Client.Do(req)
	if err != nil {
		return meta, false, fmt.Errorf("http source: HEAD: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return meta, false, source.ErrNotFound
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return meta, true, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return meta, false, fmt.Errorf("http source: HEAD returned %s", resp.Status)
	}
	if resp.ContentLength < 0 {
		return meta, true, nil
	}
	return metadataFromResponse(resp, resp.ContentLength), false, nil
}

func (s *Source) statRange(ctx context.Context, upstream string) (source.Metadata, error) {
	req, err := s.request(ctx, http.MethodGet, upstream)
	if err != nil {
		return source.Metadata{}, fmt.Errorf("http source: create stat range request: %w", err)
	}
	clearClientRepresentationConditions(req)
	req.Header.Set("Range", "bytes=0-0")

	resp, err := s.Client.Do(req)
	if err != nil {
		return source.Metadata{}, fmt.Errorf("http source: stat range request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return source.Metadata{}, source.ErrNotFound
	case http.StatusRequestedRangeNotSatisfiable:
		if size, ok := parseUnsatisfiedSize(resp.Header.Get("Content-Range")); ok && size == 0 {
			return metadataFromResponse(resp, 0), nil
		}
		return source.Metadata{}, source.ErrRangeNotSatisfiable
	case http.StatusPartialContent:
		start, end, size, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || start != 0 || end != 0 || size < 1 {
			return source.Metadata{}, fmt.Errorf("http source: invalid stat Content-Range %q", resp.Header.Get("Content-Range"))
		}
		return metadataFromResponse(resp, size), nil
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			return metadataFromResponse(resp, resp.ContentLength), nil
		}
		return source.Metadata{}, fmt.Errorf("http source: stat response has unknown size")
	default:
		return source.Metadata{}, fmt.Errorf("http source: stat range returned %s", resp.Status)
	}
}

// OpenRange opens exactly the half-open range [start, end). The response is
// rejected if the upstream no longer matches expected metadata.
func (s *Source) OpenRange(ctx context.Context, key string, start, end int64, expected source.Metadata) (io.ReadCloser, error) {
	if start < 0 || end <= start {
		return nil, fmt.Errorf("http source: invalid range [%d,%d)", start, end)
	}
	if expected.Size >= 0 && end > expected.Size {
		return nil, fmt.Errorf("%w: [%d,%d) exceeds size %d", source.ErrRangeNotSatisfiable, start, end, expected.Size)
	}

	upstream, err := s.resolve(ctx, key)
	if err != nil {
		return nil, err
	}
	req, err := s.request(ctx, http.MethodGet, upstream)
	if err != nil {
		return nil, fmt.Errorf("http source: create range request: %w", err)
	}
	clearClientRepresentationConditions(req)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	if validator := ifRangeValidator(expected); validator != "" {
		req.Header.Set("If-Range", validator)
	}

	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http source: range request: %w", err)
	}

	fail := func(err error) (io.ReadCloser, error) {
		resp.Body.Close()
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		return fail(source.ErrNotFound)
	case http.StatusRequestedRangeNotSatisfiable:
		return fail(source.ErrRangeNotSatisfiable)
	case http.StatusOK:
		if req.Header.Get("If-Range") != "" {
			return fail(fmt.Errorf("%w: upstream returned full response to If-Range request", source.ErrObjectChanged))
		}
		return fail(source.ErrRangeUnsupported)
	case http.StatusPartialContent:
		// continue below
	default:
		return fail(fmt.Errorf("http source: range request returned %s", resp.Status))
	}

	gotStart, gotEnd, gotSize, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok || gotStart != start || gotEnd != end-1 {
		return fail(fmt.Errorf("http source: unexpected Content-Range %q for [%d,%d)", resp.Header.Get("Content-Range"), start, end))
	}
	if expected.Size >= 0 && gotSize != expected.Size {
		return fail(fmt.Errorf("%w: size changed from %d to %d", source.ErrObjectChanged, expected.Size, gotSize))
	}
	if resp.ContentLength >= 0 && resp.ContentLength != end-start {
		return fail(fmt.Errorf("http source: range Content-Length is %d, want %d", resp.ContentLength, end-start))
	}
	if expected.ETag != "" {
		if got := resp.Header.Get("ETag"); got != "" && got != expected.ETag {
			return fail(fmt.Errorf("%w: ETag changed from %q to %q", source.ErrObjectChanged, expected.ETag, got))
		}
	}
	if !expected.LastModified.IsZero() {
		if value := resp.Header.Get("Last-Modified"); value != "" {
			if got, parseErr := http.ParseTime(value); parseErr == nil && !got.Equal(expected.LastModified) {
				return fail(fmt.Errorf("%w: Last-Modified changed from %s to %s", source.ErrObjectChanged, expected.LastModified, got))
			}
		}
	}
	return resp.Body, nil
}

func metadataFromResponse(resp *http.Response, size int64) source.Metadata {
	meta := source.Metadata{
		Size:        size,
		ETag:        resp.Header.Get("ETag"),
		ContentType: resp.Header.Get("Content-Type"),
	}
	if value := resp.Header.Get("Last-Modified"); value != "" {
		if parsed, err := http.ParseTime(value); err == nil {
			meta.LastModified = parsed
		}
	}
	return meta
}

func ifRangeValidator(meta source.Metadata) string {
	etag := strings.TrimSpace(meta.ETag)
	if etag != "" && !strings.HasPrefix(etag, "W/") {
		return etag
	}
	if !meta.LastModified.IsZero() {
		return meta.LastModified.UTC().Format(http.TimeFormat)
	}
	return ""
}

func parseContentRange(value string) (start, end, size int64, ok bool) {
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, false
	}
	rangePart, sizePart, found := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	if !found || sizePart == "*" {
		return 0, 0, 0, false
	}
	startPart, endPart, found := strings.Cut(rangePart, "-")
	if !found {
		return 0, 0, 0, false
	}
	start, err1 := strconv.ParseInt(startPart, 10, 64)
	end, err2 := strconv.ParseInt(endPart, 10, 64)
	size, err3 := strconv.ParseInt(sizePart, 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || start < 0 || end < start || size <= end {
		return 0, 0, 0, false
	}
	return start, end, size, true
}

func parseUnsatisfiedSize(value string) (int64, bool) {
	if !strings.HasPrefix(value, "bytes */") {
		return 0, false
	}
	size, err := strconv.ParseInt(strings.TrimPrefix(value, "bytes */"), 10, 64)
	return size, err == nil && size >= 0
}

var _ source.Source = (*Source)(nil)
