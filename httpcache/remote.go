package httpcache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tgdrive/varc/internal/types"
)

// Compile-time check that remoteFile implements RemoteObject
var _ types.RemoteObject = (*remoteFile)(nil)

// remoteFile is an HTTP-backed RemoteObject used by the proxy
// to fetch files from upstream URLs through the disk cache.
type remoteFile struct {
	url     string
	headers http.Header
	size    int64
	modTime time.Time
	etag    string
	client  *http.Client
}

func newHTTPFile(url string, headers http.Header, size int64, modTime time.Time, etag string, client *http.Client) *remoteFile {
	return &remoteFile{
		url:     url,
		headers: headers,
		size:    size,
		modTime: modTime,
		etag:    etag,
		client:  client,
	}
}

// String returns the upstream URL
func (f *remoteFile) String() string {
	return f.url
}

// Size returns the file size, or -1 if unknown
func (f *remoteFile) Size() int64 {
	return f.size
}

// Fingerprint returns a stable validator for cache invalidation when upstream provides one.
func (f *remoteFile) Fingerprint() string {
	if f.etag != "" {
		return f.etag
	}
	if !f.modTime.IsZero() || f.size >= 0 {
		return fmt.Sprintf("%s:%d:%d", f.url, f.size, f.modTime.UnixNano())
	}
	return ""
}

// ModTime returns the upstream modification time, if known.
func (f *remoteFile) ModTime(ctx context.Context) time.Time {
	return f.modTime
}

// Open opens the remote file for reading, supporting Range requests
// via types.RangeOption.
func (f *remoteFile) Open(ctx context.Context, options ...types.OpenOption) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return nil, fmt.Errorf("remoteFile.Open: %w", err)
	}

	// Apply stored headers
	for k, vv := range f.headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}

	// Apply open options (e.g., RangeOption)
	for _, opt := range options {
		if opt == nil {
			continue
		}
		k, v := opt.Header()
		req.Header.Set(k, v)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("remoteFile.Open: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("remoteFile.Open: %s (status %d)", resp.Status, resp.StatusCode)
	}

	return resp.Body, nil
}
