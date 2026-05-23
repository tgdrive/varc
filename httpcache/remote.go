package httpcache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var _ io.ReaderAt = (*remoteFile)(nil)

// remoteFile is an HTTP-backed ReaderAt used by the proxy to fetch files from
// upstream URLs through the disk cache.
type remoteFile struct {
	url          string
	headers      http.Header
	size         int64
	modTime      time.Time
	etag         string
	client       *http.Client
	discovered   bool
	discoverOnce sync.Once
	discoverErr  error
}

func newHTTPFile(url string, headers http.Header, client *http.Client) *remoteFile {
	return &remoteFile{
		url:     url,
		headers: headers,
		size:    -1,
		client:  client,
	}
}

// Discover makes a HEAD request to determine the file's size, mod time, and ETag.
// It is safe to call multiple times. After this method returns successfully,
// Size() will return the discovered content length.
//
// Discover must be called before passing the remoteFile to the cache engine,
// because the engine cannot handle unknown (-1) sizes.
func (f *remoteFile) Discover() error {
	f.discoverOnce.Do(func() {
		req, err := http.NewRequest("HEAD", f.url, nil)
		if err != nil {
			f.discoverErr = fmt.Errorf("remoteFile.Discover: %w", err)
			return
		}
		for k, vv := range f.headers {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
		resp, err := f.client.Do(req)
		if err != nil {
			f.discoverErr = fmt.Errorf("remoteFile.Discover: %w", err)
			return
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			f.discoverErr = fmt.Errorf("remoteFile.Discover: HEAD returned status %d", resp.StatusCode)
			return
		}
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if sz, err := strconv.ParseInt(cl, 10, 64); err == nil {
				f.size = sz
			}
		}
		if lm := resp.Header.Get("Last-Modified"); lm != "" {
			if t, err := http.ParseTime(lm); err == nil {
				f.modTime = t
			}
		}
		if etag := resp.Header.Get("ETag"); etag != "" {
			f.etag = etag
		}
		f.discovered = true
	})
	return f.discoverErr
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

func (f *remoteFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("remoteFile.ReadAt: negative offset %d", off)
	}
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return 0, fmt.Errorf("remoteFile.ReadAt: %w", err)
	}

	for k, vv := range f.headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("remoteFile.ReadAt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("remoteFile.ReadAt: %s (status %d)", resp.Status, resp.StatusCode)
	}

	n, err := io.ReadFull(resp.Body, p)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
