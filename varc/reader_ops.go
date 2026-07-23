package varc

import (
	"context"
	"errors"
	"io"
	"time"
)

func (r *Reader) WarmRange(ctx context.Context, start, end int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if start < 0 || end < start {
		return ErrInvalidRange
	}
	meta := r.currentMeta()
	if start > meta.Size {
		return io.EOF
	}
	if end > meta.Size {
		end = meta.Size
	}
	return r.ensureRangeMode(ctx, start, end, false)
}

// WarmAll downloads the entire object into the cache.
func (r *Reader) WarmAll(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return r.WarmRange(ctx, 0, r.Size())
}

// CopyTo writes the object to dst using the cache.  Missing ranges are fetched
// and persisted along the way.
func (r *Reader) CopyTo(ctx context.Context, dst io.Writer) (int64, error) {
	if dst == nil {
		return 0, errors.New("varc: nil writer")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	buf := make([]byte, maxWriteBufferSize)
	var off int64
	var total int64
	for off < r.Size() {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		need := min64(int64(len(buf)), r.Size()-off)
		n, err := r.ReadAtContext(ctx, buf[:need], off)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
			if wn != n {
				return total, io.ErrShortWrite
			}
			off += int64(n)
		}
		if err != nil {
			if err == io.EOF && off >= r.Size() {
				break
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

// Attr returns a metadata attribute stored at Open time.
func (r *Reader) Attr(key string) (string, bool) {
	meta := r.currentMeta()
	v, ok := meta.Attrs[key]
	return v, ok
}

// SetAttr sets or updates a metadata attribute.  It does not affect cached data.
func (r *Reader) SetAttr(key, value string) error {
	if key == "" {
		return errors.New("varc: empty attribute key")
	}
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.meta.Attrs == nil {
		st.meta.Attrs = make(map[string]string)
	}
	st.meta.Attrs[key] = value
	st.meta.UpdatedAt = time.Now()
	return r.cache.saveMetaLocked(st)
}

// RemoveAttr removes a metadata attribute.
func (r *Reader) RemoveAttr(key string) error {
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.meta.Attrs != nil {
		delete(st.meta.Attrs, key)
	}
	st.meta.UpdatedAt = time.Now()
	return r.cache.saveMetaLocked(st)
}
