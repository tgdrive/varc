package varc

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

func (r *Reader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	r.cursorMu.Lock()
	off := r.pos
	gen := r.cursorGen
	r.cursorMu.Unlock()

	n, err := r.readContextLocked(r.ctx, p, off)

	r.cursorMu.Lock()
	if r.cursorGen == gen {
		r.pos = off + int64(n)
	}
	r.cursorMu.Unlock()
	return n, err
}

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	return r.readAtContextLocked(r.ctx, p, off)
}

// ReadAtContext reads at offset using ctx for waiting on cache misses and
// source downloads. It is the preferred method for request-scoped servers.
func (r *Reader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.closed.Load() {
		return 0, ErrClosed
	}
	return r.readAtContextLocked(ctx, p, off)
}

func (r *Reader) readContextLocked(ctx context.Context, p []byte, off int64) (int, error) {
	return r.readAtContextLockedMode(ctx, p, off, false)
}

func (r *Reader) readAtContextLocked(ctx context.Context, p []byte, off int64) (int, error) {
	return r.readAtContextLockedMode(ctx, p, off, true)
}

func (r *Reader) readAtContextLockedMode(ctx context.Context, p []byte, off int64, requireFull bool) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", ErrInvalidRange, off)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	meta := r.currentMeta()
	r.beginAdaptiveAccess(off)
	if off >= meta.Size {
		return 0, io.EOF
	}
	bufferEnd := off + int64(len(p))
	requestedEnd := min64(meta.Size, bufferEnd)
	truncatedAtEOF := requestedEnd < bufferEnd
	readEnd := requestedEnd
	var err error
	if requireFull {
		err = r.ensureRange(ctx, off, requestedEnd)
	} else {
		readEnd, err = r.ensureReadablePrefix(ctx, off, requestedEnd)
	}
	if err != nil {
		return 0, err
	}
	f, err := os.Open(r.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, readErr := readFullAt(f, p[:readEnd-off], off)
	r.cache.metricReads.Add(1)
	r.cache.metricReadBytes.Add(int64(n))
	r.cache.metricHits.Add(1)
	r.cache.metricHitBytes.Add(int64(n))
	if n > 0 {
		r.finishAdaptiveAccess(off, int64(n))
		r.touch(false)
	}
	if readErr != nil && readErr != io.EOF {
		return n, readErr
	}
	if truncatedAtEOF && off+int64(n) >= meta.Size {
		return n, io.EOF
	}
	if requireFull && n < len(p) {
		return n, io.EOF
	}
	return n, readErr
}

func (r *Reader) currentMeta() cacheMeta {
	r.state.mu.Lock()
	meta := r.state.meta
	r.state.mu.Unlock()
	return meta
}

func readFullAt(f *os.File, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var total int
	for total < len(p) {
		n, err := f.ReadAt(p[total:], off+int64(total))
		total += n
		if err != nil {
			if err == io.EOF && total == len(p) {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

// Seek implements io.Seeker. It only updates cursor state and never waits for
// an in-flight cache fill.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	meta := r.currentMeta()
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()
	if r.closed.Load() {
		return 0, ErrClosed
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		next = meta.Size + offset
	default:
		return r.pos, fmt.Errorf("varc: invalid whence %d", whence)
	}
	if next < 0 {
		return r.pos, fmt.Errorf("%w: negative seek position %d", ErrInvalidRange, next)
	}
	r.pos = next
	r.cursorGen++
	r.resetAdaptive(next)
	return r.pos, nil
}

// Close closes the reader and cancels all outstanding work once no readers
// remain for the cache entry.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.cancel()
		st := r.state
		st.mu.Lock()
		if st.readers > 0 {
			st.readers--
		}
		if st.readers == 0 {
			r.cache.cancelTasksLocked(st)
		}
		st.notifyLocked()
		st.mu.Unlock()
		r.cache.releaseState(st)
	})
	return nil
}

// Size returns the logical size of the opened object.
func (r *Reader) Size() int64 { return r.currentMeta().Size }

// ModTime returns the upstream modification time when provided.
func (r *Reader) ModTime() time.Time { return r.currentMeta().ModTime }

// Fingerprint returns the fingerprint supplied at open time, if any.
func (r *Reader) Fingerprint() string { return r.currentMeta().Fingerprint }

// CachedRanges returns a snapshot of currently cached byte ranges.
func (r *Reader) CachedRanges() []byteRange { return cloneRanges(r.currentMeta().Ranges) }

// CachedBytes returns the number of logical bytes currently cached for this
// reader's entry.
func (r *Reader) CachedBytes() int64 { return rangesLen(r.currentMeta().Ranges) }

// Complete reports whether the whole object is cached.
func (r *Reader) Complete() bool {
	meta := r.currentMeta()
	return containsRange(meta.Ranges, 0, meta.Size)
}

func (r *Reader) ensureRange(ctx context.Context, start, end int64) error {
	return r.ensureRangeMode(ctx, start, end, true)
}

func (r *Reader) ensureReadablePrefix(ctx context.Context, start, end int64) (int64, error) {
	if start < 0 || end < start {
		return start, ErrInvalidRange
	}
	if start == end {
		return end, nil
	}
	st := r.state
	missRecorded := false
	st.mu.Lock()
	defer st.mu.Unlock()
	seenFailure := st.failureSeq
	for {
		if err := ctx.Err(); err != nil {
			return start, err
		}
		if r.cache.closed.Load() || st.removed || r.generation != st.generation {
			return start, ErrClosed
		}
		available := append([]byteRange(nil), st.meta.Ranges...)
		for _, volatile := range st.volatile {
			available = addRange(available, volatile.Start, volatile.End)
		}
		for _, availableRange := range available {
			if availableRange.Start <= start && availableRange.End > start {
				r.meta = st.meta
				return min64(end, availableRange.End), nil
			}
		}
		if !missRecorded {
			r.cache.metricMisses.Add(1)
			r.cache.metricMissBytes.Add(end - start)
			missRecorded = true
		}
		if r.src == nil || r.cacheOnly {
			return start, fmt.Errorf("%w for %q at %d-%d", ErrCacheMiss, r.key, start, end-1)
		}
		r.ensureAdaptiveTasksLocked(st, start, end, st.meta.Size)
		if err := waitStateChange(ctx, st); err != nil {
			return start, err
		}
		if st.failureSeq > seenFailure && st.lastError != nil {
			return start, st.lastError
		}
	}
}

func (r *Reader) ensureRangeMode(ctx context.Context, start, end int64, allowVolatile bool) error {
	if start < 0 || end < start {
		return ErrInvalidRange
	}
	if start == end {
		return nil
	}
	st := r.state
	missRecorded := false
	st.mu.Lock()
	defer st.mu.Unlock()
	seenFailure := st.failureSeq
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.cache.closed.Load() || st.removed || r.generation != st.generation {
			return ErrClosed
		}
		available := append([]byteRange(nil), st.meta.Ranges...)
		if allowVolatile {
			available = append(available, st.volatile...)
		}
		if containsRange(available, start, end) {
			r.meta = st.meta
			return nil
		}
		missingStart, missingEnd, ok := firstMissingRange(available, start, end)
		if !ok {
			r.meta = st.meta
			return nil
		}
		if !missRecorded {
			r.cache.metricMisses.Add(1)
			r.cache.metricMissBytes.Add(missingEnd - missingStart)
			missRecorded = true
		}
		if r.src == nil || r.cacheOnly {
			return fmt.Errorf("%w for %q at %d-%d", ErrCacheMiss, r.key, missingStart, missingEnd-1)
		}
		r.ensureAdaptiveTasksLocked(st, missingStart, missingEnd, st.meta.Size)
		if err := waitStateChange(ctx, st); err != nil {
			return err
		}
		if st.failureSeq > seenFailure && st.lastError != nil && !containsRange(st.meta.Ranges, start, end) {
			return st.lastError
		}
	}
}

func waitStateChange(ctx context.Context, st *entryState) error {
	changed := st.changed
	st.mu.Unlock()
	select {
	case <-changed:
	case <-ctx.Done():
	}
	st.mu.Lock()
	return ctx.Err()
}

func (st *entryState) notifyLocked() {
	st.cond.Broadcast()
	close(st.changed)
	st.changed = make(chan struct{})
}

func (r *Reader) touch(force bool) {
	st := r.state
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	if !force && r.cache.touchInterval >= 0 && !st.lastTouch.IsZero() && now.Sub(st.lastTouch) < r.cache.touchInterval {
		return
	}
	st.lastTouch = now
	st.meta.AccessedAt = now
	st.meta.UpdatedAt = now
	if err := r.cache.saveMetaLocked(st); err != nil {
		r.cache.logger.Warnf("varc: update access metadata for %q: %v", r.key, err)
	}
}
