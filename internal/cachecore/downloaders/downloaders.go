// Package downloaders provides the VFS cache downloader state machine.
package downloaders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"vfs-cache/internal/cachecore/asyncreader"
	"vfs-cache/internal/cachecore/chunkedreader"
	"vfs-cache/internal/cachecore/fs"
	"vfs-cache/internal/cachecore/fserrors"
	"vfs-cache/internal/cachecore/vfscommon"
	"vfs-cache/ranges"
)

const (
	maxDownloaderIdleTime    = 5 * time.Second
	maxSkipBytes             = 1024 * 1024
	backgroundKickerInterval = 5 * time.Second
	maxErrorCount            = 10
	minWindow                = 1024 * 1024
)

type Item interface {
	FindMissing(r ranges.Range) (outr ranges.Range)
	HasRange(r ranges.Range) bool
	WriteAtNoOverwrite(b []byte, off int64) (n int, skipped int, err error)
}

type Downloaders struct {
	ctx    context.Context
	cancel context.CancelFunc
	item   Item
	opt    *vfscommon.Options
	src    fs.Object
	remote string
	wg     sync.WaitGroup

	mu         sync.Mutex
	dls        []*downloader
	waiters    []waiter
	errorCount int
	lastErr    error
}

type waiter struct {
	r       ranges.Range
	errChan chan<- error
}

type downloader struct {
	dls  *Downloaders
	quit chan struct{}
	wg   sync.WaitGroup
	kick chan struct{}

	mu        sync.Mutex
	start     int64
	offset    int64
	maxOffset int64
	in        *downloadInput
	skipped   int64
	_closed   bool
	stop      bool
}

type downloadInput struct {
	raw   io.ReadCloser
	async *asyncreader.AsyncReader
}

func newDownloadInput(ctx context.Context, raw io.ReadCloser, size, bufferSize int64) *downloadInput {
	in := &downloadInput{raw: raw}
	var buffers int
	if size >= bufferSize || size == -1 {
		buffers = int(bufferSize / asyncreader.BufferSize)
	} else {
		buffers = int(size / asyncreader.BufferSize)
	}
	if buffers > 0 {
		if async, err := asyncreader.New(ctx, raw, buffers); err == nil {
			in.async = async
		}
	}
	return in
}

func (in *downloadInput) HasBuffer() bool { return in.async != nil }

func (in *downloadInput) StopBuffering() {
	if in.async != nil {
		in.async.StopBuffering()
	}
}

func (in *downloadInput) WriteTo(w io.Writer) (int64, error) {
	if in.async != nil {
		return in.async.WriteTo(w)
	}
	if wt, ok := in.raw.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, in.raw)
}

func (in *downloadInput) Close() error {
	if in.async != nil {
		return in.async.Close()
	}
	return in.raw.Close()
}

func New(ctx context.Context, item Item, opt *vfscommon.Options, remote string, src fs.Object) *Downloaders {
	if src == nil {
		panic("internal error: newDownloaders called with nil src object")
	}
	ctx, cancel := context.WithCancel(ctx)
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item, opt: opt, src: src, remote: remote}
	dls.wg.Go(func() {
		ticker := time.NewTicker(backgroundKickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := dls.kickWaiters(); err != nil {
					fs.Errorf(dls.src, "vfs cache: failed to kick waiters: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	})
	return dls
}

func (dls *Downloaders) _countErrors(n int64, err error) {
	if err == nil && n != 0 {
		if dls.errorCount != 0 {
			fs.Infof(dls.src, "vfs cache: downloader: resetting error count to 0")
			dls.errorCount = 0
			dls.lastErr = nil
		}
		return
	}
	if err != nil {
		dls.errorCount++
		dls.lastErr = err
		fs.Infof(dls.src, "vfs cache: downloader: error count now %d: %v", dls.errorCount, err)
	}
}

func (dls *Downloaders) countErrors(n int64, err error) {
	dls.mu.Lock()
	dls._countErrors(n, err)
	dls.mu.Unlock()
}

func (dls *Downloaders) _newDownloader(r ranges.Range) (dl *downloader, err error) {
	dl = &downloader{
		kick:      make(chan struct{}, 1),
		quit:      make(chan struct{}),
		dls:       dls,
		start:     r.Pos,
		offset:    r.Pos,
		maxOffset: r.End(),
	}
	if err = dl.open(dl.offset); err != nil {
		_ = dl.close(err)
		return nil, fmt.Errorf("failed to open downloader: %w", err)
	}
	dls.dls = append(dls.dls, dl)
	dl.wg.Go(func() {
		n, err := dl.download()
		_ = dl.close(err)
		dl.dls.countErrors(n, err)
		if err != nil {
			fs.Errorf(dl.dls.src, "vfs cache: failed to download: %v", err)
		}
		if err = dl.dls.kickWaiters(); err != nil {
			fs.Errorf(dl.dls.src, "vfs cache: failed to kick waiters: %v", err)
		}
	})
	return dl, nil
}

func (dls *Downloaders) _removeClosed() {
	newDownloaders := dls.dls[:0]
	for _, dl := range dls.dls {
		if !dl.closed() {
			newDownloaders = append(newDownloaders, dl)
		}
	}
	dls.dls = newDownloaders
}

func (dls *Downloaders) Close(inErr error) (err error) {
	dls.mu.Lock()
	defer dls.mu.Unlock()
	dls._removeClosed()
	for _, dl := range dls.dls {
		dls.mu.Unlock()
		closeErr := dl.stopAndClose(inErr)
		dls.mu.Lock()
		if closeErr != nil && err != nil {
			err = closeErr
		}
	}
	dls.cancel()
	dls.mu.Unlock()
	dls.wg.Wait()
	dls.mu.Lock()
	dls.dls = nil
	dls._dispatchWaiters()
	dls._closeWaiters(inErr)
	return err
}

func (dls *Downloaders) Download(r ranges.Range) (err error) {
	dls.mu.Lock()
	errChan := make(chan error)
	waiter := waiter{r: r, errChan: errChan}
	if err = dls._ensureDownloader(r); err != nil {
		dls.mu.Unlock()
		return err
	}
	dls.waiters = append(dls.waiters, waiter)
	dls.mu.Unlock()
	return <-errChan
}

func (dls *Downloaders) _closeWaiters(err error) {
	for _, waiter := range dls.waiters {
		waiter.errChan <- err
	}
	dls.waiters = nil
}

func (dls *Downloaders) _ensureDownloader(r ranges.Range) (err error) {
	window := fs.GetConfig(dls.ctx).BufferSize
	if dls.opt.ReadAhead > 0 {
		r.Size += dls.opt.ReadAhead
	}
	r = dls.item.FindMissing(r)
	startNew := true
	if r.IsEmpty() {
		rWindow := r
		rWindow.Size += window
		rWindowClipped := dls.item.FindMissing(rWindow)
		if rWindowClipped.IsEmpty() {
			startNew = false
			r.Pos = rWindow.End()
		} else {
			r.Pos = rWindowClipped.Pos
		}
		r.Size = 0
	}
	if window < minWindow {
		window = minWindow
	}
	var dl *downloader
	dls._removeClosed()
	for _, dl = range dls.dls {
		start, offset := dl.getRange()
		if r.Pos >= start && r.Pos < offset+window {
			dl.setRange(r)
			return nil
		}
	}
	if !startNew {
		return nil
	}
	if r.Size == 0 {
		return nil
	}
	if _, err = dls._newDownloader(r); err != nil {
		dls._countErrors(0, err)
		return fmt.Errorf("failed to start downloader: %w", err)
	}
	return err
}

func (dls *Downloaders) EnsureDownloader(r ranges.Range) (err error) {
	dls.mu.Lock()
	defer dls.mu.Unlock()
	return dls._ensureDownloader(r)
}

func (dls *Downloaders) _dispatchWaiters() {
	if len(dls.waiters) == 0 {
		return
	}
	newWaiters := dls.waiters[:0]
	for _, waiter := range dls.waiters {
		r := waiter.r
		r.Clip(dls.src.Size())
		if dls.item.HasRange(r) {
			waiter.errChan <- nil
		} else {
			newWaiters = append(newWaiters, waiter)
		}
	}
	dls.waiters = newWaiters
}

func (dls *Downloaders) kickWaiters() (err error) {
	dls.mu.Lock()
	defer dls.mu.Unlock()
	dls._dispatchWaiters()
	if len(dls.waiters) == 0 {
		return nil
	}
	for _, waiter := range dls.waiters {
		err = dls._ensureDownloader(waiter.r)
		if err != nil {
			fs.Errorf(dls.src, "vfs cache: restart download failed: %v", err)
		}
	}
	if fserrors.IsErrNoSpace(dls.lastErr) {
		fs.Errorf(dls.src, "vfs cache: cache is out of space %d/%d: last error: %v", dls.errorCount, maxErrorCount, dls.lastErr)
		dls._closeWaiters(dls.lastErr)
		return dls.lastErr
	}
	if dls.errorCount > maxErrorCount {
		fs.Errorf(dls.src, "vfs cache: too many errors %d/%d: last error: %v", dls.errorCount, maxErrorCount, dls.lastErr)
		dls._closeWaiters(dls.lastErr)
		return dls.lastErr
	}
	return nil
}

func (dl *downloader) Write(p []byte) (n int, err error) {
	defer func() {
		if n <= 0 {
			return
		}
		if waitErr := dl.dls.kickWaiters(); waitErr != nil {
			fs.Errorf(dl.dls.src, "vfs cache: download write: failed to kick waiters: %v", waitErr)
			if err == nil {
				err = waitErr
			}
		}
	}()
	dl.mu.Lock()
	defer dl.mu.Unlock()
loop:
	for dl.offset >= dl.maxOffset {
		timeout := time.NewTimer(maxDownloaderIdleTime)
		dl.mu.Unlock()
		select {
		case <-dl.quit:
			dl.mu.Lock()
			timeout.Stop()
			break loop
		case <-dl.kick:
			dl.mu.Lock()
			timeout.Stop()
		case <-timeout.C:
			dl.mu.Lock()
			if !dl.stop {
				fs.Debugf(dl.dls.src, "vfs cache: stopping download thread as it timed out")
				dl._stop()
			}
			break loop
		}
	}
	n, skipped, err := dl.dls.item.WriteAtNoOverwrite(p, dl.offset)
	if skipped == n {
		dl.skipped += int64(skipped)
	} else {
		dl.skipped = 0
	}
	dl.offset += int64(n)
	if !dl.stop && dl.skipped > maxSkipBytes {
		fs.Debugf(dl.dls.src, "vfs cache: stopping download thread as it has skipped %d bytes", dl.skipped)
		dl._stop()
	}
	if dl.stop && !dl.in.HasBuffer() {
		err = asyncreader.ErrorStreamAbandoned
	}
	return n, err
}

func (dl *downloader) open(offset int64) (err error) {
	size := dl.dls.src.Size()
	if size < 0 {
		return errors.New("can't open unknown sized file")
	}
	in0 := chunkedreader.New(dl.dls.ctx, dl.dls.src, dl.dls.opt.ChunkSize, dl.dls.opt.ChunkSizeLimit, dl.dls.opt.ChunkStreams)
	if _, err = in0.Seek(offset, 0); err != nil {
		return fmt.Errorf("vfs reader: failed to open source file: %w", err)
	}
	dl.in = newDownloadInput(dl.dls.ctx, in0, size, fs.GetConfig(dl.dls.ctx).BufferSize)
	dl.offset = offset
	return nil
}

func (dl *downloader) close(_ error) (err error) {
	checkErr := func(e error) {
		if e == nil || errors.Is(err, asyncreader.ErrorStreamAbandoned) {
			return
		}
		err = e
	}
	dl.mu.Lock()
	if dl.in != nil {
		checkErr(dl.in.Close())
		dl.in = nil
	}
	dl._closed = true
	dl.mu.Unlock()
	return nil
}

func (dl *downloader) closed() bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl._closed
}

func (dl *downloader) _stop() {
	if dl.stop {
		return
	}
	dl.stop = true
	close(dl.quit)
	if dl.in != nil {
		dl.in.StopBuffering()
	}
}

func (dl *downloader) stopAndClose(inErr error) (err error) {
	dl.mu.Lock()
	dl._stop()
	dl.mu.Unlock()
	dl.wg.Wait()
	return dl.close(inErr)
}

func (dl *downloader) download() (n int64, err error) {
	n, err = dl.in.WriteTo(dl)
	if err != nil && !errors.Is(err, asyncreader.ErrorStreamAbandoned) {
		return n, fmt.Errorf("vfs reader: failed to write to cache file: %w", err)
	}
	return n, nil
}

func (dl *downloader) setRange(r ranges.Range) {
	dl.mu.Lock()
	maxOffset := r.End()
	if maxOffset > dl.maxOffset {
		dl.maxOffset = maxOffset
	}
	dl.mu.Unlock()
	select {
	case dl.kick <- struct{}{}:
	default:
	}
}

func (dl *downloader) getRange() (start, offset int64) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.start, dl.offset
}
