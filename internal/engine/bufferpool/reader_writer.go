package bufferpool

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

type RW struct {
	pool          *Pool
	writeObserver io.Writer

	mu         sync.Mutex
	closed     bool
	active     int
	inactive   *sync.Cond
	pages      [][]byte
	size       int
	lastOffset int
	written    chan struct{}

	out int

	reserved [][]byte
}

var (
	errInvalidWhence = errors.New("pool.RW Seek: invalid whence")
	errNegativeSeek  = errors.New("pool.RW Seek: negative position")
	errSeekPastEnd   = errors.New("pool.RW Seek: attempt to seek past end of data")
)

func NewRW(pool *Pool) *RW {
	rw := &RW{pool: pool, pages: make([][]byte, 0, 16), written: make(chan struct{}, 1)}
	rw.inactive = sync.NewCond(&rw.mu)
	return rw
}

func (rw *RW) startOperation() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.closed {
		return false
	}
	rw.active++
	return true
}

func (rw *RW) finishOperation() {
	rw.mu.Lock()
	rw.active--
	if rw.active == 0 {
		rw.inactive.Broadcast()
	}
	rw.mu.Unlock()
}

func (rw *RW) Reserve(n int64) *RW {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	buffers := int((n + int64(rw.pool.bufferSize) - 1) / int64(rw.pool.bufferSize))
	rw.reserved = rw.pool.GetN(buffers)
	return rw
}

func (rw *RW) SetWriteObserver(observer io.Writer) *RW {
	rw.writeObserver = observer
	return rw
}

func (rw *RW) readPage(i int) []byte {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	pageNumber := i / rw.pool.bufferSize
	offset := i % rw.pool.bufferSize
	page := rw.pages[pageNumber]
	if pageNumber == len(rw.pages)-1 {
		page = page[:rw.lastOffset]
	}
	return page[offset:]
}

func (rw *RW) eof() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.out >= rw.size
}

func (rw *RW) Read(p []byte) (n int, err error) {
	if !rw.startOperation() {
		return 0, io.ErrClosedPipe
	}
	defer rw.finishOperation()
	for len(p) > 0 {
		if rw.eof() {
			return n, io.EOF
		}
		page := rw.readPage(rw.out)
		nn := copy(p, page)
		p = p[nn:]
		n += nn
		rw.out += nn
	}
	return n, nil
}

func (rw *RW) WriteTo(w io.Writer) (n int64, err error) {
	if !rw.startOperation() {
		return 0, io.ErrClosedPipe
	}
	defer rw.finishOperation()
	for !rw.eof() {
		page := rw.readPage(rw.out)
		nn, writeErr := w.Write(page)
		n += int64(nn)
		rw.out += nn
		if writeErr != nil {
			return n, writeErr
		}
	}
	return n, nil
}

func (rw *RW) writePage() (page []byte) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if len(rw.pages) > 0 && rw.lastOffset < rw.pool.bufferSize {
		return rw.pages[len(rw.pages)-1][rw.lastOffset:]
	}
	if len(rw.reserved) > 0 {
		i := len(rw.reserved) - 1
		page = rw.reserved[i]
		rw.reserved[i] = nil
		rw.reserved = rw.reserved[:i]
	} else {
		page = rw.pool.Get()
	}
	rw.pages = append(rw.pages, page)
	rw.lastOffset = 0
	return page
}

func (rw *RW) Write(p []byte) (n int, err error) {
	if !rw.startOperation() {
		return 0, io.ErrClosedPipe
	}
	defer rw.finishOperation()
	for len(p) > 0 {
		page := rw.writePage()
		nn := copy(page, p)
		p = p[nn:]
		n += nn
		rw.mu.Lock()
		rw.size += nn
		rw.lastOffset += nn
		rw.mu.Unlock()
		rw.signalWrite()
		if rw.writeObserver != nil {
			observed, observeErr := rw.writeObserver.Write(page[:nn])
			if observeErr != nil {
				return n, observeErr
			}
			if observed != nn {
				return n, io.ErrShortWrite
			}
		}
	}
	return n, nil
}

func (rw *RW) ReadFrom(r io.Reader) (n int64, err error) {
	if !rw.startOperation() {
		return 0, io.ErrClosedPipe
	}
	defer rw.finishOperation()
	for err == nil {
		page := rw.writePage()
		nn, readErr := r.Read(page)
		n += int64(nn)
		rw.mu.Lock()
		rw.size += nn
		rw.lastOffset += nn
		rw.mu.Unlock()
		rw.signalWrite()
		if nn > 0 && rw.writeObserver != nil {
			observed, observeErr := rw.writeObserver.Write(page[:nn])
			if observeErr != nil {
				return n, observeErr
			}
			if observed != nn {
				return n, io.ErrShortWrite
			}
		}
		err = readErr
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

func (rw *RW) signalWrite() {
	select {
	case rw.written <- struct{}{}:
	default:
	}
}

func (rw *RW) WaitWrite(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	select {
	case <-timer.C:
	case <-ctx.Done():
	case <-rw.written:
	}
	timer.Stop()
}

func (rw *RW) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	rw.mu.Lock()
	size := int64(rw.size)
	rw.mu.Unlock()
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(rw.out) + offset
	case io.SeekEnd:
		abs = size + offset
	default:
		return 0, errInvalidWhence
	}
	if abs < 0 {
		return 0, errNegativeSeek
	}
	if abs > size {
		return offset - (abs - size), errSeekPastEnd
	}
	rw.out = int(abs)
	return abs, nil
}

func (rw *RW) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.closed {
		return nil
	}
	rw.closed = true
	rw.signalWrite()
	for rw.active > 0 {
		rw.inactive.Wait()
	}
	rw.pool.PutN(rw.pages)
	clear(rw.pages)
	rw.pages = nil
	rw.pool.PutN(rw.reserved)
	clear(rw.reserved)
	rw.reserved = nil
	return nil
}

func (rw *RW) Size() int64 {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return int64(rw.size)
}

var (
	_ io.Reader     = (*RW)(nil)
	_ io.ReaderFrom = (*RW)(nil)
	_ io.Writer     = (*RW)(nil)
	_ io.WriterTo   = (*RW)(nil)
	_ io.Seeker     = (*RW)(nil)
	_ io.Closer     = (*RW)(nil)
)
