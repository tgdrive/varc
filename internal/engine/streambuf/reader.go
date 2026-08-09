// Package streambuf provides asynchronous buffering for origin reads.
package streambuf

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/tgdrive/varc/internal/engine/bufferpool"
	"github.com/tgdrive/varc/internal/engine/objectio"
	"github.com/tgdrive/varc/internal/engine/readutil"
)

const (
	BufferSize       = bufferpool.BufferSize
	softStartInitial = 4 * 1024
)

var ErrorStreamAbandoned = errors.New("stream abandoned")

type Reader struct {
	in      io.ReadCloser
	ready   chan *buffer
	token   chan struct{}
	exit    chan struct{}
	buffers int
	err     error
	cur     *buffer
	exited  chan struct{}
	size    int
	closed  bool
	mu      sync.Mutex
	ci      *objectio.Config
	pool    *bufferpool.Pool
}

func New(ctx context.Context, rd io.ReadCloser, buffers int) (*Reader, error) {
	if buffers <= 0 {
		return nil, errors.New("number of buffers too small")
	}
	if rd == nil {
		return nil, errors.New("nil reader supplied")
	}
	a := &Reader{ci: objectio.GetConfig(ctx), pool: bufferpool.Global()}
	a.init(rd, buffers)
	return a, nil
}

func (a *Reader) init(rd io.ReadCloser, buffers int) {
	a.in = rd
	a.ready = make(chan *buffer, buffers)
	a.token = make(chan struct{}, buffers)
	a.exit = make(chan struct{})
	a.exited = make(chan struct{})
	a.buffers = buffers
	a.cur = nil
	a.size = softStartInitial
	for range buffers {
		a.token <- struct{}{}
	}
	go func() {
		defer close(a.exited)
		defer close(a.ready)
		for {
			select {
			case <-a.token:
				b := a.getBuffer()
				if a.size < BufferSize {
					b.buf = b.buf[:a.size]
					a.size <<= 1
				}
				err := b.read(a.in)
				a.ready <- b
				if err != nil {
					return
				}
			case <-a.exit:
				return
			}
		}
	}()
}

func (a *Reader) putBuffer(b *buffer) {
	a.pool.Put(b.buf)
	b.buf = nil
}

func (a *Reader) getBuffer() *buffer { return &buffer{buf: a.pool.Get()} }

func (a *Reader) fill() (err error) {
	if a.cur.isEmpty() {
		if a.cur != nil {
			a.putBuffer(a.cur)
			a.token <- struct{}{}
			a.cur = nil
		}
		b, ok := <-a.ready
		if !ok {
			if a.err == nil {
				return ErrorStreamAbandoned
			}
			return a.err
		}
		a.cur = b
	}
	return nil
}

func (a *Reader) Read(p []byte) (n int, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err = a.fill(); err != nil {
		return 0, err
	}
	n = copy(p, a.cur.buffer())
	a.cur.increment(n)
	if a.cur.isEmpty() {
		a.err = a.cur.err
		return n, a.err
	}
	return n, nil
}

func (a *Reader) WriteTo(w io.Writer) (n int64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		err = a.fill()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		n2, writeErr := w.Write(a.cur.buffer())
		a.cur.increment(n2)
		n += int64(n2)
		if writeErr != nil {
			return n, writeErr
		}
		if a.cur.err == io.EOF {
			a.err = a.cur.err
			return n, err
		}
		if a.cur.err != nil {
			a.err = a.cur.err
			return n, a.cur.err
		}
	}
}

func (a *Reader) SkipBytes(skip int) (ok bool) {
	a.mu.Lock()
	defer func() {
		a.mu.Unlock()
		if !ok {
			a.Abandon()
		}
	}()
	if a.err != nil {
		return false
	}
	if skip < 0 {
		if a.cur != nil && a.cur.offset+skip >= 0 {
			a.cur.offset += skip
			return true
		}
		return false
	}
	if skip >= (len(a.ready)+1)*BufferSize {
		return false
	}
	refillTokens := 0
	for {
		if a.cur.isEmpty() {
			if a.cur != nil {
				a.putBuffer(a.cur)
				refillTokens++
				a.cur = nil
			}
			select {
			case b, ok := <-a.ready:
				if !ok {
					return false
				}
				a.cur = b
			default:
				return false
			}
		}
		n := min(len(a.cur.buffer()), skip)
		a.cur.increment(n)
		skip -= n
		if skip == 0 {
			for ; refillTokens > 0; refillTokens-- {
				a.token <- struct{}{}
			}
			if a.cur.isEmpty() && a.cur.err != nil {
				a.err = a.cur.err
			}
			return true
		}
		if a.cur.err != nil {
			a.err = a.cur.err
			return false
		}
	}
}

func (a *Reader) StopBuffering() {
	select {
	case <-a.exit:
		return
	default:
	}
	close(a.exit)
	<-a.exited
}

func (a *Reader) Abandon() {
	a.StopBuffering()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur != nil {
		a.putBuffer(a.cur)
		a.cur = nil
	}
	for b := range a.ready {
		a.putBuffer(b)
	}
}

func (a *Reader) Close() error {
	a.Abandon()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.in.Close()
}

type buffer struct {
	buf    []byte
	err    error
	offset int
}

func (b *buffer) isEmpty() bool {
	return b == nil || len(b.buf)-b.offset <= 0
}

func (b *buffer) read(rd io.Reader) error {
	var n int
	n, b.err = readutil.ReadFill(rd, b.buf)
	b.buf = b.buf[:n]
	b.offset = 0
	return b.err
}

func (b *buffer) buffer() []byte  { return b.buf[b.offset:] }
func (b *buffer) increment(n int) { b.offset += n }
