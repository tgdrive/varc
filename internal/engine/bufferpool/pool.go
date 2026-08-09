// Package bufferpool provides deterministic reusable byte buffers for cache reads.
package bufferpool

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	mapbuffer "varc/internal/engine/mapbuffer"
	"varc/internal/engine/objectio"
)

const (
	BufferSize           = 1024 * 1024
	BufferCacheSize      = 64
	BufferCacheFlushTime = 5 * time.Second
)

type Pool struct {
	mu           sync.Mutex
	cache        [][]byte
	minFill      int
	bufferSize   int
	poolSize     int
	timer        *time.Timer
	inUse        int
	alloced      int
	flushTime    time.Duration
	flushPending bool
	alloc        func(int) ([]byte, error)
	free         func([]byte) error
}

var totalMemory *semaphore.Weighted
var totalMemoryInit sync.Once

func New(flushTime time.Duration, bufferSize, poolSize int, useMmap bool) *Pool {
	bp := &Pool{
		cache:      make([][]byte, 0, poolSize),
		poolSize:   poolSize,
		flushTime:  flushTime,
		bufferSize: bufferSize,
	}
	if useMmap {
		bp.alloc = mapbuffer.Alloc
		bp.free = mapbuffer.Free
	} else {
		bp.alloc = func(size int) ([]byte, error) { return make([]byte, size), nil }
		bp.free = func([]byte) error { return nil }
	}
	totalMemoryInit.Do(func() {
		ci := objectio.GetConfig(context.Background())
		if ci.MaxBufferMemory > 0 {
			totalMemory = semaphore.NewWeighted(ci.MaxBufferMemory)
		}
	})
	bp.timer = time.AfterFunc(flushTime, bp.flushAged)
	return bp
}

func (bp *Pool) get() []byte {
	n := len(bp.cache) - 1
	buf := bp.cache[n]
	bp.cache[n] = nil
	bp.cache = bp.cache[:n]
	return buf
}

func (bp *Pool) getN(n int) [][]byte {
	i := len(bp.cache) - n
	bufs := slices.Clone(bp.cache[i:])
	bp.cache = slices.Delete(bp.cache, i, len(bp.cache))
	return bufs
}

func (bp *Pool) put(buf []byte)     { bp.cache = append(bp.cache, buf) }
func (bp *Pool) putN(bufs [][]byte) { bp.cache = append(bp.cache, bufs...) }
func (bp *Pool) buffers() int       { return len(bp.cache) }

func (bp *Pool) flush(n int) {
	for range n {
		bp.freeBuffer(bp.get())
	}
	bp.minFill = len(bp.cache)
}

func (bp *Pool) Flush() {
	bp.mu.Lock()
	bp.flush(len(bp.cache))
	bp.mu.Unlock()
}

func (bp *Pool) flushAged() {
	bp.mu.Lock()
	bp.flushPending = false
	bp.flush(bp.minFill)
	if len(bp.cache) != 0 {
		bp.kickFlusher()
	}
	bp.mu.Unlock()
}

func (bp *Pool) InUse() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.inUse
}

func (bp *Pool) InPool() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.cache)
}

func (bp *Pool) Alloced() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.alloced
}

func (bp *Pool) kickFlusher() {
	if bp.flushPending {
		return
	}
	bp.flushPending = true
	bp.timer.Reset(bp.flushTime)
}

func (bp *Pool) updateMinFill() {
	if len(bp.cache) < bp.minFill {
		bp.minFill = len(bp.cache)
	}
}

func (bp *Pool) acquire(mem int64) error {
	if totalMemory == nil {
		return nil
	}
	return totalMemory.Acquire(context.Background(), mem)
}

func (bp *Pool) release(mem int64) {
	if totalMemory != nil {
		totalMemory.Release(mem)
	}
}

func (bp *Pool) Get() []byte { return bp.GetN(1)[0] }

func (bp *Pool) GetN(n int) [][]byte {
	bp.mu.Lock()
	var (
		waitTime = time.Millisecond
		err      error
		buf      []byte
		bufs     [][]byte
		have     int
		want     int
		acquired bool
	)
	for {
		acquired = false
		bp.mu.Unlock()
		err = bp.acquire(int64(bp.bufferSize) * int64(n))
		bp.mu.Lock()
		if err != nil {
			goto FAIL
		}
		acquired = true
		have = min(bp.buffers(), n)
		want = n - have
		bufs = bp.getN(have)
		for range want {
			buf, err = bp.alloc(bp.bufferSize)
			if err != nil {
				goto FAIL
			}
			bp.alloced++
			bufs = append(bufs, buf)
		}
		break
	FAIL:
		bp.putN(bufs)
		if acquired {
			bp.release(int64(bp.bufferSize) * int64(n))
		}
		bp.mu.Unlock()
		time.Sleep(waitTime)
		bp.mu.Lock()
		waitTime *= 2
		clear(bufs)
		bufs = nil
	}
	bp.inUse += n
	bp.updateMinFill()
	bp.mu.Unlock()
	return bufs
}

func (bp *Pool) freeBuffer(mem []byte) {
	if err := bp.free(mem); err != nil {
	}
	bp.alloced--
}

func (bp *Pool) _put(buf []byte) {
	buf = buf[0:cap(buf)]
	if len(buf) != bp.bufferSize {
		panic(fmt.Sprintf("Returning buffer sized %d but expecting %d", len(buf), bp.bufferSize))
	}
	if len(bp.cache) < bp.poolSize {
		bp.put(buf)
	} else {
		bp.freeBuffer(buf)
	}
	bp.release(int64(bp.bufferSize))
}

func (bp *Pool) Put(buf []byte) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp._put(buf)
	bp.inUse--
	bp.updateMinFill()
	bp.kickFlusher()
}

func (bp *Pool) PutN(bufs [][]byte) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for _, buf := range bufs {
		bp._put(buf)
	}
	bp.inUse -= len(bufs)
	bp.updateMinFill()
	bp.kickFlusher()
}

var bufferPool *Pool
var bufferPoolOnce sync.Once

func Global() *Pool {
	bufferPoolOnce.Do(func() {
		ci := objectio.GetConfig(context.Background())
		bufferPool = New(BufferCacheFlushTime, BufferSize, BufferCacheSize, ci.UseMmap)
	})
	return bufferPool
}
