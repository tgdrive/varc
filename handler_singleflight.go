package varc

import (
	"context"
	"fmt"
	"net/http"
	"sync"
)

type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	waiters int
	v       any
	err     error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{m: make(map[string]*flightCall)}
}

func (g *flightGroup) do(ctx context.Context, key string, fn func(context.Context) (any, error)) (v any, err error, shared bool) {
	if g == nil {
		v, err = fn(ctx)
		return v, err, false
	}
	g.mu.Lock()
	if c := g.m[key]; c != nil {
		c.waiters++
		g.mu.Unlock()
		return g.wait(ctx, c, true)
	}
	callCtx, cancel := context.WithCancel(context.Background())
	c := &flightCall{done: make(chan struct{}), ctx: callCtx, cancel: cancel, waiters: 1}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		c.v, c.err = fn(c.ctx)
		close(c.done)
		c.cancel()
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
	}()
	return g.wait(ctx, c, false)
}

func (g *flightGroup) wait(ctx context.Context, c *flightCall, shared bool) (any, error, bool) {
	select {
	case <-c.done:
		return c.v, c.err, shared
	case <-ctx.Done():
		g.mu.Lock()
		c.waiters--
		if c.waiters == 0 {
			c.cancel()
		}
		g.mu.Unlock()
		return nil, ctx.Err(), shared
	}
}

func (h *Handler) ensureRuntime() {
	h.runtimeMu.Lock()
	defer h.runtimeMu.Unlock()
	if h.flights == nil {
		h.flights = newFlightGroup()
	}
}

func (h *Handler) probeRemoteSingleflight(ctx context.Context, r *http.Request, key, sourceURL string) (RemoteObject, error) {
	h.ensureRuntime()
	h.metrics.originProbes.Add(1)
	v, err, shared := h.flights.do(ctx, "probe:"+key+"\x00"+sourceURL, func(flightCtx context.Context) (any, error) {
		return h.probeRemote(flightCtx, r, sourceURL)
	})
	if shared {
		h.metrics.originProbeShared.Add(1)
	}
	if err != nil {
		return RemoteObject{}, err
	}
	remote, ok := v.(RemoteObject)
	if !ok {
		return RemoteObject{}, fmt.Errorf("varc: probe singleflight returned %T", v)
	}
	return remote, nil
}
