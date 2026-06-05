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
	done chan struct{}
	v    any
	err  error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{m: make(map[string]*flightCall)}
}

func (g *flightGroup) do(ctx context.Context, key string, fn func() (any, error)) (v any, err error, shared bool) {
	if g == nil {
		v, err = fn()
		return v, err, false
	}
	g.mu.Lock()
	if c := g.m[key]; c != nil {
		g.mu.Unlock()
		select {
		case <-c.done:
			return c.v, c.err, true
		case <-ctx.Done():
			return nil, ctx.Err(), true
		}
	}
	c := &flightCall{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	c.v, c.err = fn()
	close(c.done)

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.v, c.err, false
}

func (h *Handler) ensureRuntime() {
	if h.flights == nil {
		h.flights = newFlightGroup()
	}
}

func (h *Handler) probeRemoteSingleflight(ctx context.Context, r *http.Request, key, sourceURL string) (RemoteObject, error) {
	h.ensureRuntime()
	h.metrics.originProbes.Add(1)
	v, err, shared := h.flights.do(ctx, "probe:"+key, func() (any, error) {
		return h.probeRemote(ctx, r, sourceURL)
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
