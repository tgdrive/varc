package varc

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

type handlerMetrics struct {
	requests            atomic.Int64
	hits                atomic.Int64
	misses              atomic.Int64
	staleHits           atomic.Int64
	bypass              atomic.Int64
	cacheOnlyMisses     atomic.Int64
	originProbes        atomic.Int64
	originProbeShared   atomic.Int64
	originRangeFetches  atomic.Int64
	rangeNotSatisfiable atomic.Int64
	bytesServed         atomic.Int64
	bytesFromCache      atomic.Int64
	bytesFromOrigin     atomic.Int64
	purges              atomic.Int64
	pins                atomic.Int64
	unpins              atomic.Int64
	warms               atomic.Int64
	errors              atomic.Int64
	durationNanos       atomic.Int64
}

func (m *handlerMetrics) observeDuration(d time.Duration) {
	if d > 0 {
		m.durationNanos.Add(int64(d))
	}
}

func (m *handlerMetrics) snapshot() map[string]int64 {
	return map[string]int64{
		"requests":              m.requests.Load(),
		"hits":                  m.hits.Load(),
		"misses":                m.misses.Load(),
		"stale_hits":            m.staleHits.Load(),
		"bypass":                m.bypass.Load(),
		"cache_only_misses":     m.cacheOnlyMisses.Load(),
		"origin_probes":         m.originProbes.Load(),
		"origin_probe_shared":   m.originProbeShared.Load(),
		"origin_range_fetches":  m.originRangeFetches.Load(),
		"range_not_satisfiable": m.rangeNotSatisfiable.Load(),
		"bytes_served":          m.bytesServed.Load(),
		"bytes_from_cache":      m.bytesFromCache.Load(),
		"bytes_from_origin":     m.bytesFromOrigin.Load(),
		"purges":                m.purges.Load(),
		"pins":                  m.pins.Load(),
		"unpins":                m.unpins.Load(),
		"warms":                 m.warms.Load(),
		"errors":                m.errors.Load(),
		"duration_nanos_total":  m.durationNanos.Load(),
	}
}

func (h *Handler) writePrometheusMetrics(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	stats := h.metrics.snapshot()
	for name, value := range stats {
		metric := "varc_handler_" + strings.ReplaceAll(name, "-", "_")
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", metric, metric, value)
	}
	if h.cache != nil {
		core := h.cache.Metrics()
		v := reflect.ValueOf(core)
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			name := field.Tag.Get("json")
			if comma := strings.IndexByte(name, ','); comma >= 0 {
				name = name[:comma]
			}
			if name == "" || name == "-" {
				name = field.Name
			}
			metric := "varc_core_" + sanitizeMetricName(name)
			fmt.Fprintf(w, "# TYPE %s gauge\n%s %v\n", metric, metric, v.Field(i).Interface())
		}
	}
	return nil
}

func sanitizeMetricName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}
