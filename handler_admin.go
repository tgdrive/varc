package varc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/tgdrive/varc/varc"
)

func (h *Handler) isAdminRequest(r *http.Request) bool {
	if h.AdminPath == "" {
		return false
	}
	return r.URL.Path == h.AdminPath || strings.HasPrefix(r.URL.Path, strings.TrimRight(h.AdminPath, "/")+"/")
}

func (h *Handler) serveAdmin(w http.ResponseWriter, r *http.Request) error {
	h.ensureRuntime()
	if h.cache == nil {
		return caddyhttp.Error(http.StatusInternalServerError, errors.New("varc: cache is not provisioned"))
	}
	if !h.adminAllowed(r) {
		return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("varc: admin endpoint is not allowed from this client"))
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("varc: admin method %s not supported", r.Method))
	}
	if r.URL.Path == strings.TrimRight(h.AdminPath, "/")+"/metrics" || r.URL.Query().Get("action") == "metrics" {
		return h.writePrometheusMetrics(w)
	}
	action := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("action")))
	if action == "" && r.URL.Path != h.AdminPath {
		action = strings.Trim(strings.TrimPrefix(r.URL.Path, strings.TrimRight(h.AdminPath, "/")), "/")
	}
	if action == "" {
		action = "status"
	}
	if r.Method == http.MethodGet && action != "status" && action != "object" && action != "health" && action != "plan" && action != "metrics" {
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("varc: admin action %q requires POST", action))
	}

	var payload map[string]any
	var err error
	switch action {
	case "status":
		payload, err = h.adminStatus(r)
	case "health":
		payload = map[string]any{"health": h.cache.Health(r.Context()), "time": nowRFC3339Nano()}
	case "object", "plan":
		payload, err = h.adminObject(r)
	case "prune":
		payload, err = h.adminPrune(r)
	case "purge", "remove", "delete":
		payload, err = h.adminPurge(r)
	case "pin":
		payload, err = h.adminPin(r, true)
	case "unpin":
		payload, err = h.adminPin(r, false)
	case "repair":
		payload, err = h.adminRepair(r)
	case "warm":
		payload, err = h.adminWarm(r)
	default:
		return caddyhttp.Error(http.StatusBadRequest, fmt.Errorf("varc: unknown admin action %q", action))
	}
	if err != nil {
		h.metrics.errors.Add(1)
		return caddyhttp.Error(http.StatusBadRequest, err)
	}
	return writeAdminJSON(w, payload)
}

func (h *Handler) adminAllowed(r *http.Request) bool {
	if h.AdminToken != "" {
		bearer := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if bearer == h.AdminToken || r.Header.Get("X-Varc-Admin-Token") == h.AdminToken {
			return true
		}
		return false
	}
	if h.AdminAllowRemote {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (h *Handler) adminStatus(r *http.Request) (map[string]any, error) {
	payload := map[string]any{
		"cache_dir":       h.cache.CacheDir(),
		"core_metrics":    h.cache.Metrics(),
		"handler_metrics": h.metrics.snapshot(),
		"time":            nowRFC3339Nano(),
	}
	entries, err := h.cache.ListEntries(r.Context())
	if err == nil {
		payload["entries"] = len(entries)
	} else {
		payload["entries_error"] = err.Error()
	}
	return payload, nil
}

func (h *Handler) adminObject(r *http.Request) (map[string]any, error) {
	key, sourceURL, err := h.adminKeyAndSource(r)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{"key": key, "source_url": sourceURL, "time": nowRFC3339Nano()}
	if meta, err := h.cache.SnapshotMeta(key); err == nil {
		payload["metadata"] = meta
	} else {
		payload["metadata_error"] = err.Error()
	}
	start, end := parseAdminRange(r.URL.Query(), 0, -1)
	if plan, err := h.cache.Plan(r.Context(), key, start, end); err == nil {
		payload["plan"] = plan
	} else {
		payload["plan_error"] = err.Error()
	}
	if cached, size, complete, err := h.cache.Coverage(key); err == nil {
		payload["coverage"] = map[string]any{"cached": cached, "size": size, "complete": complete}
	}
	if pinned, err := h.cache.IsPinned(key); err == nil {
		payload["pinned"] = pinned
	}
	return payload, nil
}

func (h *Handler) adminPrune(r *http.Request) (map[string]any, error) {
	stats, err := h.cache.Prune(r.Context())
	payload := map[string]any{"action": "prune", "prune": stats, "time": nowRFC3339Nano()}
	return payload, err
}

func (h *Handler) adminPurge(r *http.Request) (map[string]any, error) {
	key, sourceURL, err := h.adminKeyAndSource(r)
	if err != nil {
		return nil, err
	}
	err = h.cache.Remove(key)
	if err == nil {
		h.metrics.purges.Add(1)
	}
	return map[string]any{"action": "purge", "key": key, "source_url": sourceURL, "removed": err == nil, "time": nowRFC3339Nano()}, err
}

func (h *Handler) adminPin(r *http.Request, pin bool) (map[string]any, error) {
	key, sourceURL, err := h.adminKeyAndSource(r)
	if err != nil {
		return nil, err
	}
	if pin {
		err = h.cache.Pin(r.Context(), key)
		if err == nil {
			h.metrics.pins.Add(1)
		}
	} else {
		err = h.cache.Unpin(r.Context(), key)
		if err == nil {
			h.metrics.unpins.Add(1)
		}
	}
	return map[string]any{"action": map[bool]string{true: "pin", false: "unpin"}[pin], "key": key, "source_url": sourceURL, "pinned": pin, "time": nowRFC3339Nano()}, err
}

func (h *Handler) adminRepair(r *http.Request) (map[string]any, error) {
	q := r.URL.Query()
	opt := varc.RepairOptions{
		DryRun:            queryBool(q, "dry_run", false),
		RemoveCorruptMeta: queryBool(q, "remove_corrupt_meta", true),
		RemoveMissingData: queryBool(q, "remove_missing_data", true),
		DropBadRanges:     queryBool(q, "drop_bad_ranges", true),
		DropBadChecksums:  queryBool(q, "drop_bad_checksums", true),
		TouchRepaired:     queryBool(q, "touch_repaired", true),
	}
	stats, err := h.cache.Repair(r.Context(), opt)
	return map[string]any{"action": "repair", "repair": stats, "time": nowRFC3339Nano()}, err
}

func (h *Handler) adminWarm(r *http.Request) (map[string]any, error) {
	key, sourceURL, err := h.adminKeyAndSource(r)
	if err != nil {
		return nil, err
	}
	remote, err := h.probeRemoteSingleflight(r.Context(), r, key, sourceURL)
	if err != nil {
		return nil, err
	}
	if remote.Size < 0 {
		return nil, fmt.Errorf("varc: upstream did not provide a byte-addressable size")
	}
	start, end := parseAdminRange(r.URL.Query(), 0, remote.Size)
	if end < 0 || end > remote.Size {
		end = remote.Size
	}
	if start < 0 || start > end {
		return nil, fmt.Errorf("varc: invalid warm range")
	}
	src, opts := h.cacheSourceAndOptions(r, sourceURL, remote)
	job := varc.WarmJob{Key: key, Size: remote.Size, Source: src, OpenOptions: opts}
	if end > start {
		job.Ranges = []varc.Range{{Start: start, End: end}}
	}
	results, warmErr := h.cache.WarmBatch(r.Context(), []varc.WarmJob{job}, varc.WarmOptions{Concurrency: 1, Class: "admin"})
	if warmErr == nil {
		h.metrics.warms.Add(1)
	}
	return map[string]any{"action": "warm", "key": key, "source_url": sourceURL, "remote": remote, "results": results, "time": nowRFC3339Nano()}, warmErr
}

func (h *Handler) adminKeyAndSource(r *http.Request) (key string, sourceURL string, err error) {
	q := r.URL.Query()
	sourceURL = strings.TrimSpace(q.Get("url"))
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(q.Get("source_url"))
	}
	if sourceURL == "" {
		sourceURL, err = h.resolveSourceURL(replacerFromRequest(r), r)
		if err != nil && strings.TrimSpace(q.Get("key")) == "" {
			return "", "", err
		}
	}
	if sourceURL != "" {
		if _, parseErr := url.Parse(sourceURL); parseErr != nil {
			return "", "", parseErr
		}
	}
	key = strings.TrimSpace(q.Get("key"))
	if key == "" {
		if sourceURL == "" {
			return "", "", fmt.Errorf("varc: key or url is required")
		}
		key = h.cacheKey(replacerFromRequest(r), r, sourceURL)
	}
	return key, sourceURL, nil
}

func parseAdminRange(q url.Values, defaultStart, defaultEnd int64) (int64, int64) {
	start := queryInt64(q, "start", defaultStart)
	end := queryInt64(q, "end", defaultEnd)
	if raw := strings.TrimSpace(q.Get("range")); raw != "" {
		parts := strings.SplitN(raw, "-", 2)
		if len(parts) == 2 {
			if v, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
				start = v
			}
			if v, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
				end = v + 1
			}
		}
	}
	return start, end
}

func queryBool(q url.Values, key string, def bool) bool {
	raw := strings.TrimSpace(strings.ToLower(q.Get(key)))
	if raw == "" {
		return def
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func queryInt64(q url.Values, key string, def int64) int64 {
	raw := strings.TrimSpace(q.Get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func writeAdminJSON(w http.ResponseWriter, payload map[string]any) error {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func nowRFC3339Nano() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
