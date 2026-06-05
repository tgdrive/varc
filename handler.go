// Package varc implements a Caddy v2 HTTP handler that serves remote
// byte-addressable objects through the varc sparse read-through cache.
//
// The handler is intentionally cache-first.  If an HTTP byte range is already
// present in varc, it serves directly from the local sparse file and does not
// contact the upstream origin at all.  On cache miss, it probes the origin for
// size/validators and uses HTTP Range requests as varc's io.ReaderAt source.
package varc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/tgdrive/varc/varc"
	"go.uber.org/zap"
)

const (
	defaultCacheDir       = "/var/cache/caddy/varc"
	defaultTimeout        = 60 * time.Second
	defaultProbeTimeout   = 15 * time.Second
	defaultResponseBuffer = 256 * 1024
)

func init() {
	caddy.RegisterModule(new(Handler))
}

// Handler is a Caddy HTTP middleware that turns a remote HTTP origin into a
// local sparse range cache backed by varc.
//
// JSON example:
//
//	{
//	  "handler": "varc",
//	  "upstream": "https://origin.example.com/files",
//	  "cache_dir": "/var/cache/caddy/varc",
//	  "chunk_size": 134217728,
//	  "block_size": 1048576,
//	  "chunk_streams": 4,
//	  "read_ahead": 16777216,
//	  "debug_headers": true
//	}
//
// By default, the request path and query are appended to Upstream.  For
// example, upstream=https://origin.example.com and request=/video/a.mp4?x=1
// resolves to https://origin.example.com/video/a.mp4?x=1.
type Handler struct {
	// Upstream is the origin base URL or a full placeholder-expanded URL.  When
	// AppendURI is true, the current request URI is appended to it.
	Upstream string `json:"upstream,omitempty"`

	// CacheDir is the varc data directory.
	CacheDir string `json:"cache_dir,omitempty"`

	// Key is an optional Caddy placeholder template used as the varc key.  If
	// empty, the resolved upstream URL is used.  Good custom keys usually include
	// host, path, query/version, tenant, and authorization scope when applicable.
	Key string `json:"key,omitempty"`

	// AppendURI controls whether the request path/query is appended to Upstream.
	// Nil means true.
	AppendURI *bool `json:"append_uri,omitempty"`

	// IgnoreQuery drops the request query string when AppendURI builds the
	// upstream URL and when the default key is based on that URL.
	IgnoreQuery bool `json:"ignore_query,omitempty"`

	// StripQuery removes named query parameters before building the upstream URL
	// and default cache key.  It is intended for tracking parameters like utm_*.
	StripQuery []string `json:"strip_query,omitempty"`

	// SortQuery sorts query keys/values into a canonical order before building the
	// upstream URL and default key, so ?b=2&a=1 and ?a=1&b=2 collapse.
	SortQuery bool `json:"sort_query,omitempty"`

	// LowercaseHost canonicalizes the upstream host used in the default key.
	LowercaseHost bool `json:"lowercase_host,omitempty"`

	// VaryHeaders adds selected request headers to the varc key.  Use this when a
	// forwarded header changes object bytes, for example Accept-Language or auth scope.
	VaryHeaders []string `json:"vary_headers,omitempty"`

	// BypassHeaders bypasses varc and streams from origin if any named header is
	// present.  Authorization is bypassed by default unless CacheAuthorization is true.
	BypassHeaders []string `json:"bypass_headers,omitempty"`

	// BypassCookies bypasses varc if any named cookie is present.
	BypassCookies []string `json:"bypass_cookies,omitempty"`

	// BypassQuery bypasses varc if any named query parameter is present.
	BypassQuery []string `json:"bypass_query,omitempty"`

	// CacheAuthorization allows requests with Authorization to use the shared cache.
	// Leave false unless the key includes the auth scope or the origin ignores auth.
	CacheAuthorization bool `json:"cache_authorization,omitempty"`

	// CacheSetCookie allows responses carrying Set-Cookie to be cached.  The default
	// is to bypass those responses to avoid poisoning a shared cache.
	CacheSetCookie bool `json:"cache_set_cookie,omitempty"`

	// CachePrivate allows Cache-Control: private responses to be cached.
	CachePrivate bool `json:"cache_private,omitempty"`

	// CacheNoStore allows Cache-Control: no-store responses to be cached.
	CacheNoStore bool `json:"cache_no_store,omitempty"`

	// StaleIfError serves a cached copy for the requested range when the origin
	// probe/open path fails.  This is range-scoped and does not refresh in background.
	StaleIfError caddy.Duration `json:"stale_if_error,omitempty"`

	// CacheOnly serves only already-cached complete/range data.  No upstream probe
	// or fetch is performed on misses.
	CacheOnly bool `json:"cache_only,omitempty"`

	// PassThru calls the next Caddy handler when this handler cannot serve.  This
	// is useful when varc should only accelerate a subset of paths.
	PassThru bool `json:"pass_thru,omitempty"`

	// DebugHeaders adds X-Varc-* response headers.
	DebugHeaders bool `json:"debug_headers,omitempty"`

	// AdminPath enables JSON operator endpoints on the same handler.  By default it
	// is loopback-only unless AdminAllowRemote is true.  AdminToken adds bearer-token auth.
	AdminPath string `json:"admin_path,omitempty"`

	// AdminToken requires Authorization: Bearer <token> or X-Varc-Admin-Token.
	AdminToken string `json:"admin_token,omitempty"`

	// AdminAllowRemote allows non-loopback clients to reach AdminPath.  Prefer to
	// keep this false and protect admin routes with Caddy matchers/auth.
	AdminAllowRemote bool `json:"admin_allow_remote,omitempty"`

	// Timeouts and transport tuning.
	Timeout         caddy.Duration `json:"timeout,omitempty"`
	ProbeTimeout    caddy.Duration `json:"probe_timeout,omitempty"`
	DialTimeout     caddy.Duration `json:"dial_timeout,omitempty"`
	TLSHandshake    caddy.Duration `json:"tls_handshake_timeout,omitempty"`
	ResponseTimeout caddy.Duration `json:"response_header_timeout,omitempty"`
	IdleConnTimeout caddy.Duration `json:"idle_conn_timeout,omitempty"`
	MaxIdleConns    int            `json:"max_idle_conns,omitempty"`

	// Header forwarding.  StaticHeaders are sent to origin after placeholder
	// expansion.  ForwardHeaders copies named request headers to origin.
	StaticHeaders  http.Header `json:"headers,omitempty"`
	ForwardHeaders []string    `json:"forward_headers,omitempty"`

	// Cache tuning.  These map directly onto varc.Options.
	BlockSize         int64          `json:"block_size,omitempty"`
	ChunkSize         int64          `json:"chunk_size,omitempty"`
	ChunkSizeLimit    int64          `json:"chunk_size_limit,omitempty"`
	ChunkStreams      int            `json:"chunk_streams,omitempty"`
	MaxInflightBytes  int64          `json:"max_inflight_bytes,omitempty"`
	CacheMaxAge       caddy.Duration `json:"cache_max_age,omitempty"`
	CacheMaxSize      int64          `json:"cache_max_size,omitempty"`
	CacheMinFreeSpace int64          `json:"cache_min_free_space,omitempty"`
	CachePollInterval caddy.Duration `json:"cache_poll_interval,omitempty"`
	ReadAhead         int64          `json:"read_ahead,omitempty"`
	ShardLevel        int            `json:"shard_level,omitempty"`
	SyncWrites        bool           `json:"sync_writes,omitempty"`
	CleanOnStart      bool           `json:"clean_on_start,omitempty"`
	VerifyChecksum    bool           `json:"verify_checksum,omitempty"`
	ReadRetryCount    int            `json:"read_retry_count,omitempty"`
	ReadRetryDelay    caddy.Duration `json:"read_retry_delay,omitempty"`

	logger *zap.Logger
	cache  *varc.Cache
	client *http.Client

	metrics handlerMetrics
	flights *flightGroup
}

// CaddyModule returns the Caddy module information.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.varc",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision prepares the HTTP client and varc cache.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger(h)
	if h.flights == nil {
		h.flights = newFlightGroup()
	}
	if h.CacheDir == "" {
		h.CacheDir = defaultCacheDir
	}
	if h.Timeout == 0 {
		h.Timeout = caddy.Duration(defaultTimeout)
	}
	if h.ProbeTimeout == 0 {
		h.ProbeTimeout = caddy.Duration(defaultProbeTimeout)
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          defaultInt(h.MaxIdleConns, 256),
		MaxIdleConnsPerHost:   defaultInt(h.MaxIdleConns, 256),
		IdleConnTimeout:       defaultDuration(time.Duration(h.IdleConnTimeout), 90*time.Second),
		TLSHandshakeTimeout:   defaultDuration(time.Duration(h.TLSHandshake), 10*time.Second),
		ResponseHeaderTimeout: defaultDuration(time.Duration(h.ResponseTimeout), 30*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		DialContext: (&net.Dialer{
			Timeout:   defaultDuration(time.Duration(h.DialTimeout), 10*time.Second),
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	h.client = &http.Client{Transport: transport, Timeout: time.Duration(h.Timeout)}

	opt := varc.DefaultOptions()
	opt.CacheDir = h.CacheDir
	if h.BlockSize > 0 {
		opt.BlockSize = h.BlockSize
	}
	if h.ChunkSize > 0 {
		opt.ChunkSize = h.ChunkSize
	}
	if h.ChunkSizeLimit != 0 {
		opt.ChunkSizeLimit = h.ChunkSizeLimit
	}
	if h.ChunkStreams > 0 {
		opt.ChunkStreams = h.ChunkStreams
	}
	if h.MaxInflightBytes > 0 {
		opt.MaxInflightBytes = h.MaxInflightBytes
	}
	if h.CacheMaxAge != 0 {
		opt.CacheMaxAge = time.Duration(h.CacheMaxAge)
	}
	if h.CacheMaxSize != 0 {
		opt.CacheMaxSize = h.CacheMaxSize
	}
	if h.CacheMinFreeSpace != 0 {
		opt.CacheMinFreeSpace = h.CacheMinFreeSpace
	}
	if h.CachePollInterval != 0 {
		opt.CachePollInterval = time.Duration(h.CachePollInterval)
	}
	if h.ReadAhead != 0 {
		opt.ReadAhead = h.ReadAhead
	}
	if h.ShardLevel != 0 {
		opt.ShardLevel = h.ShardLevel
	}
	if h.ReadRetryCount != 0 {
		opt.ReadRetryCount = h.ReadRetryCount
	}
	if h.ReadRetryDelay != 0 {
		opt.ReadRetryDelay = time.Duration(h.ReadRetryDelay)
	}
	opt.SyncWrites = h.SyncWrites
	opt.CleanOnStart = h.CleanOnStart
	opt.VerifyChecksum = h.VerifyChecksum
	opt.Logger = zapPrintfLogger{log: h.logger}

	cache, err := varc.New(context.Background(), opt)
	if err != nil {
		return fmt.Errorf("create varc cache: %w", err)
	}
	h.cache = cache
	return nil
}

// Validate checks configuration mistakes before serving traffic.
func (h *Handler) Validate() error {
	if h.Upstream == "" && !h.CacheOnly {
		return fmt.Errorf("varc: upstream is required unless cache_only is enabled")
	}
	if h.Upstream != "" {
		raw := strings.TrimSpace(h.Upstream)
		if !strings.Contains(raw, "{") {
			u, err := url.Parse(raw)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("varc: upstream must be an absolute URL or placeholder template")
			}
		}
	}
	if h.AdminPath != "" && !strings.HasPrefix(h.AdminPath, "/") {
		return fmt.Errorf("varc: admin_path must start with /")
	}
	return nil
}

// Cleanup closes the varc cache when Caddy unloads this module.
func (h *Handler) Cleanup() error {
	if h.cache != nil {
		return h.cache.Close()
	}
	return nil
}

// ServeHTTP serves GET/HEAD requests through varc.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if h.isAdminRequest(r) {
		return h.serveAdmin(w, r)
	}
	h.ensureRuntime()
	start := time.Now()
	h.metrics.requests.Add(1)
	defer func() { h.metrics.observeDuration(time.Since(start)) }()

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.metrics.bypass.Add(1)
		if h.PassThru {
			return next.ServeHTTP(w, r)
		}
		w.Header().Set("Allow", "GET, HEAD")
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("varc: method %s is not supported", r.Method))
	}
	if h.cache == nil {
		return caddyhttp.Error(http.StatusInternalServerError, errors.New("varc: cache is not provisioned"))
	}

	repl := replacerFromRequest(r)
	sourceURL, err := h.resolveSourceURL(repl, r)
	if err != nil {
		h.metrics.errors.Add(1)
		return caddyhttp.Error(http.StatusBadGateway, err)
	}
	key := h.cacheKey(repl, r, sourceURL)

	if reason := h.requestBypassReason(r); reason != "" {
		h.metrics.bypass.Add(1)
		return h.proxyBypass(w, r, sourceURL, key, reason)
	}

	served, err := h.tryServeCache(w, r, key, sourceURL, "HIT")
	if err != nil {
		h.metrics.errors.Add(1)
		return err
	}
	if served {
		return nil
	}
	if h.CacheOnly {
		if h.PassThru {
			h.metrics.bypass.Add(1)
			return next.ServeHTTP(w, r)
		}
		h.metrics.cacheOnlyMisses.Add(1)
		return caddyhttp.Error(http.StatusNotFound, fmt.Errorf("varc: cache miss for %s", key))
	}

	remote, err := h.probeRemoteSingleflight(r.Context(), r, key, sourceURL)
	if err != nil {
		if h.canServeStale() {
			served, staleErr := h.tryServeCache(w, r, key, sourceURL, "STALE")
			if staleErr == nil && served {
				return nil
			}
		}
		if h.PassThru {
			h.logger.Warn("varc upstream probe failed; passing through", zap.Error(err), zap.String("url", sourceURL))
			h.metrics.bypass.Add(1)
			return next.ServeHTTP(w, r)
		}
		h.metrics.errors.Add(1)
		return caddyhttp.Error(http.StatusBadGateway, err)
	}
	if reason := h.responseBypassReason(remote); reason != "" {
		h.metrics.bypass.Add(1)
		return h.proxyBypass(w, r, sourceURL, key, reason)
	}
	if remote.Size < 0 {
		h.metrics.errors.Add(1)
		return caddyhttp.Error(http.StatusBadGateway, fmt.Errorf("varc: upstream did not provide a byte-addressable size"))
	}
	span, err := parseSingleRange(r.Header.Get("Range"), remote.Size)
	if err != nil {
		h.metrics.rangeNotSatisfiable.Add(1)
		writeRangeNotSatisfiable(w, remote.Size)
		return nil
	}
	if !rangeAllowedByIfRange(r, remote.ETag, remote.LastModified) {
		span = fullSpan(remote.Size)
	}
	if isNotModified(r, remote.ETag, remote.LastModified) && !span.Partial {
		writeNotModified(w, remote)
		return nil
	}

	src := &HTTPRangeSource{
		Context:      r.Context(),
		Client:       h.client,
		URL:          sourceURL,
		Headers:      h.originHeaders(r),
		Logger:       h.logger,
		ValidateSize: remote.Size,
	}
	fingerprint := remote.Fingerprint()
	opts := []varc.OpenOption{
		varc.WithFingerprint(fingerprint),
		varc.WithStrictFingerprint(),
		varc.WithAttr("source_url", sourceURL),
		varc.WithAttr("content_type", remote.ContentType),
		varc.WithAttr("etag", remote.ETag),
		varc.WithAttr("last_modified", formatHTTPTime(remote.LastModified)),
		varc.WithAttr("cache_control", remote.CacheControl),
	}
	if !remote.LastModified.IsZero() {
		opts = append(opts, varc.WithModTime(remote.LastModified))
	}
	vr, err := h.cache.Open(r.Context(), key, remote.Size, src, opts...)
	if err != nil {
		if h.canServeStale() {
			served, staleErr := h.tryServeCache(w, r, key, sourceURL, "STALE")
			if staleErr == nil && served {
				return nil
			}
		}
		h.metrics.errors.Add(1)
		return caddyhttp.Error(http.StatusBadGateway, fmt.Errorf("varc open: %w", err))
	}
	defer vr.Close()
	return h.serveReader(w, r, vr, span, remoteFromReader(vr, remote), "MISS", sourceURL, key)
}

func (h *Handler) tryServeCache(w http.ResponseWriter, r *http.Request, key, sourceURL, cacheStatus string) (bool, error) {
	vr, err := h.cache.Open(r.Context(), key, 0, nil, varc.WithCacheOnly())
	if err != nil {
		if errors.Is(err, varc.ErrCacheMiss) {
			return false, nil
		}
		h.logger.Warn("varc cache-only open failed", zap.Error(err), zap.String("key", key))
		return false, nil
	}
	defer vr.Close()

	remote := remoteFromReader(vr, RemoteObject{SourceURL: sourceURL})
	span, err := parseSingleRange(r.Header.Get("Range"), vr.Size())
	if err != nil {
		h.metrics.rangeNotSatisfiable.Add(1)
		writeRangeNotSatisfiable(w, vr.Size())
		return true, nil
	}
	if !rangeAllowedByIfRange(r, remote.ETag, remote.LastModified) {
		span = fullSpan(vr.Size())
	}
	if isNotModified(r, remote.ETag, remote.LastModified) && !span.Partial {
		writeNotModified(w, remote)
		return true, nil
	}
	cached, err := h.cache.RangeCached(key, span.Start, span.End)
	if err != nil || !cached {
		return false, nil
	}
	return true, h.serveReader(w, r, vr, span, remote, cacheStatus, sourceURL, key)
}

func (h *Handler) serveReader(w http.ResponseWriter, r *http.Request, vr *varc.Reader, span byteSpan, remote RemoteObject, cacheStatus, sourceURL, key string) error {
	setObjectHeaders(w, remote, span)
	if h.DebugHeaders {
		w.Header().Set("X-Varc-Cache", cacheStatus)
		w.Header().Set("X-Varc-Key", key)
		w.Header().Set("X-Varc-Source", sourceURL)
		w.Header().Set("X-Varc-Range", fmt.Sprintf("%d-%d", span.Start, span.End))
	}
	status := http.StatusOK
	if span.Partial {
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	h.metrics.bytesServed.Add(span.Length())
	switch cacheStatus {
	case "HIT":
		h.metrics.hits.Add(1)
		h.metrics.bytesFromCache.Add(span.Length())
	case "STALE":
		h.metrics.staleHits.Add(1)
		h.metrics.bytesFromCache.Add(span.Length())
	default:
		h.metrics.misses.Add(1)
	}
	if r.Method == http.MethodHead || span.Length() == 0 {
		return nil
	}
	buf := make([]byte, defaultResponseBuffer)
	_, err := io.CopyBuffer(w, io.NewSectionReader(vr, span.Start, span.Length()), buf)
	if err != nil {
		return fmt.Errorf("varc stream: %w", err)
	}
	return nil
}

func (h *Handler) resolveSourceURL(repl *caddy.Replacer, r *http.Request) (string, error) {
	raw := strings.TrimSpace(repl.ReplaceAll(h.Upstream, ""))
	if raw == "" {
		return "", fmt.Errorf("varc: empty upstream after placeholder expansion")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("varc: invalid upstream %q: %w", raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("varc: upstream must resolve to absolute URL")
	}
	if h.LowercaseHost {
		u.Host = strings.ToLower(u.Host)
	}
	if h.appendURI() {
		u.Path = joinURLPath(u.Path, r.URL.EscapedPath())
		if !h.IgnoreQuery {
			u.RawQuery = h.normalizeQuery(r.URL.RawQuery)
		}
	} else if !h.IgnoreQuery && (len(h.StripQuery) > 0 || h.SortQuery) {
		u.RawQuery = h.normalizeQuery(u.RawQuery)
	}
	return u.String(), nil
}

func (h *Handler) cacheKey(repl *caddy.Replacer, r *http.Request, sourceURL string) string {
	normalizedURI := r.URL.EscapedPath()
	if !h.IgnoreQuery {
		if q := h.normalizeQuery(r.URL.RawQuery); q != "" {
			normalizedURI += "?" + q
		}
	}
	if strings.TrimSpace(h.Key) == "" {
		key := sourceURL
		if len(h.VaryHeaders) > 0 {
			key += "|vary=" + h.varyKey(r)
		}
		return key
	}
	key := repl.ReplaceAll(h.Key, "")
	key = strings.ReplaceAll(key, "{uri}", r.URL.RequestURI())
	key = strings.ReplaceAll(key, "{normalized_uri}", normalizedURI)
	key = strings.ReplaceAll(key, "{normalized_query}", h.normalizeQuery(r.URL.RawQuery))
	key = strings.ReplaceAll(key, "{path}", r.URL.EscapedPath())
	key = strings.ReplaceAll(key, "{host}", r.Host)
	if len(h.VaryHeaders) > 0 {
		key += "|vary=" + h.varyKey(r)
	}
	if key == "" {
		return sourceURL
	}
	return key
}

func (h *Handler) normalizeQuery(raw string) string {
	if raw == "" || h.IgnoreQuery {
		return ""
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	for _, name := range h.StripQuery {
		delete(values, name)
	}
	if h.SortQuery || len(h.StripQuery) > 0 {
		return values.Encode()
	}
	return raw
}

func (h *Handler) varyKey(r *http.Request) string {
	if len(h.VaryHeaders) == 0 {
		return ""
	}
	parts := make([]string, 0, len(h.VaryHeaders))
	for _, name := range h.VaryHeaders {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" {
			continue
		}
		vals := append([]string(nil), r.Header.Values(canonical)...)
		sort.Strings(vals)
		parts = append(parts, canonical+"="+strings.Join(vals, ","))
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func (h *Handler) appendURI() bool {
	if h.AppendURI == nil {
		return true
	}
	return *h.AppendURI
}

func (h *Handler) originHeaders(r *http.Request) http.Header {
	headers := make(http.Header)
	for name, values := range h.StaticHeaders {
		for _, value := range values {
			repl := replacerFromRequest(r)
			headers.Add(name, repl.ReplaceAll(value, ""))
		}
	}
	for _, name := range h.ForwardHeaders {
		canonical := http.CanonicalHeaderKey(name)
		for _, value := range r.Header.Values(canonical) {
			headers.Add(canonical, value)
		}
	}
	headers.Set("Accept-Encoding", "identity")
	return headers
}

func setObjectHeaders(w http.ResponseWriter, remote RemoteObject, span byteSpan) {
	h := w.Header()
	h.Set("Accept-Ranges", "bytes")
	if remote.ContentType != "" {
		h.Set("Content-Type", remote.ContentType)
	} else {
		h.Set("Content-Type", "application/octet-stream")
	}
	if remote.ETag != "" {
		h.Set("ETag", remote.ETag)
	}
	if !remote.LastModified.IsZero() {
		h.Set("Last-Modified", remote.LastModified.UTC().Format(http.TimeFormat))
	}
	h.Set("Content-Length", strconv.FormatInt(span.Length(), 10))
	if span.Partial {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", span.Start, span.End-1, remote.Size))
	}
}

func writeRangeNotSatisfiable(w http.ResponseWriter, size int64) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
}

func writeNotModified(w http.ResponseWriter, remote RemoteObject) {
	if remote.ETag != "" {
		w.Header().Set("ETag", remote.ETag)
	}
	if !remote.LastModified.IsZero() {
		w.Header().Set("Last-Modified", remote.LastModified.UTC().Format(http.TimeFormat))
	}
	w.WriteHeader(http.StatusNotModified)
}

func remoteFromReader(r *varc.Reader, fallback RemoteObject) RemoteObject {
	out := fallback
	out.Size = r.Size()
	if v, ok := r.Attr("content_type"); ok && v != "" {
		out.ContentType = v
	}
	if v, ok := r.Attr("etag"); ok && v != "" {
		out.ETag = v
	}
	if v, ok := r.Attr("last_modified"); ok && v != "" {
		if t, err := http.ParseTime(v); err == nil {
			out.LastModified = t
		}
	}
	if v, ok := r.Attr("cache_control"); ok && v != "" {
		out.CacheControl = v
	}
	if out.LastModified.IsZero() {
		out.LastModified = r.ModTime()
	}
	return out
}

func replacerFromRequest(r *http.Request) *caddy.Replacer {
	if repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer); ok && repl != nil {
		return repl
	}
	repl := caddy.NewReplacer()
	repl.Set("http.request.host", r.Host)
	repl.Set("http.request.uri", r.URL.RequestURI())
	repl.Set("http.request.uri.path", r.URL.EscapedPath())
	repl.Set("host", r.Host)
	repl.Set("uri", r.URL.RequestURI())
	repl.Set("path", r.URL.EscapedPath())
	return repl
}

func defaultInt(v, d int) int {
	if v > 0 {
		return v
	}
	return d
}

func defaultDuration(v, d time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return d
}

func joinURLPath(basePath, reqPath string) string {
	if basePath == "" || basePath == "/" {
		if reqPath == "" {
			return "/"
		}
		return reqPath
	}
	if reqPath == "" || reqPath == "/" {
		return basePath
	}
	joined := path.Join(basePath, reqPath)
	if strings.HasSuffix(reqPath, "/") && !strings.HasSuffix(joined, "/") {
		joined += "/"
	}
	if strings.HasPrefix(basePath, "/") && !strings.HasPrefix(joined, "/") {
		joined = "/" + joined
	}
	return joined
}

var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
