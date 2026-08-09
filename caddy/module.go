package caddycache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"

	"varc/cache"
	"varc/proxy"
	httpsource "varc/source/http"
)

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterHandlerDirective("varc", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("varc", httpcaddyfile.Before, "reverse_proxy")
}

// Handler is a Caddy HTTP middleware which serves GET and HEAD requests from
// the sparse object cache. Other HTTP methods continue to the next handler.
type Handler struct {
	Upstream          string          `json:"upstream,omitempty"`
	CacheDir          string          `json:"cache_dir,omitempty"`
	Key               string          `json:"key,omitempty"`
	Headers           http.Header     `json:"headers,omitempty"`
	CachePollInterval *caddy.Duration `json:"cache_poll_interval,omitempty"`
	CacheMaxAge       *caddy.Duration `json:"cache_max_age,omitempty"`
	CacheMaxSize      *int64          `json:"cache_max_size,omitempty"`
	CacheMinFreeSpace *int64          `json:"cache_min_free_space,omitempty"`
	CacheShardDepth   *int            `json:"cache_shard_depth,omitempty"`
	ChunkSize         *int64          `json:"chunk_size,omitempty"`
	ChunkSizeLimit    *int64          `json:"chunk_size_limit,omitempty"`
	ChunkStreams      *int            `json:"chunk_streams,omitempty"`
	ReadAhead         *int64          `json:"read_ahead,omitempty"`
	BufferSize        *int64          `json:"buffer_size,omitempty"`
	HandleCaching     *caddy.Duration `json:"handle_caching,omitempty"`
	LowLevelRetries   *int            `json:"low_level_retries,omitempty"`

	cache   *cache.Cache
	handler *proxy.Handler
}

// CaddyModule returns the module registration information.
func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.varc",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision initializes the HTTP source and local cache for this config load.
func (h *Handler) Provision(ctx caddy.Context) error {
	return h.provision(ctx)
}

func (h *Handler) provision(ctx context.Context) error {
	if h.Upstream == "" {
		return errors.New("varc: upstream is required")
	}
	if h.CacheDir == "" {
		return errors.New("varc: cache_dir is required")
	}

	var sourceKey proxy.KeyFunc
	var src *httpsource.Source
	if strings.Contains(h.Upstream, "{") {
		sourceKey = func(r *http.Request) (string, error) {
			repl, _ := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
			if repl == nil {
				repl = caddy.NewReplacer()
				r = caddyhttp.PrepareRequest(r, repl, nil, nil)
			}
			resolved, err := repl.ReplaceOrErr(h.Upstream, false, true)
			if err != nil {
				return "", fmt.Errorf("varc: expand upstream: %w", err)
			}
			u, err := parseUpstreamURL(resolved)
			if err != nil {
				return "", err
			}
			return u.String(), nil
		}
		src = httpsource.New(http.DefaultClient, func(_ context.Context, key string) (string, error) {
			return key, nil
		})
	} else {
		base, err := parseUpstreamURL(h.Upstream)
		if err != nil {
			return err
		}
		src = httpsource.New(http.DefaultClient, func(_ context.Context, key string) (string, error) {
			parts := strings.Split(strings.Trim(key, "/"), "/")
			resolved := base.JoinPath(parts...)
			return resolved.String(), nil
		})
	}
	if h.Headers != nil {
		src.Header = h.Headers.Clone()
	}

	opt := cache.DefaultOptions()
	if h.CachePollInterval != nil {
		opt.CachePollInterval = time.Duration(*h.CachePollInterval)
	}
	if h.CacheMaxAge != nil {
		opt.CacheMaxAge = time.Duration(*h.CacheMaxAge)
	}
	if h.CacheMaxSize != nil {
		opt.CacheMaxSize = *h.CacheMaxSize
	}
	if h.CacheMinFreeSpace != nil {
		opt.CacheMinFreeSpace = *h.CacheMinFreeSpace
	}
	if h.CacheShardDepth != nil {
		opt.CacheShardDepth = *h.CacheShardDepth
	}
	if h.ChunkSize != nil {
		opt.ChunkSize = *h.ChunkSize
	}
	if h.ChunkSizeLimit != nil {
		opt.ChunkSizeLimit = *h.ChunkSizeLimit
	}
	if h.ChunkStreams != nil {
		opt.ChunkStreams = *h.ChunkStreams
	}
	if h.ReadAhead != nil {
		opt.ReadAhead = *h.ReadAhead
	}
	if h.BufferSize != nil {
		opt.BufferSize = *h.BufferSize
	}
	if h.HandleCaching != nil {
		opt.HandleCaching = time.Duration(*h.HandleCaching)
	}
	if h.LowLevelRetries != nil {
		opt.LowLevelRetries = *h.LowLevelRetries
	}

	c, err := cache.New(ctx, h.CacheDir, src, opt)
	if err != nil {
		return fmt.Errorf("varc: initialize cache: %w", err)
	}
	h.cache = c
	h.handler = &proxy.Handler{Cache: c, Key: sourceKey}
	if h.Key != "" {
		h.handler.CacheKey = newCacheKeyFunc(h.Key)
	}
	return nil
}

func parseUpstreamURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("varc: parse upstream: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("varc: upstream scheme must be http or https")
	}
	if u.Host == "" {
		return nil, errors.New("varc: upstream host is required")
	}
	if u.Fragment != "" {
		return nil, errors.New("varc: upstream must not contain a URL fragment")
	}
	return u, nil
}

// Validate verifies that provisioning produced a usable handler.
func (h *Handler) Validate() error {
	if h.cache == nil || h.handler == nil {
		return errors.New("varc: module is not provisioned")
	}
	return nil
}

// Cleanup closes file handles and stops the cache cleaner on config unload.
func (h *Handler) Cleanup() error {
	if h.cache == nil {
		return nil
	}
	err := h.cache.Close()
	h.cache = nil
	h.handler = nil
	return err
}

// ServeHTTP serves cacheable reads and leaves other methods to the next Caddy
// handler, which makes it safe to place before reverse_proxy.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return next.ServeHTTP(w, r)
	}
	if h.handler == nil {
		return errors.New("varc: handler is not provisioned")
	}
	h.handler.ServeHTTP(w, r)
	return nil
}

func newCacheKeyFunc(template string) proxy.KeyFunc {
	template = normalizeCacheKeyTemplate(template)
	return func(r *http.Request) (string, error) {
		repl, _ := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
		if repl == nil {
			repl = caddy.NewReplacer()
			r = caddyhttp.PrepareRequest(r, repl, nil, nil)
		}
		key, err := repl.ReplaceOrErr(template, false, true)
		if err != nil {
			return "", fmt.Errorf("varc: expand cache key: %w", err)
		}
		if key == "" {
			return "", errors.New("varc: cache key expanded to an empty string")
		}
		return key, nil
	}
}

func normalizeCacheKeyTemplate(template string) string {
	return strings.NewReplacer(
		"{host}", "{http.request.host}",
		"{uri}", "{http.request.uri}",
	).Replace(template)
}

// UnmarshalCaddyfile parses:
//
//	varc <upstream> {
//	    cache_dir <path>
//	    key <template>
//	    max_size <bytes|KiB|MiB|GiB|TiB>
//	    min_free_space <bytes|KiB|MiB|GiB|TiB>
//	    max_age <duration>
//	    poll_interval <duration>
//	    chunk_size <bytes|KiB|MiB|GiB|TiB>
//	    chunk_size_limit <bytes|KiB|MiB|GiB|TiB>
//	    chunk_streams <count>
//	    read_ahead <bytes|KiB|MiB|GiB|TiB>
//	    buffer_size <bytes|KiB|MiB|GiB|TiB>
//	    handle_caching <duration>
//	    retries <count>
//	    header_up <field> <value>
//	}
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if !d.NextArg() {
			return d.ArgErr()
		}
		h.Upstream = d.Val()
		if d.NextArg() {
			return d.ArgErr()
		}

		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "cache_dir":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				h.CacheDir = value
			case "key":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				h.Key = value
			case "max_size":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid max_size: %v", err)
				}
				h.CacheMaxSize = &size
			case "min_free_space":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid min_free_space: %v", err)
				}
				h.CacheMinFreeSpace = &size
			case "shard_depth":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				depth, err := strconv.Atoi(value)
				if err != nil || depth < 0 || depth > 16 {
					return d.Errf("invalid shard_depth %q", value)
				}
				h.CacheShardDepth = &depth
			case "max_age":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				duration, err := caddy.ParseDuration(value)
				if err != nil {
					return d.Errf("invalid max_age: %v", err)
				}
				v := caddy.Duration(duration)
				h.CacheMaxAge = &v
			case "poll_interval":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				duration, err := caddy.ParseDuration(value)
				if err != nil {
					return d.Errf("invalid poll_interval: %v", err)
				}
				v := caddy.Duration(duration)
				h.CachePollInterval = &v
			case "chunk_size":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid chunk_size: %v", err)
				}
				h.ChunkSize = &size
			case "chunk_size_limit":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid chunk_size_limit: %v", err)
				}
				h.ChunkSizeLimit = &size
			case "chunk_streams":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				count, err := strconv.Atoi(value)
				if err != nil || count < 0 {
					return d.Errf("invalid chunk_streams %q", value)
				}
				h.ChunkStreams = &count
			case "read_ahead":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid read_ahead: %v", err)
				}
				h.ReadAhead = &size
			case "buffer_size":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				size, err := parseBytes(value)
				if err != nil {
					return d.Errf("invalid buffer_size: %v", err)
				}
				h.BufferSize = &size
			case "handle_caching":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				duration, err := caddy.ParseDuration(value)
				if err != nil {
					return d.Errf("invalid handle_caching: %v", err)
				}
				v := caddy.Duration(duration)
				h.HandleCaching = &v
			case "retries":
				value, err := oneArg(d)
				if err != nil {
					return err
				}
				count, err := strconv.Atoi(value)
				if err != nil || count < 0 {
					return d.Errf("invalid retries %q", value)
				}
				h.LowLevelRetries = &count
			case "header_up":
				args := d.RemainingArgs()
				if len(args) < 2 {
					return d.ArgErr()
				}
				if h.Headers == nil {
					h.Headers = make(http.Header)
				}
				h.Headers.Add(args[0], strings.Join(args[1:], " "))
			default:
				return d.Errf("unrecognized varc option %q", d.Val())
			}
		}
	}
	return nil
}

func oneArg(d *caddyfile.Dispenser) (string, error) {
	if !d.NextArg() {
		return "", d.ArgErr()
	}
	value := d.Val()
	if d.NextArg() {
		return "", d.ArgErr()
	}
	return value, nil
}

func parseBytes(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("empty size")
	}

	upper := strings.ToUpper(value)
	multipliers := []struct {
		suffix string
		value  int64
	}{
		{"TIB", 1 << 40},
		{"GIB", 1 << 30},
		{"MIB", 1 << 20},
		{"KIB", 1 << 10},
		{"TB", 1_000_000_000_000},
		{"GB", 1_000_000_000},
		{"MB", 1_000_000},
		{"KB", 1_000},
		{"B", 1},
	}
	multiplier := int64(1)
	for _, candidate := range multipliers {
		if strings.HasSuffix(upper, candidate.suffix) {
			upper = strings.TrimSpace(strings.TrimSuffix(upper, candidate.suffix))
			multiplier = candidate.value
			break
		}
	}
	n, err := strconv.ParseInt(upper, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	if n > (1<<63-1)/multiplier {
		return 0, fmt.Errorf("byte size %q overflows int64", value)
	}
	return n * multiplier, nil
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var handler Handler
	if err := handler.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}
	return &handler, nil
}

var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
