package varc

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"

	"github.com/tgdrive/varc/httpcache"
)

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterHandlerDirective("varc", parseCaddyfile)

	// Register directive order so varc runs before reverse_proxy
	httpcaddyfile.RegisterDirectiveOrder("varc", httpcaddyfile.Before, "reverse_proxy")
}

// Handler implements a Caddy HTTP handler that proxies requests through the varc cache.
type Handler struct {
	// Upstream is the base URL to proxy requests to. If empty, the target
	// URL is resolved from the request (query param "url" or base64-encoded path).
	Upstream string `json:"upstream,omitempty"`

	// MetricsPath sets an optional path where cache metrics are served.
	// Example: "/varc/metrics"
	MetricsPath string `json:"metrics_path,omitempty"`

	httpcache.Options

	handler     *httpcache.Handler
	logger      *zap.Logger
	upstreamURL *url.URL
}

// CaddyModule returns the Caddy module information.
func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.handlers.varc",
		New: func() caddy.Module {
			return &Handler{
				Options: httpcache.DefaultOptions(),
			}
		},
	}
}

// Provision sets up the handler.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger(h)

	// Parse upstream URL if configured
	if h.Upstream != "" {
		parsedURL, err := url.Parse(h.Upstream)
		if err != nil {
			return fmt.Errorf("invalid upstream URL: %w", err)
		}
		h.upstreamURL = parsedURL
	}

	handler, err := httpcache.NewHandler(h.Options)
	if err != nil {
		return fmt.Errorf("failed to create cache handler: %w", err)
	}

	h.handler = handler

	if h.Upstream != "" {
		h.logger.Info("cache handler provisioned",
			zap.String("upstream", h.Upstream),
			zap.String("cache_dir", h.CacheDir),
		)
	} else {
		h.logger.Info("cache handler provisioned (dynamic upstream)",
			zap.String("cache_dir", h.CacheDir),
		)
	}
	return nil
}

// Validate ensures the configuration is valid.
func (h *Handler) Validate() error {
	// Validate upstream URL if configured
	if h.Upstream != "" {
		if h.upstreamURL == nil {
			return fmt.Errorf("upstream URL was not parsed")
		}
		if h.upstreamURL.Scheme != "http" && h.upstreamURL.Scheme != "https" {
			return fmt.Errorf("upstream URL must use http or https scheme, got %q", h.upstreamURL.Scheme)
		}
	}

	// Validate chunk_streams if provided
	if h.CacheChunkStreams < 0 {
		return fmt.Errorf("chunk_streams must be non-negative, got %d", h.CacheChunkStreams)
	}

	return nil
}

// Cleanup cleans up the handler resources.
func (h *Handler) Cleanup() error {
	if h.handler != nil {
		h.logger.Info("Shutting down cache handler")
		h.handler.Shutdown()
	}
	return nil
}

// ServeHTTP serves the HTTP request through the cache proxy.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// Serve metrics endpoint if configured
	if h.MetricsPath != "" && r.URL.Path == h.MetricsPath {
		h.handler.ServeMetrics(w)
		return nil
	}

	// Resolve the target URL
	targetURL := h.resolveTargetURL(r)

	// Wrap in panic recovery
	defer func() {
		if rec := recover(); rec != nil {
			h.logger.Error("panic in ServeHTTP",
				zap.Any("panic", rec),
				zap.String("url", r.URL.String()),
				zap.String("method", r.Method),
				zap.String("stack", string(debug.Stack())),
			)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	// If passthrough is enabled, use Caddy's ResponseRecorder to buffer 404 responses
	if h.Passthrough && next != nil {
		buf := new(bytes.Buffer)
		shouldBuffer := func(status int, header http.Header) bool {
			return status == http.StatusNotFound
		}
		rec := caddyhttp.NewResponseRecorder(w, buf, shouldBuffer)
		h.handler.Serve(rec, r, targetURL)
		if rec.Buffered() {
			return next.ServeHTTP(w, r)
		}
		return nil
	}

	h.handler.Serve(w, r, targetURL)
	return nil
}

// resolveTargetURL resolves the upstream URL from the configured upstream
// or from the request itself (query param or base64 path).
func (h *Handler) resolveTargetURL(r *http.Request) string {
	if h.upstreamURL != nil {
		// Static upstream: build URL from upstream base + request path
		fullURL := h.upstreamURL.JoinPath(r.URL.Path).String()
		if r.URL.RawQuery != "" {
			fullURL += "?" + r.URL.RawQuery
		}
		return fullURL
	}

	// Dynamic upstream: resolve from request
	if targetURL := r.URL.Query().Get("url"); targetURL != "" {
		return targetURL
	}

	// Check for Base64 URL in path (like standalone mode)
	if strings.HasPrefix(r.URL.Path, "/stream/") {
		encodedURL := strings.TrimPrefix(r.URL.Path, "/stream/")
		if decoded, err := base64.RawURLEncoding.DecodeString(encodedURL); err == nil {
			return string(decoded)
		}
		if decoded, err := base64.URLEncoding.DecodeString(encodedURL); err == nil {
			return string(decoded)
		}
	}

	return ""
}

// parseCaddyfile parses the Caddyfile configuration.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	v := &Handler{
		Options: httpcache.DefaultOptions(),
	}
	err := v.UnmarshalCaddyfile(h.Dispenser)
	return v, err
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		// First positional arg is upstream (optional)
		if d.NextArg() {
			h.Upstream = d.Val()
		}

		for d.NextBlock(0) {
			directive := d.Val()

			switch directive {
			case "passthrough":
				h.Passthrough = true
				continue
			case "metrics":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.MetricsPath = d.Val()
				continue
			case "upstream":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.Upstream = d.Val()
				continue
			}

			// Try to match directive with Options tags
			found := false
			val := reflect.ValueOf(&h.Options).Elem()
			typ := val.Type()
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				if field.Tag.Get("caddy") == directive {
					f := val.Field(i)
					switch f.Kind() {
					case reflect.Bool:
						f.SetBool(true)
					case reflect.String:
						if !d.NextArg() {
							return d.ArgErr()
						}
						f.SetString(d.Val())
					case reflect.Int:
						if !d.NextArg() {
							return d.ArgErr()
						}
						i, err := strconv.Atoi(d.Val())
						if err != nil {
							return d.Errf("invalid value for %s: %v", directive, err)
						}
						f.SetInt(int64(i))
					}
					found = true
					break
				}
			}

			if !found {
				return d.Errf("unknown subdirective '%s'", directive)
			}
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
