package varc

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func init() {
	httpcaddyfile.RegisterHandlerDirective("varc", parseCaddyfile)
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Handler
	if err := m.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}
	return &m, nil
}

// UnmarshalCaddyfile supports:
//
//	varc <upstream> {
//	    cache_dir /var/cache/caddy/varc
//	    key {host}{uri}
//	    append_uri on|off
//	    ignore_query on|off
//	    strip_query utm_source utm_medium fbclid
//	    sort_query on|off
//	    lowercase_host on|off
//	    vary_header Accept-Language
//	    bypass_header X-No-Cache
//	    bypass_cookie session
//	    bypass_query nocache
//	    cache_authorization on|off
//	    cache_set_cookie on|off
//	    cache_private on|off
//	    cache_no_store on|off
//	    stale_if_error 1h
//	    cache_only on|off
//	    pass_thru on|off
//	    debug_headers on|off
//	    admin_path /_varc
//	    admin_token {$VARC_ADMIN_TOKEN}
//	    admin_allow_remote off
//	    header Authorization "Bearer {$TOKEN}"
//	    forward_header Authorization
//	    block_size 1MiB
//	    chunk_size 128MiB
//	    chunk_streams 4
//	    max_size 500GiB
//	    max_age 168h
//	    read_ahead 16MiB
//	}
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		args := d.RemainingArgs()
		if len(args) > 1 {
			return d.ArgErr()
		}
		if len(args) == 1 {
			h.Upstream = args[0]
		}
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "upstream":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.Upstream = d.Val()
				if d.NextArg() {
					return d.ArgErr()
				}
			case "cache_dir":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.CacheDir = d.Val()
			case "key":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.Key = d.Val()
			case "append_uri":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.AppendURI = &v
			case "ignore_query":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.IgnoreQuery = v
			case "strip_query":
				vals := d.RemainingArgs()
				if len(vals) == 0 {
					return d.ArgErr()
				}
				h.StripQuery = append(h.StripQuery, vals...)
			case "sort_query":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.SortQuery = v
			case "lowercase_host":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.LowercaseHost = v
			case "vary_header":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.VaryHeaders = append(h.VaryHeaders, d.Val())
				if d.NextArg() {
					return d.ArgErr()
				}
			case "bypass_header":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.BypassHeaders = append(h.BypassHeaders, d.Val())
				if d.NextArg() {
					return d.ArgErr()
				}
			case "bypass_cookie":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.BypassCookies = append(h.BypassCookies, d.Val())
				if d.NextArg() {
					return d.ArgErr()
				}
			case "bypass_query":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.BypassQuery = append(h.BypassQuery, d.Val())
				if d.NextArg() {
					return d.ArgErr()
				}
			case "cache_authorization":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CacheAuthorization = v
			case "cache_set_cookie":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CacheSetCookie = v
			case "cache_private":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CachePrivate = v
			case "cache_no_store":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CacheNoStore = v
			case "stale_if_error":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.StaleIfError = caddy.Duration(dur)
			case "cache_only":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CacheOnly = v
			case "pass_thru":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.PassThru = v
			case "debug_headers":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.DebugHeaders = v
			case "sync_writes":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.SyncWrites = v
			case "clean_on_start":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.CleanOnStart = v
			case "verify_checksum":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.VerifyChecksum = v
			case "admin_path":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.AdminPath = d.Val()
			case "admin_token":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.AdminToken = d.Val()
			case "admin_allow_remote":
				v, err := nextBool(d)
				if err != nil {
					return err
				}
				h.AdminAllowRemote = v
			case "header":
				name, value, err := nextPair(d)
				if err != nil {
					return err
				}
				if h.StaticHeaders == nil {
					h.StaticHeaders = make(http.Header)
				}
				h.StaticHeaders.Add(name, value)
			case "forward_header":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.ForwardHeaders = append(h.ForwardHeaders, d.Val())
			case "timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.Timeout = caddy.Duration(dur)
			case "probe_timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.ProbeTimeout = caddy.Duration(dur)
			case "dial_timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.DialTimeout = caddy.Duration(dur)
			case "tls_handshake_timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.TLSHandshake = caddy.Duration(dur)
			case "response_header_timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.ResponseTimeout = caddy.Duration(dur)
			case "idle_conn_timeout":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.IdleConnTimeout = caddy.Duration(dur)
			case "max_idle_conns":
				v, err := nextInt(d)
				if err != nil {
					return err
				}
				h.MaxIdleConns = v
			case "block_size":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.BlockSize = v
			case "chunk_size":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.ChunkSize = v
			case "chunk_size_limit":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.ChunkSizeLimit = v
			case "chunk_streams":
				v, err := nextInt(d)
				if err != nil {
					return err
				}
				h.ChunkStreams = v
			case "max_inflight_bytes":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.MaxInflightBytes = v
			case "max_size", "cache_max_size":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.CacheMaxSize = v
			case "min_free_space", "cache_min_free_space":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.CacheMinFreeSpace = v
			case "max_age", "cache_max_age":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.CacheMaxAge = caddy.Duration(dur)
			case "poll_interval", "cache_poll_interval":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.CachePollInterval = caddy.Duration(dur)
			case "read_ahead":
				v, err := nextBytes(d)
				if err != nil {
					return err
				}
				h.ReadAhead = v
			case "shard_level":
				v, err := nextInt(d)
				if err != nil {
					return err
				}
				h.ShardLevel = v
			case "read_retry_count":
				v, err := nextInt(d)
				if err != nil {
					return err
				}
				h.ReadRetryCount = v
			case "read_retry_delay":
				dur, err := nextDuration(d)
				if err != nil {
					return err
				}
				h.ReadRetryDelay = caddy.Duration(dur)
			default:
				return d.Errf("unknown varc option %q", d.Val())
			}
		}
	}
	return nil
}

func nextBool(d *caddyfile.Dispenser) (bool, error) {
	if !d.NextArg() {
		return false, d.ArgErr()
	}
	v := strings.ToLower(d.Val())
	if d.NextArg() {
		return false, d.ArgErr()
	}
	switch v {
	case "on", "true", "yes", "1":
		return true, nil
	case "off", "false", "no", "0":
		return false, nil
	default:
		return false, d.Errf("expected boolean, got %q", v)
	}
}

func nextPair(d *caddyfile.Dispenser) (string, string, error) {
	if !d.NextArg() {
		return "", "", d.ArgErr()
	}
	name := d.Val()
	if !d.NextArg() {
		return "", "", d.ArgErr()
	}
	value := d.Val()
	if d.NextArg() {
		return "", "", d.ArgErr()
	}
	return name, value, nil
}

func nextInt(d *caddyfile.Dispenser) (int, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	v, err := strconv.Atoi(d.Val())
	if err != nil {
		return 0, d.Errf("invalid integer %q", d.Val())
	}
	if d.NextArg() {
		return 0, d.ArgErr()
	}
	return v, nil
}

func nextDuration(d *caddyfile.Dispenser) (time.Duration, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	v, err := caddy.ParseDuration(d.Val())
	if err != nil {
		return 0, d.Errf("invalid duration %q", d.Val())
	}
	if d.NextArg() {
		return 0, d.ArgErr()
	}
	return v, nil
}

func nextBytes(d *caddyfile.Dispenser) (int64, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	v, err := parseBytes(d.Val())
	if err != nil {
		return 0, d.Errf("invalid byte size %q: %v", d.Val(), err)
	}
	if d.NextArg() {
		return 0, d.ArgErr()
	}
	return v, nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	lower := strings.ToLower(s)
	mult := int64(1)
	for _, suffix := range []struct {
		name string
		mul  int64
	}{
		{"kib", 1024}, {"kb", 1000}, {"k", 1024},
		{"mib", 1024 * 1024}, {"mb", 1000 * 1000}, {"m", 1024 * 1024},
		{"gib", 1024 * 1024 * 1024}, {"gb", 1000 * 1000 * 1000}, {"g", 1024 * 1024 * 1024},
		{"tib", 1024 * 1024 * 1024 * 1024}, {"tb", 1000 * 1000 * 1000 * 1000}, {"t", 1024 * 1024 * 1024 * 1024},
	} {
		if strings.HasSuffix(lower, suffix.name) {
			mult = suffix.mul
			s = strings.TrimSpace(s[:len(s)-len(suffix.name)])
			break
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("negative size")
	}
	return int64(v * float64(mult)), nil
}
