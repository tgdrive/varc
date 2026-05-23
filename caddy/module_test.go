package varc

import (
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/tgdrive/varc/httpcache"
)

func TestUnmarshalCaddyfile(t *testing.T) {
	t.Run("positional upstream + subdirectives", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`
			varc https://example.com {
				cache_dir /tmp/cache
				max_age 24h
				chunk_size 4M
				chunk_streams 4
				strip_query
				shard_level 3
				passthrough
			}
		`)

		v := &Handler{
			Options: httpcache.DefaultOptions(),
		}

		err := v.UnmarshalCaddyfile(d)
		if err != nil {
			t.Fatalf("failed to unmarshal caddyfile: %v", err)
		}

		if v.Upstream != "https://example.com" {
			t.Errorf("expected Upstream 'https://example.com', got '%s'", v.Upstream)
		}

		// Test passthrough flows to Options (used by proxy handler)
		if !v.Passthrough {
			t.Error("expected Passthrough to be true")
		}

		// Test reflection-mapped string options
		if v.CacheDir != "/tmp/cache" {
			t.Errorf("expected CacheDir '/tmp/cache', got '%s'", v.CacheDir)
		}
		if v.CacheMaxAge != "24h" {
			t.Errorf("expected CacheMaxAge '24h', got '%s'", v.CacheMaxAge)
		}
		if v.CacheChunkSize != "4M" {
			t.Errorf("expected CacheChunkSize '4M', got '%s'", v.CacheChunkSize)
		}

		// Test reflection-mapped integer options
		if v.CacheChunkStreams != 4 {
			t.Errorf("expected CacheChunkStreams 4, got %d", v.CacheChunkStreams)
		}
		if v.ShardLevel != 3 {
			t.Errorf("expected ShardLevel 3, got %d", v.ShardLevel)
		}

		// Test reflection-mapped boolean flags
		if !v.StripQuery {
			t.Error("expected StripQuery to be true")
		}
	})

	t.Run("upstream as subdirective", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`
			varc {
				upstream https://cdn.example.com
				cache_dir /var/cache/varc
			}
		`)

		v := &Handler{
			Options: httpcache.DefaultOptions(),
		}

		err := v.UnmarshalCaddyfile(d)
		if err != nil {
			t.Fatalf("failed to unmarshal caddyfile: %v", err)
		}

		if v.Upstream != "https://cdn.example.com" {
			t.Errorf("expected Upstream 'https://cdn.example.com', got '%s'", v.Upstream)
		}
		if v.CacheDir != "/var/cache/varc" {
			t.Errorf("expected CacheDir '/var/cache/varc', got '%s'", v.CacheDir)
		}
	})

	t.Run("dynamic upstream (no upstream configured)", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`
			varc {
				cache_dir /tmp/cache
			}
		`)

		v := &Handler{
			Options: httpcache.DefaultOptions(),
		}

		err := v.UnmarshalCaddyfile(d)
		if err != nil {
			t.Fatalf("failed to unmarshal caddyfile: %v", err)
		}

		if v.Upstream != "" {
			t.Errorf("expected Upstream to be empty for dynamic mode, got '%s'", v.Upstream)
		}
		if v.CacheDir != "/tmp/cache" {
			t.Errorf("expected CacheDir '/tmp/cache', got '%s'", v.CacheDir)
		}
	})

	t.Run("metrics path", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`
			varc https://example.com {
				metrics /varc/stats
			}
		`)

		v := &Handler{
			Options: httpcache.DefaultOptions(),
		}

		err := v.UnmarshalCaddyfile(d)
		if err != nil {
			t.Fatalf("failed to unmarshal caddyfile: %v", err)
		}

		if v.MetricsPath != "/varc/stats" {
			t.Errorf("expected MetricsPath '/varc/stats', got '%s'", v.MetricsPath)
		}
	})

	t.Run("unknown subdirective returns error", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`
			varc https://example.com {
				nonexistent_directive
			}
		`)

		v := &Handler{
			Options: httpcache.DefaultOptions(),
		}

		err := v.UnmarshalCaddyfile(d)
		if err == nil {
			t.Fatal("expected error for unknown subdirective, got nil")
		}
	})
}
