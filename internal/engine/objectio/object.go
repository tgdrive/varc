package objectio

import (
	"context"
	"io"
)

// Span describes an inclusive byte range. End < 0 means through EOF.
type Span struct {
	Start int64
	End   int64
}

// RangeSeeker is the range-aware seek interface used by chunk readers.
type RangeSeeker interface {
	RangeSeek(ctx context.Context, offset int64, whence int, length int64) (int64, error)
}

// Object is the narrow source contract consumed by the cache engine.
type Object interface {
	Size() int64
	Open(ctx context.Context, span *Span) (io.ReadCloser, error)
}

// Config contains read-buffer settings shared by the internal engine.
type Config struct {
	BufferSize      int64
	MaxBufferMemory int64
	UseMmap         bool
}

type configKey struct{}

var defaultConfig = Config{BufferSize: 16 * 1024 * 1024}

// WithConfig attaches engine read settings to ctx.
func WithConfig(ctx context.Context, cfg Config) context.Context {
	return context.WithValue(ctx, configKey{}, &cfg)
}

// GetConfig returns engine read settings from ctx, or standalone defaults.
func GetConfig(ctx context.Context) *Config {
	if ctx != nil {
		if cfg, ok := ctx.Value(configKey{}).(*Config); ok && cfg != nil {
			return cfg
		}
	}
	cfg := defaultConfig
	return &cfg
}
