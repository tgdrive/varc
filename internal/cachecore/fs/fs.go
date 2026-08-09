package fs

import (
	"context"
	"io"
)

// OpenOption describes an option passed to Object.Open.
type OpenOption interface {
	isOpenOption()
}

// RangeOption requests an inclusive byte range. End < 0 means through EOF.
type RangeOption struct {
	Start int64
	End   int64
}

func (*RangeOption) isOpenOption() {}

// HashesOption is retained so the copied chunked-reader call shape stays the same.
// The standalone cache does not calculate source hashes during reads.
type HashesOption struct {
	Hashes any
}

func (*HashesOption) isOpenOption() {}

// RangeSeeker is the range-aware seek interface used by the chunked reader.
type RangeSeeker interface {
	RangeSeek(ctx context.Context, offset int64, whence int, length int64) (int64, error)
}

// Object is the narrow part of the object contract consumed by the read path.
type Object interface {
	Size() int64
	Open(ctx context.Context, options ...OpenOption) (io.ReadCloser, error)
}

// ConfigInfo contains the global read settings consumed by the copied read path.
type ConfigInfo struct {
	BufferSize      int64
	MaxBufferMemory int64
	UseMmap         bool
}

type configKey struct{}

var defaultConfig = ConfigInfo{BufferSize: 16 * 1024 * 1024}

// WithConfig attaches read settings to ctx.
func WithConfig(ctx context.Context, ci ConfigInfo) context.Context {
	return context.WithValue(ctx, configKey{}, &ci)
}

// GetConfig returns read settings from ctx, or standalone defaults.
func GetConfig(ctx context.Context) *ConfigInfo {
	if ctx != nil {
		if ci, ok := ctx.Value(configKey{}).(*ConfigInfo); ok && ci != nil {
			return ci
		}
	}
	ci := defaultConfig
	return &ci
}

// Logging hooks intentionally do nothing in the internal compatibility layer.
// The cache/proxy packages own externally visible logging.
func Debugf(any, string, ...any) {}
func Infof(any, string, ...any)  {}
func Errorf(any, string, ...any) {}
func Logf(any, string, ...any)   {}
