package cache

import (
	"fmt"
	"time"
)

const mebi = int64(1024 * 1024)

// Options configures the standalone sparse read cache.
type Options struct {
	CachePollInterval time.Duration
	CacheMaxAge       time.Duration
	CacheMaxSize      int64
	CacheMinFreeSpace int64
	CacheShardDepth   int

	ChunkSize      int64
	ChunkSizeLimit int64
	ChunkStreams   int
	ReadAhead      int64
	BufferSize     int64

	HandleCaching   time.Duration
	LowLevelRetries int
}

// DefaultOptions returns the read-cache defaults used by the extracted VFS path.
func DefaultOptions() Options {
	return Options{
		CachePollInterval: 60 * time.Second,
		CacheMaxAge:       time.Hour,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		CacheShardDepth:   1,
		ChunkSize:         128 * mebi,
		ChunkSizeLimit:    -1,
		ChunkStreams:      0,
		ReadAhead:         0,
		BufferSize:        16 * mebi,
		HandleCaching:     5 * time.Second,
		LowLevelRetries:   10,
	}
}

func (opt Options) validate() error {
	if opt.CachePollInterval < 0 || opt.CacheMaxAge < 0 || opt.CacheMaxSize < -1 || opt.CacheMinFreeSpace < -1 || opt.CacheShardDepth < 0 || opt.CacheShardDepth > 16 || opt.ChunkSizeLimit < -1 || opt.ReadAhead < 0 || opt.BufferSize < 0 || opt.HandleCaching < 0 || opt.LowLevelRetries < 0 {
		return fmt.Errorf("cache: invalid negative option")
	}
	return nil
}
