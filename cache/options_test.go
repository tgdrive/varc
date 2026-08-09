package cache

import (
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	got := DefaultOptions()
	want := Options{
		CachePollInterval: 60 * time.Second,
		CacheMaxAge:       time.Hour,
		CacheMaxSize:      -1,
		CacheMinFreeSpace: -1,
		CacheShardDepth:   1,
		ChunkSize:         128 * 1024 * 1024,
		ChunkSizeLimit:    -1,
		ChunkStreams:      0,
		ReadAhead:         0,
		BufferSize:        16 * 1024 * 1024,
		HandleCaching:     5 * time.Second,
		LowLevelRetries:   10,
	}
	if got != want {
		t.Fatalf("defaults = %+v, want %+v", got, want)
	}
}

func TestNegativeOptionsFailValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "poll interval", mutate: func(o *Options) { o.CachePollInterval = -time.Second }},
		{name: "max age", mutate: func(o *Options) { o.CacheMaxAge = -time.Second }},
		{name: "max size", mutate: func(o *Options) { o.CacheMaxSize = -2 }},
		{name: "min free space", mutate: func(o *Options) { o.CacheMinFreeSpace = -2 }},
		{name: "shard depth", mutate: func(o *Options) { o.CacheShardDepth = -1 }},
		{name: "chunk size limit", mutate: func(o *Options) { o.ChunkSizeLimit = -2 }},
		{name: "buffer size", mutate: func(o *Options) { o.BufferSize = -1 }},
		{name: "read ahead", mutate: func(o *Options) { o.ReadAhead = -1 }},
		{name: "handle caching", mutate: func(o *Options) { o.HandleCaching = -time.Second }},
		{name: "retries", mutate: func(o *Options) { o.LowLevelRetries = -1 }},
		{name: "shard depth too large", mutate: func(o *Options) { o.CacheShardDepth = 17 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opt := DefaultOptions()
			test.mutate(&opt)
			if err := opt.validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
