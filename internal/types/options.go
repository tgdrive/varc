package types

import "time"

const mebi = 1048576 // 1 MiB in bytes

// Options is options for the cache engine
type Options struct {
	ChunkSize         int64 // if > 0 read files in chunks
	ChunkSizeLimit    int64 // if > ChunkSize double the chunk size after each chunk until reached
	ChunkStreams      int   // Number of download streams to use
	CacheMaxAge       time.Duration
	CacheMaxSize      int64
	CacheMinFreeSpace int64
	CachePollInterval time.Duration
	ReadAhead         int64         // bytes to read ahead in cache mode "full"
	FastFingerprint   bool          // if set use fast fingerprints
	HandleCaching     time.Duration // time to keep handle alive after last close
	CacheDir          string        // path to the cache directory on local disk

	// Logger is the logging backend. If nil, all log output is suppressed.
	Logger Logger
}

// Opt is the default options
var Opt = Options{
	CachePollInterval: 60 * time.Second,
	CacheMaxAge:       3600 * time.Second,
	CacheMaxSize:      -1,
	CacheMinFreeSpace: -1,
	ChunkSize:         128 * mebi,
	ChunkSizeLimit:    -1,
	HandleCaching:     5 * time.Second,
}

// Init checks options and sets defaults
func (opt *Options) Init() {
	if opt.Logger == nil {
		opt.Logger = NopLogger()
	}
}
