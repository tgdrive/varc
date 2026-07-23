package varc

func mergeOptions(defaults, override Options) Options {
	if override.CacheDir != "" {
		defaults.CacheDir = override.CacheDir
	}
	if override.ChunkSize != 0 {
		defaults.ChunkSize = override.ChunkSize
	}
	if override.ChunkSizeLimit != 0 {
		defaults.ChunkSizeLimit = override.ChunkSizeLimit
	}
	if override.PreloadChunks != 0 {
		defaults.PreloadChunks = override.PreloadChunks
	}
	if override.CacheMaxAge != 0 {
		defaults.CacheMaxAge = override.CacheMaxAge
	}
	if override.CacheMaxSize != 0 {
		defaults.CacheMaxSize = override.CacheMaxSize
	}
	if override.CacheMinFreeSpace != 0 {
		defaults.CacheMinFreeSpace = override.CacheMinFreeSpace
	}
	if override.CachePollInterval != 0 {
		defaults.CachePollInterval = override.CachePollInterval
	}
	if override.FastFingerprint {
		defaults.FastFingerprint = true
	}
	if override.HandleCaching != 0 {
		defaults.HandleCaching = override.HandleCaching
	}
	if override.ShardLevel != 0 {
		defaults.ShardLevel = override.ShardLevel
	}
	if override.Logger != nil {
		defaults.Logger = override.Logger
	}
	if override.FileMode != 0 {
		defaults.FileMode = override.FileMode
	}
	if override.DirMode != 0 {
		defaults.DirMode = override.DirMode
	}
	if override.SyncWrites {
		defaults.SyncWrites = true
	}
	if override.NoBackground {
		defaults.NoBackground = true
	}
	if override.CleanOnStart {
		defaults.CleanOnStart = true
	}
	if override.ReadRetryCount != 0 {
		defaults.ReadRetryCount = override.ReadRetryCount
	}
	if override.ReadRetryDelay != 0 {
		defaults.ReadRetryDelay = override.ReadRetryDelay
	}
	if override.ReadIdleTimeout != 0 {
		defaults.ReadIdleTimeout = override.ReadIdleTimeout
	}
	if override.VerifyChecksum {
		defaults.VerifyChecksum = true
	}
	if override.TouchInterval != 0 {
		defaults.TouchInterval = override.TouchInterval
	}
	return defaults
}
