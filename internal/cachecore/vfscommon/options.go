package vfscommon

type Options struct {
	ChunkSize      int64
	ChunkSizeLimit int64
	ChunkStreams   int
	ReadAhead      int64
}
