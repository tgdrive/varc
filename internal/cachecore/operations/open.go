package operations

import (
	"context"
	"io"

	"vfs-cache/internal/cachecore/fs"
)

// Open opens o with options. This is the narrow operation used by the parallel chunked reader.
func Open(ctx context.Context, o fs.Object, options ...fs.OpenOption) (io.ReadCloser, error) {
	return o.Open(ctx, options...)
}
