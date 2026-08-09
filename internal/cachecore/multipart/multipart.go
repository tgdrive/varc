package multipart

import "vfs-cache/internal/cachecore/pool"

const BufferSize = pool.BufferSize

func NewRW() *pool.RW { return pool.NewRW(pool.Global()) }
