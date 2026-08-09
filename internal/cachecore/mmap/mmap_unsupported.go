//go:build plan9 || js

package mmap

func Alloc(size int) ([]byte, error) { return make([]byte, size), nil }
func Free([]byte) error              { return nil }
