//go:build !plan9 && !windows && !js

package mapbuffer

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func Alloc(size int) ([]byte, error) {
	mem, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return nil, fmt.Errorf("mmap: failed to allocate memory for buffer: %w", err)
	}
	return mem, nil
}

func Free(mem []byte) error {
	if err := unix.Munmap(mem); err != nil {
		return fmt.Errorf("mmap: failed to unmap memory: %w", err)
	}
	return nil
}
