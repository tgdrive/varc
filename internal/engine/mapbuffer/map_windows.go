//go:build windows

package mapbuffer

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

func Alloc(size int) ([]byte, error) {
	p, err := windows.VirtualAlloc(0, uintptr(size), windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		return nil, fmt.Errorf("mmap: failed to allocate memory for buffer: %w", err)
	}
	pp := unsafe.Pointer(&p)
	up := *(*unsafe.Pointer)(pp)
	return unsafe.Slice((*byte)(up), size), nil
}

func Free(mem []byte) error {
	p := unsafe.SliceData(mem)
	if err := windows.VirtualFree(uintptr(unsafe.Pointer(p)), 0, windows.MEM_RELEASE); err != nil {
		return fmt.Errorf("mmap: failed to unmap memory: %w", err)
	}
	return nil
}
