// Package mapbuffer provides platform-backed buffer allocation helpers.
package mapbuffer

import "os"

// PageSize is the minimum allocation size.
var PageSize = os.Getpagesize()

func MustAlloc(size int) []byte {
	mem, err := Alloc(size)
	if err != nil {
		panic(err)
	}
	return mem
}

func MustFree(mem []byte) {
	if err := Free(mem); err != nil {
		panic(err)
	}
}
