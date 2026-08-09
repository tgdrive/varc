package diskusage

import "errors"

type Info struct {
	Free      uint64
	Available uint64
	Total     uint64
}

var ErrUnsupported = errors.New("disk usage unsupported on this platform")
