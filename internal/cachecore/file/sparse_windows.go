//go:build windows

package file

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

const SetSparseImplemented = true

func SetSparse(out *os.File) error {
	var bytesReturned uint32
	err := syscall.DeviceIoControl(syscall.Handle(out.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &bytesReturned, nil)
	if err != nil {
		return fmt.Errorf("DeviceIoControl FSCTL_SET_SPARSE: %w", err)
	}
	return nil
}
