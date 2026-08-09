//go:build windows

package diskusage

import "golang.org/x/sys/windows"

func New(dir string) (info Info, err error) {
	dir16 := windows.StringToUTF16Ptr(dir)
	err = windows.GetDiskFreeSpaceEx(dir16, &info.Available, &info.Total, &info.Free)
	return info, err
}
