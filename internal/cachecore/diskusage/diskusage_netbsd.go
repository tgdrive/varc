//go:build netbsd

package diskusage

import "golang.org/x/sys/unix"

func New(dir string) (info Info, err error) {
	var statfs unix.Statvfs_t
	if err = unix.Statvfs(dir, &statfs); err != nil {
		return info, err
	}
	info.Free = uint64(statfs.Bfree) * uint64(statfs.Bsize)
	info.Available = uint64(statfs.Bavail) * uint64(statfs.Bsize)
	info.Total = uint64(statfs.Blocks) * uint64(statfs.Bsize)
	return info, nil
}
