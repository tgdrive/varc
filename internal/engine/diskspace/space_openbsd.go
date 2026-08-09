//go:build openbsd

package diskspace

import "golang.org/x/sys/unix"

func New(dir string) (info Info, err error) {
	var statfs unix.Statfs_t
	if err = unix.Statfs(dir, &statfs); err != nil {
		return info, err
	}
	info.Free = uint64(statfs.F_bfree) * uint64(statfs.F_bsize)
	info.Available = uint64(statfs.F_bavail) * uint64(statfs.F_bsize)
	info.Total = uint64(statfs.F_blocks) * uint64(statfs.F_bsize)
	return info, nil
}
