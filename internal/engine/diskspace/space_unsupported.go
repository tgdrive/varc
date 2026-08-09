//go:build illumos || js || plan9 || solaris

package diskspace

func New(string) (Info, error) { return Info{}, ErrUnsupported }
