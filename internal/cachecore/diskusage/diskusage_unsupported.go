//go:build illumos || js || plan9 || solaris

package diskusage

func New(string) (Info, error) { return Info{}, ErrUnsupported }
