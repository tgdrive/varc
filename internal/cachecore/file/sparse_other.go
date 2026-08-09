//go:build !windows

package file

import "os"

const SetSparseImplemented = false

func SetSparse(*os.File) error { return nil }
