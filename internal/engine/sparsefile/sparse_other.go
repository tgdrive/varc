//go:build !windows

package sparsefile

import "os"

const SetSparseImplemented = false

func SetSparse(*os.File) error { return nil }
