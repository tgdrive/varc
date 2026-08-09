package ioerrors

import (
	"errors"
	"syscall"
)

func IsNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
