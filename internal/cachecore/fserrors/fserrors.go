package fserrors

import (
	"errors"
	"syscall"
)

func IsErrNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
