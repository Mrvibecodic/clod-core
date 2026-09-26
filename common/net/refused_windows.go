package net

import (
	"errors"
	"syscall"
)

// WSAECONNREFUSED: Windows reports a closed port with its own code, not with
// syscall.ECONNREFUSED.
const wsaeconnrefused = syscall.Errno(10061)

func isOSConnRefused(err error) bool {
	return errors.Is(err, wsaeconnrefused)
}
