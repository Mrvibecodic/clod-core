package net

import (
	"errors"
	"strings"
	"syscall"
)

// IsConnRefused reports whether err says the other side refused the
// connection: its port is closed.
//
// The error code is checked first, because on Windows the text of a refused
// connection is the system's message, often in the system's language. The
// English phrases stay as a fallback for errors that lost their chain.
func IsConnRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) || isOSConnRefused(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "actively refused")
}
