//go:build !windows

package net

func isOSConnRefused(error) bool {
	return false
}
