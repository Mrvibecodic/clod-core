package net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestIsConnRefused(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	for name, err := range map[string]error{
		"wrapped by the outbound": fmt.Errorf("1.2.3.4:443 connect error: %w", refused),
		"joined by the dialer":    errors.Join(fmt.Errorf("connect failed: %w", refused)),
		"chain lost":              errors.New("dial tcp 1.2.3.4:443: connect: connection refused"),
		"chain lost, windows":     errors.New("connectex: No connection could be made because the target machine actively refused it."),
	} {
		if !IsConnRefused(err) {
			t.Errorf("%s: %v must count as refused", name, err)
		}
	}
	for name, err := range map[string]error{
		"no error": nil,
		"timeout":  fmt.Errorf("connect error: %w", context.DeadlineExceeded),
		"reset":    &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)},
	} {
		if IsConnRefused(err) {
			t.Errorf("%s: %v must not count as refused", name, err)
		}
	}
}
