package net

import (
	"fmt"
	"net"
	"os"
	"testing"
)

func TestWindowsRefusalWhateverItsText(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connectex", wsaeconnrefused)}
	if !IsConnRefused(fmt.Errorf("1.2.3.4:443 connect error: %w", refused)) {
		t.Fatal("WSAECONNREFUSED must count as refused")
	}
}
