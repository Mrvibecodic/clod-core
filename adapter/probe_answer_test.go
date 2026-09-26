package adapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
)

func TestDefiniteAnswers(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	for name, err := range map[string]error{
		"refused, wrapped by the outbound": fmt.Errorf("1.2.3.4:443 connect error: %w", refused),
		"name not found by the system":     &net.DNSError{Err: "no such host", Name: "node.example", IsNotFound: true},
	} {
		if !isDefiniteAnswer(err) {
			t.Errorf("%s: %v must count as an answer", name, err)
		}
	}

	for name, err := range map[string]error{
		"no error":                      nil,
		"timeout":                       fmt.Errorf("connect error: %w", context.DeadlineExceeded),
		"reset":                         &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)},
		"handshake closed":              errors.New("EOF"),
		"dns timeout":                   &net.DNSError{Err: "i/o timeout", Name: "node.example", IsTimeout: true},
		"core resolver, maybe a hiccup": fmt.Errorf("dns resolve failed: %w", resolver.ErrIPNotFound),
	} {
		if isDefiniteAnswer(err) {
			t.Errorf("%s: %v must stay retryable", name, err)
		}
	}
}

func TestAProbeOfAClosedPortIsAnAnswer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	proxy := NewProxy(outbound.NewDirect())
	ctx, held := C.WithHeldProbe(context.Background())
	_, _ = proxy.URLTest(ctx, "http://"+addr+"/generate_204", nil)

	if !held.Held || held.Err == nil {
		t.Fatalf("the probe of a closed port must fail: %+v", held)
	}
	if held.Stage != C.ProbeStageDial || !held.Answered || held.Retryable() {
		t.Fatalf("a closed port is an answer at the dial stage, not a stall: %+v", held)
	}
}

func TestRetryableFollowsTheStageAndTheAnswer(t *testing.T) {
	failed := errors.New("stalled")
	cases := []struct {
		result    C.ProbeResult
		retryable bool
	}{
		{C.ProbeResult{Stage: C.ProbeStageDial, Err: failed}, true},
		{C.ProbeResult{Stage: C.ProbeStageRequest, Err: failed}, true},
		{C.ProbeResult{Stage: C.ProbeStageDial, Err: failed, Answered: true}, false},
		{C.ProbeResult{Stage: C.ProbeStageStatus}, false},
		{C.ProbeResult{Stage: C.ProbeStageAddress, Err: failed}, false},
	}
	for _, c := range cases {
		if got := c.result.Retryable(); got != c.retryable {
			t.Errorf("%+v: Retryable() = %v, want %v", c.result, got, c.retryable)
		}
	}
}
