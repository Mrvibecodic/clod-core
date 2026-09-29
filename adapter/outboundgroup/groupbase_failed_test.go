package outboundgroup

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type blockingProvider struct {
	P.ProxyProvider
	checks  atomic.Int32
	release chan struct{}
}

func (p *blockingProvider) HealthCheck() {
	p.checks.Add(1)
	<-p.release
}

// Failures that arrive while the group check runs must neither wait for it
// nor start a second full check as soon as it ends.
func TestGroupFailuresDuringACheckDoNotStartAnother(t *testing.T) {
	provider := &blockingProvider{release: make(chan struct{})}
	gb := NewGroupBase(GroupBaseOption{
		Name:           "auto",
		TestTimeout:    60000,
		MaxFailedTimes: 2,
		Providers:      []P.ProxyProvider{provider},
	})
	failure := errors.New("dial failed")

	for i := 0; i < 2; i++ {
		gb.onDialFailed(C.Vless, failure, gb.healthCheck)
	}
	deadline := time.Now().Add(2 * time.Second)
	for provider.checks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if provider.checks.Load() != 1 {
		t.Fatalf("checks started = %d, want 1", provider.checks.Load())
	}

	for i := 0; i < 10; i++ {
		gb.onDialFailed(C.Vless, failure, gb.healthCheck)
	}
	// Every failure is counted while the check still runs: none of them may
	// wait for it to end.
	counted := func() int {
		gb.failedTestMux.Lock()
		defer gb.failedTestMux.Unlock()
		return gb.failedTimes
	}
	deadline = time.Now().Add(2 * time.Second)
	for counted() < 12 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := counted(); got != 12 {
		t.Fatalf("failures counted during the check = %d, want 12", got)
	}
	close(provider.release)
	time.Sleep(200 * time.Millisecond)

	if got := provider.checks.Load(); got != 1 {
		t.Fatalf("checks started = %d, want 1", got)
	}
}
