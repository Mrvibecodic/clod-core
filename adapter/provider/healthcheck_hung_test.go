package provider

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// stuckAdapter is a node whose first connections get stuck and do not watch
// their deadline, the way a stuck transport session does; the later ones go
// through.
type stuckAdapter struct {
	C.ProxyAdapter
	stuck   int32
	dials   atomic.Int32
	release chan struct{}
}

func (s *stuckAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if s.dials.Add(1) <= s.stuck {
		<-s.release
		return nil, errors.New("released")
	}
	return s.ProxyAdapter.DialContext(ctx, metadata)
}

func TestAProbeThatOutlivesItsDeadlineDoesNotHoldTheNode(t *testing.T) {
	target := newProbeTarget(t, 0)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	proxy := adapter.NewProxy(&stuckAdapter{ProxyAdapter: outbound.NewDirect(), stuck: 2, release: release})
	const timeout = 300 * time.Millisecond
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), uint(timeout/time.Millisecond), 0, false, nil)
	t.Cleanup(hc.close)

	// Both probes of the round get stuck: the round ends with them all the
	// same, and the node did not answer.
	round := make(chan struct{})
	go func() {
		hc.check()
		close(round)
	}()
	select {
	case <-round:
	case <-time.After(probeHedgeDelay + timeout + probeAbandonGrace + 2*time.Second):
		t.Fatal("the round waited for a probe past its deadline")
	}
	if proxy.AliveForTestUrl(target.url()) {
		t.Fatal("a node that did not answer within the timeout stays alive")
	}

	// The connections work again: the next probe reaches the node instead of
	// waiting for the stuck ones.
	ctx, cancel := context.WithTimeout(context.Background(), timeout+ProbeHedgeDelay+C.ProbeReserve)
	defer cancel()
	if _, err := ProbeNode(ctx, proxy, target.url(), nil, timeout); err != nil {
		t.Fatalf("the node is probed past its stuck probes: %v", err)
	}
	if !proxy.AliveForTestUrl(target.url()) {
		t.Fatal("a node that answered stays dead")
	}
}
