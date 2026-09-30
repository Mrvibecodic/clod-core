package provider

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// A provider starts its health check before it loads its proxies, and a
// subscription update replaces them while a round may be walking them. Run
// with -race: every round and every replacement here overlap.
func TestHealthCheckProxiesCanBeReplacedDuringARound(t *testing.T) {
	var proxies []C.Proxy
	for i := 0; i < 8; i++ {
		proxies = append(proxies, adapter.NewProxy(outbound.NewDirect()))
	}
	hc := NewHealthCheck(nil, "http://127.0.0.1:1/", 1, 300, true, nil)
	defer hc.close()

	replaced := make(chan struct{})
	go func() {
		defer close(replaced)
		for i := 0; i < 64; i++ {
			hc.setProxies(proxies[:i%len(proxies)+1])
		}
	}()
	for i := 0; i < 32; i++ {
		hc.check()
	}
	<-replaced
}

// A provider update while a round runs: the round takes the new list before
// it ends, the new nodes are not left for the next interval.
func TestNodesOfAnUpdateDuringARoundAreProbed(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "stall")
	stale := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{stale}, target.url(), 2000, 0, false, nil)
	t.Cleanup(hc.close)
	go hc.check()
	time.Sleep(300 * time.Millisecond)
	fresh := adapter.NewProxy(outbound.NewDirect())
	hc.setProxies([]C.Proxy{fresh})
	hc.check()
	if len(fresh.DelayHistoryForTestUrl(target.url())) == 0 {
		t.Fatal("the node of the update was not probed")
	}
}

// A provider update right after a round is probed at once, not answered with
// the round just ended.
func TestNodesOfAnUpdateRightAfterARoundAreProbed(t *testing.T) {
	target := newProbeTarget(t, 0)
	first := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{first}, target.url(), 2000, 0, false, nil)
	t.Cleanup(hc.close)
	hc.check()
	fresh := adapter.NewProxy(outbound.NewDirect())
	hc.setProxies([]C.Proxy{fresh})
	hc.check()
	if len(fresh.DelayHistoryForTestUrl(target.url())) == 0 {
		t.Fatal("the node of the update was not probed")
	}
}
