package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type namedAdapter struct {
	C.ProxyAdapter
	name string
}

func (n *namedAdapter) Name() string { return n.name }

// A node that was never measured at the group url (for example, left out of
// the checks by a filter) must not hold the group once other nodes have
// measured delays: only a node replaced by a provider update waits for its
// first check.
func TestURLTestNeverMeasuredNodeDoesNotHoldTheGroup(t *testing.T) {
	const url = "https://probe.invalid/204"
	mk := func(name string) *adapter.Proxy {
		return adapter.NewProxy(&namedAdapter{outbound.NewDirect(), name})
	}
	us, hk1, hk2 := mk("US-1"), mk("HK-1"), mk("HK-2")
	provider := &measuredProvider{proxies: []C.Proxy{us, hk1, hk2}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: url, TestTimeout: 5000},
		URLTestOption{Tolerance: 50}, us, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.Now(); got != "US-1" {
		t.Fatalf("before any check = %s", got)
	}
	now := time.Now()
	hk1.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Delay: 80, Satisfied: true})
	hk2.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Delay: 90, Satisfied: true})
	group.fastSingle.Reset()
	if got := group.Now(); got != "HK-1" {
		t.Fatalf("after the check = %s, want HK-1", got)
	}
}
