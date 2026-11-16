package outboundgroup

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type countingProvider struct {
	P.ProxyProvider
	proxies []C.Proxy
	// url is the group's: the records of real nodes are kept by it.
	url    string
	checks atomic.Int32
	// revive is what a check finds: the node alive again or not.
	revive func()
	// skips is how many checks do not reach the nodes: the first ones.
	skips int32
}

func (p *countingProvider) Proxies() []C.Proxy { return p.proxies }
func (p *countingProvider) Version() uint32    { return 1 }
func (p *countingProvider) Touch()             {}
func (p *countingProvider) HealthCheck() {
	if p.checks.Add(1) <= p.skips {
		return
	}
	// A check that reaches a node leaves a record of its probe.
	for _, proxy := range p.proxies {
		switch node := proxy.(type) {
		case *measuredProxy:
			node.probes = append(node.probes, C.DelayHistory{Time: time.Now()})
		case *adapter.Proxy:
			node.RecordSoftFailure(p.url, time.Now())
		}
	}
	if p.revive != nil {
		p.revive()
	}
}

func waitChecks(t *testing.T, p *countingProvider, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.checks.Load() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// A check that should not start gets the same time to show up.
	time.Sleep(50 * time.Millisecond)
	if got := p.checks.Load(); got != want {
		t.Fatalf("checks = %d, want %d", got, want)
	}
}

func reviveState(gb *GroupBase) (time.Duration, bool) {
	gb.reviveMux.Lock()
	defer gb.reviveMux.Unlock()
	return gb.reviveWait, time.Now().Before(gb.reviveNext)
}

// A connection that works through a node marked dead starts one check of the
// group; a live node starts none.
func TestADeadNodeThatWorksStartsOneCheck(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})

	gb.onDeadNodeDialed(&measuredProxy{name: "B", alive: true}, "", gb.healthCheck)
	waitChecks(t, provider, 0)

	for i := 0; i < 5; i++ {
		gb.onDeadNodeDialed(node, "", gb.healthCheck)
	}
	waitChecks(t, provider, 1)
	// The check left the node dead: the next one waits twice as long.
	if wait, pending := reviveState(gb); wait != 2*reviveFirstWait || !pending {
		t.Fatalf("wait %s, pending %v; want %s", wait, pending, 2*reviveFirstWait)
	}
	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 1)
}

// A check that brings the node back keeps the short wait.
func TestACheckThatRevivesKeepsTheShortWait(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}}
	provider.revive = func() { node.alive = true }
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})
	gb.reviveWait = 4 * reviveFirstWait

	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 1)
	if wait, _ := reviveState(gb); wait != reviveFirstWait {
		t.Fatalf("wait %s, want %s", wait, reviveFirstWait)
	}
}

// The wait stops doubling at its cap.
func TestTheWaitStopsAtItsCap(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})
	gb.reviveWait = reviveMaxWait

	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 1)
	if wait, _ := reviveState(gb); wait != reviveMaxWait {
		t.Fatalf("wait %s, want %s", wait, reviveMaxWait)
	}
}

// A check asked for right after another has ended does not reach the node:
// it is asked for once more, and the wait is not lengthened for it.
func TestACheckThatDidNotReachTheNodeIsAskedForAgain(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}, skips: 1}
	provider.revive = func() { node.alive = true }
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})

	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 2)
	if wait, _ := reviveState(gb); wait != reviveFirstWait {
		t.Fatalf("wait %s, want %s", wait, reviveFirstWait)
	}
}

// A check that reached the node neither time found nothing out: the wait
// before the next one stays as it was.
func TestChecksThatDidNotReachTheNodeDoNotMakeTheWaitLonger(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}, skips: 2}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})

	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 2)
	if wait, _ := reviveState(gb); wait != reviveFirstWait {
		t.Fatalf("wait %s, want %s", wait, reviveFirstWait)
	}
}

// A check of the group that is already running probes the node itself: a
// connection through the node starts no other and does not lengthen the wait.
func TestARunningCheckIsNotAskedForAgain(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})
	gb.failedTesting.Store(true)

	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 0)
	if wait, pending := reviveState(gb); wait != 0 || pending {
		t.Fatalf("wait %s, pending %v; want none", wait, pending)
	}
	gb.failedTesting.Store(false)
	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 1)
}

// A url-test group whose only node is marked dead checks itself when a
// connection through that node works.
func TestURLTestChecksWhenADeadNodeCarriesAConnection(t *testing.T) {
	const url = "https://probe.invalid/204"
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	node := adapter.NewProxy(outbound.NewDirect())
	node.RecordProbe(url, &C.ProbeResult{Held: true, Time: time.Now(), Err: context.DeadlineExceeded})
	if node.AliveForTestUrl(url) {
		t.Fatal("the node must start dead")
	}
	provider := &countingProvider{proxies: []C.Proxy{node}, url: url}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: url, TestTimeout: 5000},
		URLTestOption{}, node, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	conn, err := group.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP, DstIP: addr.AddrPort().Addr(), DstPort: uint16(addr.Port)})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	waitChecks(t, provider, 1)
}
