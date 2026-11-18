package outboundgroup

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// probingProvider is a provider whose check writes what each node would
// answer, as the scheduled check does, and whose nodes can be replaced.
type probingProvider struct {
	P.ProxyProvider
	mu      sync.Mutex
	proxies []C.Proxy
	version uint32
	checks  atomic.Int32
}

func (p *probingProvider) Proxies() []C.Proxy {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.proxies
}

func (p *probingProvider) Version() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.version
}

func (p *probingProvider) Touch() {}

func (p *probingProvider) HealthCheck() {
	p.checks.Add(1)
	for _, proxy := range p.Proxies() {
		if n, ok := proxy.(interface{ asked() *askedProxy }); ok {
			n.asked().writeProbe()
		}
	}
}

func (p *probingProvider) replace(proxies ...C.Proxy) {
	p.mu.Lock()
	p.proxies = proxies
	p.version++
	p.mu.Unlock()
}

func (p *askedProxy) asked() *askedProxy { return p }

// writeProbe writes a probe of the node by someone else.
func (p *askedProxy) writeProbe() {
	if p.answers.Load() {
		p.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Delay: 10, Satisfied: true})
	} else {
		p.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Err: C.ErrNotSupport})
	}
}

func probingGroup(nodes ...*askedProxy) (*GroupBase, *probingProvider, doubt) {
	proxies := make([]C.Proxy, len(nodes))
	for i, n := range nodes {
		proxies[i] = n
	}
	provider := &probingProvider{proxies: proxies, version: 1}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 300, Providers: []P.ProxyProvider{provider}})
	return gb, provider, doubt{url: "http://probe.invalid/", all: gb.healthCheck, next: func(but C.Proxy) []C.Proxy {
		return aliveBut(gb.GetProxies(false), but, "")
	}}
}

func waitFor(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Connections that were being opened through the node when it was found dead
// fail after it. With another node alive they ask nothing: no check of the
// whole group probes the node, and the series probes it at its turns and
// gives it back to the group once it answers.
func TestLateFailuresOfTheNodeLeftLeaveTheSeriesAlone(t *testing.T) {
	a, b := deadNode("A"), askedNode("B", "192.0.2.2", true)
	gb, provider, d := probingGroup(a, b)
	var again atomic.Int32
	d.again = func() { again.Add(1) }
	pauses := []time.Duration{150 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond}
	gb.recheck(a, d, pauses)
	for i := 0; i < 5; i++ {
		gb.failedOf(a, time.Now(), d)
		time.Sleep(10 * time.Millisecond)
	}
	if got := provider.checks.Load(); got != 0 {
		t.Fatalf("late failures of the node left had the whole group checked %d times", got)
	}
	waitFor(t, 2*time.Second, "the series did not probe the node at its first turn", func() bool { return a.asks.Load() >= 1 })
	a.answers.Store(true)
	waitFor(t, 2*time.Second, "the series did not give the node back", func() bool { return !a.dead.Load() && again.Load() == 1 })
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked")
	}
}

// A probe of the node by someone else between two turns does not end the
// series: a fresh one stands for the turn's own probe, and the series goes
// on.
func TestAForeignProbeBetweenTurnsDoesNotEndTheSeries(t *testing.T) {
	a := deadNode("A")
	gb, _, d := probingGroup(a, askedNode("B", "192.0.2.2", true))
	gb.recheck(a, d, []time.Duration{200 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond})
	time.Sleep(100 * time.Millisecond)
	a.writeProbe()
	time.Sleep(200 * time.Millisecond)
	if got := a.asks.Load(); got != 0 {
		t.Fatalf("probed %d times at a turn right after a fresh probe of someone else", got)
	}
	waitFor(t, 2*time.Second, "the series ended after a probe of someone else", func() bool { return a.asks.Load() >= 1 })
	a.answers.Store(true)
	waitFor(t, 2*time.Second, "the node was not found alive", func() bool { return !a.dead.Load() })
}

// A node a provider update dropped from the group is probed no more.
func TestANodeDroppedFromTheGroupEndsTheSeries(t *testing.T) {
	a, b := deadNode("A"), askedNode("B", "192.0.2.2", true)
	gb, provider, d := probingGroup(a, b)
	gb.recheck(a, d, []time.Duration{100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond})
	provider.replace(b)
	time.Sleep(600 * time.Millisecond)
	if got := a.asks.Load(); got != 0 {
		t.Fatalf("a node dropped from the group was probed %d times", got)
	}
}
