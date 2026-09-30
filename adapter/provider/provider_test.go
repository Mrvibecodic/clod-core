package provider

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// An update of the subscription parses every node anew: a node it kept keeps
// what the checks know of it, a node whose server changed starts afresh.
func TestAnUpdateKeepsWhatTheChecksKnowOfAKeptNode(t *testing.T) {
	const url = "https://probe.invalid/204"
	node := func(name, server string) *adapter.Proxy {
		n, err := outbound.NewHttp(outbound.HttpOption{Name: name, Server: server, Port: 443})
		if err != nil {
			t.Fatal(err)
		}
		return adapter.NewProxy(n)
	}
	deadA, liveB, moved := node("A", "10.0.0.1"), node("B", "10.0.0.2"), node("C", "10.0.0.3")
	now := time.Now()
	deadA.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Err: context.DeadlineExceeded, Stage: C.ProbeStageDial})
	liveB.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Delay: 80, Satisfied: true, Elapsed: 300 * time.Millisecond})
	moved.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Err: context.DeadlineExceeded, Stage: C.ProbeStageDial})
	bp := &baseProvider{healthCheck: NewHealthCheck(nil, url, 5000, 0, false, nil)}
	t.Cleanup(bp.healthCheck.close)
	bp.setProxies([]C.Proxy{deadA, liveB, moved})

	a2, b2, c2 := node("A", "10.0.0.1"), node("B", "10.0.0.2"), node("C", "10.0.0.9")
	bp.setProxies([]C.Proxy{a2, b2, c2})
	if a2.AliveForTestUrl(url) {
		t.Fatal("a dead node came back alive with the update")
	}
	if got := b2.LastDelayForTestUrl(url); got != 80 {
		t.Fatalf("the delay of a kept node = %d, want 80", got)
	}
	if got := b2.LastProbeElapsed(url); got != 300*time.Millisecond {
		t.Fatalf("the probe time of a kept node = %s, want 300ms", got)
	}
	if !c2.AliveForTestUrl(url) || len(c2.DelayHistory()) != 0 {
		t.Fatal("a node whose server changed took the old server's state")
	}
	// Replaced again before a check: the state goes on.
	b3 := node("B", "10.0.0.2")
	bp.setProxies([]C.Proxy{a2, b3, c2})
	if got := b3.LastDelayForTestUrl(url); got != 80 {
		t.Fatalf("second replacement: delay %d, want 80", got)
	}
}
