package outboundgroup

import (
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// reviveAfterOutage is a group after a long outage: the wait has grown to its
// cap and the last check, a minute ago, holds the next one back.
func reviveAfterOutage(nodes ...C.Proxy) (*GroupBase, *countingProvider) {
	provider := &countingProvider{proxies: nodes}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})
	gb.reviveWait = reviveMaxWait
	gb.reviveAt = time.Now().Add(-time.Minute)
	gb.reviveNext = time.Now().Add(reviveMaxWait - time.Minute)
	return gb, provider
}

// A node of the group alive since the last check means the outage that made
// the wait grow is over: the first connection that works through a dead node
// in the next one checks the group at once, and the wait starts short again.
func TestANewOutageStartsWithTheShortWait(t *testing.T) {
	a := &measuredProxy{name: "A", alive: false}
	b := &measuredProxy{name: "B", alive: false}
	b.probes = []C.DelayHistory{{Time: time.Now().Add(-30 * time.Second), Delay: 120}, {Time: time.Now().Add(-5 * time.Second)}}
	gb, provider := reviveAfterOutage(a, b)
	gb.onDeadNodeDialed(a, "", gb.healthCheck)
	waitChecks(t, provider, 1)
	// The check left the node dead: the short wait doubled once.
	if wait, pending := reviveState(gb); wait != 2*reviveFirstWait || !pending {
		t.Fatalf("wait %s, pending %v; want %s", wait, pending, 2*reviveFirstWait)
	}
}

// With no node alive since the last check it is the same outage, and its
// wait holds.
func TestTheSameOutageKeepsItsWait(t *testing.T) {
	a := &measuredProxy{name: "A", alive: false}
	b := &measuredProxy{name: "B", alive: false}
	b.probes = []C.DelayHistory{{Time: time.Now().Add(-2 * time.Minute), Delay: 120}, {Time: time.Now().Add(-5 * time.Second)}}
	gb, provider := reviveAfterOutage(a, b)
	gb.onDeadNodeDialed(a, "", gb.healthCheck)
	waitChecks(t, provider, 0)
	if wait, _ := reviveState(gb); wait != reviveMaxWait {
		t.Fatalf("wait %s, want %s", wait, reviveMaxWait)
	}
}

// A check that did not reach the node found nothing out: the next connection
// that works may ask again in a few seconds, not after the whole wait.
func TestACheckThatDidNotReachTheNodeIsRetriedSoon(t *testing.T) {
	node := &measuredProxy{name: "A", alive: false}
	provider := &countingProvider{proxies: []C.Proxy{node}, skips: 2}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000, MaxFailedTimes: 5, Providers: []P.ProxyProvider{provider}})
	gb.onDeadNodeDialed(node, "", gb.healthCheck)
	waitChecks(t, provider, 2)
	gb.reviveMux.Lock()
	next := time.Until(gb.reviveNext)
	gb.reviveMux.Unlock()
	if next > reviveUnreachedWait || next < reviveUnreachedWait-time.Second {
		t.Fatalf("the next check may start in %s, want %s", next, reviveUnreachedWait)
	}
}
