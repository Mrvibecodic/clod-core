package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// followed is the node the group's series of probes follows and how many
// series have started.
func followed(gb *GroupBase) (C.Proxy, int32) {
	gb.recheckMux.Lock()
	defer gb.recheckMux.Unlock()
	return gb.recheckOf, gb.rechecks.Load()
}

// The series sleeps for seconds: a newer one ends it before it probes the
// nodes of the test, which cannot be probed.
func endSeries(t *testing.T, gb *GroupBase) {
	t.Cleanup(func() { gb.rechecks.Add(1) })
}

// A url-test group that leaves a node because the node is dead has it probed
// again, whoever found it dead: once per leave.
func TestURLTestHasTheDeadNodeItLeftProbedAgain(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	endSeries(t, group.GroupBase)
	group.fast(false)
	if node, series := followed(group.GroupBase); node != nil || series != 0 {
		t.Fatal("a series started with no node left")
	}
	// The scheduled check marks A dead.
	a.alive, a.delay = false, 0xffff
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s, want B", got.Name())
	}
	group.fastSingle.Reset()
	group.fast(false)
	if node, series := followed(group.GroupBase); node != C.Proxy(a) || series != 1 {
		t.Fatalf("series %d of %v, want one of A", series, node)
	}
}

// A check that found the node dead has started its series already: the group
// leaving the node then starts no second one.
func TestTheLeaveAfterACheckStartsNoSecondSeries(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	endSeries(t, group.GroupBase)
	group.fast(false)
	a.alive, a.delay = false, 0xffff
	group.leftDead(a, group.doubt(a))
	group.fast(false)
	if node, series := followed(group.GroupBase); node != C.Proxy(a) || series != 1 {
		t.Fatalf("series %d of %v, want one of A", series, node)
	}
}

// With every node dead the group goes nowhere, and starts no series.
func TestURLTestStartsNoSeriesWithEveryNodeDead(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	endSeries(t, group.GroupBase)
	group.fast(false)
	a.alive, a.delay = false, 0xffff
	b.alive, b.delay = false, 0xffff
	group.fastSingle.Reset()
	group.fast(false)
	if _, series := followed(group.GroupBase); series != 0 {
		t.Fatal("a series started though the group went nowhere")
	}
}

// A fallback group that moves off its dead node has it probed again: it goes
// back to it as soon as it answers.
func TestFallbackHasTheDeadNodeItLeftProbedAgain(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 100, alive: true}
	provider := &measuredProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewFallback(GroupCommonOption{Name: "fb", URL: "https://probe.invalid/"}, FallbackOption{}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	endSeries(t, group.GroupBase)
	if got := group.findAliveProxy(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want A", got.Name())
	}
	a.alive = false
	for i := 0; i < 3; i++ {
		if got := group.findAliveProxy(false); got != C.Proxy(b) {
			t.Fatalf("chosen %s, want B", got.Name())
		}
	}
	if node, series := followed(group.GroupBase); node != C.Proxy(a) || series != 1 {
		t.Fatalf("series %d of %v, want one of A", series, node)
	}
	a.alive = true
	if got := group.findAliveProxy(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s after A answered, want A", got.Name())
	}
	if _, series := followed(group.GroupBase); series != 1 {
		t.Fatal("moving back to an alive node started a series")
	}
}
