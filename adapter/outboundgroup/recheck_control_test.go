package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// scriptedNode answers its probes as the script says, in order, the last
// answer from then on, each at once; it has a delay as a real node does.
type scriptedNode struct {
	*askedProxy
	delay  uint16
	mu     sync.Mutex
	script []bool
}

func scripted(name string, delay uint16, script ...bool) *scriptedNode {
	return &scriptedNode{askedProxy: askedNode(name, "192.0.2.9", false), delay: delay, script: script}
}

func (n *scriptedNode) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	asks := int(n.asks.Add(1))
	n.mu.Lock()
	answers := n.script[len(n.script)-1]
	if asks <= len(n.script) {
		answers = n.script[asks-1]
	}
	n.mu.Unlock()
	result := C.HeldProbe(ctx)
	if answers {
		*result = C.ProbeResult{Held: true, Time: time.Now(), Delay: n.delay, Satisfied: true}
		return n.delay, nil
	}
	err := errors.New("down")
	*result = C.ProbeResult{Held: true, Time: time.Now(), Err: err, Stage: C.ProbeStageAddress}
	return 0, err
}

func (n *scriptedNode) LastDelayForTestUrl(string) uint16 {
	if n.dead.Load() {
		return 0xffff
	}
	history := n.DelayHistoryForTestUrl("")
	if len(history) == 0 {
		return 0
	}
	return history[len(history)-1].Delay
}

func (n *scriptedNode) record(delays ...uint16) {
	for _, delay := range delays {
		if delay == 0 {
			n.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Err: C.ErrNotSupport})
		} else {
			n.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Delay: delay, Satisfied: true})
		}
	}
}

// controlGroup is a url-test group with the settings of the measurements: a probe timeout of 4
// s, so a quarter of it, a second, for each recent failure, and a tolerance
// of 150 ms. B is the node it is on, A the faster node it left dead.
func controlGroup(t *testing.T, a, b *scriptedNode) *URLTest {
	t.Helper()
	b.record(300)
	a.record(10, 10, 0)
	provider := &probingProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "http://probe.invalid/", TestTimeout: 4000},
		URLTestOption{Tolerance: 150}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s, want B", got.Name())
	}
	return group
}

// controlsOf is the control probes of the series, each after pause.
func controlsOf(pause time.Duration) []time.Duration {
	controls := make([]time.Duration, len(recheckControlPauses))
	for i := range controls {
		controls[i] = pause
	}
	return controls
}

// After the series finds the node alive, a url-test group makes its control
// probes of it; all clean, the node is ranked by them alone, and the group
// goes back to it at once, without waiting for its kept choice to run out.
func TestCleanControlProbesTakeTheGroupBack(t *testing.T) {
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	group.recheckEvery(a, group.doubt(a), []time.Duration{50 * time.Millisecond}, controlsOf(200*time.Millisecond), 0)
	waitFor(t, 2*time.Second, "the series did not find the node alive", func() bool { return a.asks.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	group.fastSingle.Reset()
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatal("the group went back to the node before the control probes")
	}
	waitFor(t, 2*time.Second, "the group did not go back to the node", func() bool { return group.CurrentNode() == C.Proxy(a) })
	if got := a.asks.Load(); got != 1+int32(len(recheckControlPauses)) {
		t.Fatalf("the node was probed %d times, want the probe that found it alive and %d control probes", got, len(recheckControlPauses))
	}
}

// A node up for a moment and down again does not have the group bounce: the
// control probe it fails starts the series over, and the group stays where
// it is all along.
func TestAFlappingNodeDoesNotTakeTheGroupBack(t *testing.T) {
	// Alive at a turn, dead at the control probe, then dead at a turn.
	var script []bool
	for i := 0; i < 10; i++ {
		script = append(script, true, false, false)
	}
	a, b := scripted("A", 10, script...), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	pause := 40 * time.Millisecond
	group.recheckEvery(a, group.doubt(a), []time.Duration{pause, pause, pause, pause}, controlsOf(pause), 0)
	// Probes of one host are spaced a quarter of a second apart.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		group.fastSingle.Reset()
		if got := group.fast(false); got != C.Proxy(b) {
			t.Fatalf("the group went to the flapping node after %d probes of it", a.asks.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if a.asks.Load() < 6 || group.rechecks.Load() < 2 {
		t.Fatalf("probes %d, series %d: the series did not start over", a.asks.Load(), group.rechecks.Load())
	}
	group.rechecks.Add(1)
}

// A newer series of the group stands: a control probe that fails does not
// start the old one over.
func TestAFailedControlProbeYieldsToANewerSeries(t *testing.T) {
	a, b := scripted("A", 10, true, false), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	group.recheckEvery(a, group.doubt(a), []time.Duration{20 * time.Millisecond}, controlsOf(200*time.Millisecond), 0)
	waitFor(t, 2*time.Second, "the series did not find the node alive", func() bool { return a.asks.Load() == 1 })
	group.rechecks.Add(1)
	time.Sleep(400 * time.Millisecond)
	if a.asks.Load() != 1 || group.rechecks.Load() != 2 {
		t.Fatalf("probes %d, series %d", a.asks.Load(), group.rechecks.Load())
	}
}

// A fallback group goes back to its node as soon as it is alive: no control
// probes.
func TestFallbackMakesNoControlProbes(t *testing.T) {
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	a.record(0)
	provider := &probingProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewFallback(GroupCommonOption{Name: "fb", URL: "http://probe.invalid/", TestTimeout: 4000}, FallbackOption{}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	group.recheckEvery(a, group.doubt(), []time.Duration{20 * time.Millisecond}, controlsOf(50*time.Millisecond), 0)
	waitFor(t, 2*time.Second, "the series did not find the node alive", func() bool { return !a.dead.Load() })
	time.Sleep(300 * time.Millisecond)
	if got := a.asks.Load(); got != 1 {
		t.Fatalf("the node was probed %d times", got)
	}
	if got := group.findAliveProxy(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want A", got.Name())
	}
}

// A node alive at every turn and dead at every control probe has its series
// start over recheckRestarts times in a row and no more: then only the
// scheduled checks probe it. The group leaving the node anew counts anew.
func TestAFlappingNodeHasItsSeriesStartOverThreeTimesAtMost(t *testing.T) {
	var script []bool
	for i := 0; i < 20; i++ {
		script = append(script, true, false)
	}
	a, b := scripted("A", 10, script...), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	pause := 10 * time.Millisecond
	d := group.doubt(a)
	// A turn and a control probe for the series and for each start over.
	want := int32(2 * (1 + recheckRestarts))
	group.recheckEvery(a, d, []time.Duration{pause, pause}, controlsOf(pause), 0)
	ended := func() bool { node, _ := followed(group.GroupBase); return node == nil }
	waitFor(t, 5*time.Second, "the series of the flapping node did not end", ended)
	time.Sleep(time.Second)
	if got := a.asks.Load(); got != want || !ended() {
		t.Fatalf("the node was probed %d times, want %d", got, want)
	}
	// A leave starts a series of its own, not one in place of another.
	group.recheckEvery(a, d, []time.Duration{pause, pause}, controlsOf(pause), 0)
	waitFor(t, 5*time.Second, "a new leave did not start the count anew", func() bool { return a.asks.Load() == 2*want && ended() })
	time.Sleep(time.Second)
	if got := a.asks.Load(); got != 2*want {
		t.Fatalf("the node was probed %d times, want %d", got, 2*want)
	}
}

// softControlNode has its last control probe pass at its second try: the
// first try is written down as a soft failure.
type softControlNode struct {
	*scriptedNode
	softAt int32
}

func (n *softControlNode) URLTest(ctx context.Context, url string, expected utils.IntRanges[uint16]) (uint16, error) {
	if n.asks.Load()+1 == n.softAt {
		n.RecordSoftFailure(url, time.Now())
	}
	return n.scriptedNode.URLTest(ctx, url, expected)
}

// Control probes passed with a try one of them needed still among the
// records the node is ranked by: the group is not asked to choose anew, the
// scheduled checks rank the node.
func TestControlProbesWithASoftFailureLeaveTheChoiceAlone(t *testing.T) {
	for _, soft := range []bool{false, true} {
		a, b := scripted("A", 10, true), scripted("B", 300, true)
		node := &softControlNode{scriptedNode: a}
		if soft {
			node.softAt = 1 + int32(len(recheckControlPauses))
		}
		b.record(300)
		a.record(10, 10, 0)
		group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "http://probe.invalid/", TestTimeout: 4000},
			URLTestOption{Tolerance: 150}, a, []P.ProxyProvider{&probingProvider{proxies: []C.Proxy{node, b}, version: 1}})
		if err != nil {
			t.Fatal(err)
		}
		var agains atomic.Int32
		d := group.doubt(node)
		d.again = func() { agains.Add(1) }
		group.recheckEvery(node, d, []time.Duration{10 * time.Millisecond}, controlsOf(10*time.Millisecond), 0)
		waitFor(t, 3*time.Second, "the series did not end", func() bool { n, _ := followed(group.GroupBase); return n == nil })
		if got := a.asks.Load(); got != 1+int32(len(recheckControlPauses)) {
			t.Fatalf("soft %t: the node was probed %d times", soft, got)
		}
		if want := map[bool]int32{false: 1, true: 0}[soft]; agains.Load() != want {
			t.Fatalf("soft %t: the group was asked to choose anew %d times, want %d", soft, agains.Load(), want)
		}
	}
}

// A node alive for a while after it was found alive, but not through the
// last control probe, is not gone back to: two control probes, 30 s, were
// all a node up for 40 s at a time had to pass, and the group came back and
// left again with each outage.
func TestANodeThatDiesBeforeTheLastControlProbeIsNotGoneBackTo(t *testing.T) {
	a, b := scripted("A", 10, true, true, true, false), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	endSeries(t, group.GroupBase)
	var agains atomic.Int32
	d := group.doubt(a)
	d.again = func() { agains.Add(1) }
	group.recheckEvery(a, d, []time.Duration{10 * time.Millisecond, time.Hour}, controlsOf(10*time.Millisecond), 0)
	want := 1 + int32(len(recheckControlPauses))
	waitFor(t, 3*time.Second, "the control probes did not run", func() bool { return a.asks.Load() == want })
	time.Sleep(300 * time.Millisecond)
	if agains.Load() != 0 || !a.dead.Load() {
		t.Fatalf("the group was asked to choose anew %d times, node dead %t", agains.Load(), a.dead.Load())
	}
}

// The node a url-test group went back to through control probes and left
// dead again soon after is gone back to by its scheduled checks alone: its
// next revival takes no control probes and asks for no new choice, for
// recheckReturnGuard from that leave. After that the control probes take it
// back again.
func TestANodeThatFailedItsReturnGetsNoControlProbesForAWhile(t *testing.T) {
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	var agains atomic.Int32
	d := group.doubt(a)
	d.again = func() { agains.Add(1) }
	controls := int32(len(recheckControlPauses))
	pause := 10 * time.Millisecond
	ended := func() bool { n, _ := followed(group.GroupBase); return n == nil }
	leave := func() {
		a.record(0)
		group.noteLeave(a)
		group.recheckEvery(a, d, []time.Duration{pause}, controlsOf(pause), 0)
	}

	// It passes all the control probes: the group goes back to it once.
	group.recheckEvery(a, d, []time.Duration{pause}, controlsOf(pause), 0)
	waitFor(t, 3*time.Second, "the series did not end", ended)
	if a.asks.Load() != 1+controls || agains.Load() != 1 {
		t.Fatalf("probes %d, choices anew %d, want %d and 1", a.asks.Load(), agains.Load(), 1+controls)
	}

	// Left dead within the guard of that return and alive again: no
	// control probes, no choice anew.
	leave()
	waitFor(t, 3*time.Second, "the series did not find the node alive", func() bool { return a.asks.Load() == 2+controls })
	waitFor(t, 3*time.Second, "the series did not end", ended)
	time.Sleep(300 * time.Millisecond)
	if a.asks.Load() != 2+controls || agains.Load() != 1 {
		t.Fatalf("probes %d, choices anew %d after a failed return, want %d and 1", a.asks.Load(), agains.Load(), 2+controls)
	}

	// The guard is over: the control probes take the node back again.
	group.recheckMux.Lock()
	group.returnedAt = time.Now().Add(-2 * recheckReturnGuard)
	group.returnGuardUntil = time.Now().Add(-time.Millisecond)
	group.recheckMux.Unlock()
	leave()
	waitFor(t, 3*time.Second, "the control probes did not take the node back", func() bool { return agains.Load() == 2 })
	if a.asks.Load() != 3+2*controls {
		t.Fatalf("probes %d, want %d", a.asks.Load(), 3+2*controls)
	}
}

// phasedNode is down for the first down of every period from its start, up
// for the rest of it, and up for good from steady on.
type phasedNode struct {
	*scriptedNode
	start                time.Time
	down, period, steady time.Duration
}

func (n *phasedNode) URLTest(ctx context.Context, url string, expected utils.IntRanges[uint16]) (uint16, error) {
	since := time.Since(n.start)
	up := since >= n.steady || since%n.period >= n.down
	n.mu.Lock()
	n.script = []bool{up}
	n.mu.Unlock()
	n.asks.Store(0)
	return n.scriptedNode.URLTest(ctx, url, expected)
}

// A node down for 20 s and up for 40 s over and over, left at
// 10 s: a control probe falls into a down phase each time, and the group
// does not go back to the node while it flaps. Once the node stays up, a
// minute of control probes takes the group back, once. A second of the pattern is a
// unit here.
func TestANodeDown20sUp40sIsNotGoneBackToWhileItFlaps(t *testing.T) {
	const unit = 50 * time.Millisecond
	scale := func(real []time.Duration) []time.Duration {
		scaled := make([]time.Duration, len(real))
		for i, d := range real {
			scaled[i] = d / time.Second * unit
		}
		return scaled
	}
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	// Down at 0-20, 60-80 and 120-140, up for good from 140.
	node := &phasedNode{scriptedNode: a, start: time.Now(), down: 20 * unit, period: 60 * unit, steady: 140 * unit}
	b.record(300)
	a.record(10, 10, 0)
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "http://probe.invalid/", TestTimeout: 4000},
		URLTestOption{Tolerance: 150}, a, []P.ProxyProvider{&probingProvider{proxies: []C.Proxy{node, b}, version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	endSeries(t, group.GroupBase)
	var mu sync.Mutex
	var back []time.Duration
	d := group.doubt(node)
	d.again = func() {
		mu.Lock()
		back = append(back, time.Since(node.start))
		mu.Unlock()
	}
	time.Sleep(10 * unit)
	group.recheckEvery(node, d, scale(recheckPauses), scale(recheckControlPauses), 0)
	time.Sleep(260 * unit)
	mu.Lock()
	defer mu.Unlock()
	if len(back) != 1 || back[0] < 190*unit {
		t.Fatalf("the group went back at %v (in seconds of the pattern ×%v), want once, after the node stayed up for a minute", back, unit)
	}
}

// onProbationGroup is a url-test group on B whose faster node A the series
// has found alive again and is making its control probes of, slowly: A has
// three clean records already, as after its first control probes.
func onProbationGroup(t *testing.T) (*URLTest, *scriptedNode, *scriptedNode) {
	t.Helper()
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	group := controlGroup(t, a, b)
	endSeries(t, group.GroupBase)
	group.recheckEvery(a, group.doubt(a), []time.Duration{10 * time.Millisecond}, controlsOf(time.Minute), 0)
	waitFor(t, 3*time.Second, "the node was not put on probation", func() bool { return group.onProbation() == C.Proxy(a) })
	a.record(10, 10)
	return group, a, b
}

func choose(group *URLTest) C.Proxy {
	group.fastSingle.Reset()
	return group.fast(false)
}

// Three clean records rank the node first again, but until its control
// probes are over the group does not go to it.
func TestTheGroupDoesNotGoToANodeOnProbation(t *testing.T) {
	group, _, b := onProbationGroup(t)
	if got := choose(group); got != C.Proxy(b) {
		t.Fatalf("chosen %s during the control probes, want B", got.Name())
	}
}

// With no other node alive the node on probation is used: the group is not
// left with nowhere to go.
func TestANodeOnProbationIsUsedWhenNoOtherIsAlive(t *testing.T) {
	group, a, b := onProbationGroup(t)
	b.record(0)
	b.dead.Store(true)
	if got := choose(group); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want the only node alive", got.Name())
	}
}

// A node pinned by hand is taken back as soon as it is alive, probation or
// not.
func TestAPinnedNodeOnProbationIsTakenBack(t *testing.T) {
	group, a, _ := onProbationGroup(t)
	group.ForceSet("A")
	if got := choose(group); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want the pinned node", got.Name())
	}
}

// A newer series of the group ends the probation of the old one's node.
func TestANewerSeriesEndsTheProbation(t *testing.T) {
	group, a, _ := onProbationGroup(t)
	group.rechecks.Add(1)
	if group.onProbation() != nil {
		t.Fatal("the probation outlived its series")
	}
	if got := choose(group); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want A by its speed", got.Name())
	}
}

// A node the group is on, down for 20 s and up for 40 s three
// times and up from then on, the scheduled check sees it down at 10 s:
// the group leaves it once and goes back to it once, after the node passed a
// full minute of control probes. Every choice is made afresh, as after the
// kept one runs out.
func TestAFlappingNodeIsLeftOnceAndGoneBackToOnce(t *testing.T) {
	const unit = 50 * time.Millisecond
	scale := func(real []time.Duration) []time.Duration {
		scaled := make([]time.Duration, len(real))
		for i, d := range real {
			scaled[i] = d / time.Second * unit
		}
		return scaled
	}
	a, b := scripted("A", 10, true), scripted("B", 300, true)
	node := &phasedNode{scriptedNode: a, start: time.Now(), down: 20 * unit, period: 60 * unit, steady: 140 * unit}
	b.record(300)
	a.record(10, 10, 10)
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "http://probe.invalid/", TestTimeout: 4000},
		URLTestOption{Tolerance: 150}, a, []P.ProxyProvider{&probingProvider{proxies: []C.Proxy{node, b}, version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	endSeries(t, group.GroupBase)
	var switches []string
	on := choose(group)
	if on != C.Proxy(node) {
		t.Fatalf("chosen %s at the start, want A", on.Name())
	}
	for since := time.Duration(0); since < 260*unit; since = time.Since(node.start) {
		if since >= 10*unit && !a.dead.Load() && len(switches) == 0 {
			// The scheduled check finds A down.
			a.record(0)
		}
		if got := choose(group); got != on {
			switches = append(switches, fmt.Sprintf("%s at %d", got.Name(), since/unit))
			on = got
			if got == C.Proxy(b) {
				// The series of the leave, at the pattern's pace.
				group.recheckEvery(node, group.doubt(node), scale(recheckPauses), scale(recheckControlPauses), 0)
			}
		}
		time.Sleep(unit / 5)
	}
	if len(switches) != 2 || on != C.Proxy(node) {
		t.Fatalf("switches %v, want one leave and one return", switches)
	}
	var back int
	_, _ = fmt.Sscanf(switches[1], "A at %d", &back)
	if back < 190 {
		t.Fatalf("switches %v: the group went back before the node passed its control probes", switches)
	}
}
