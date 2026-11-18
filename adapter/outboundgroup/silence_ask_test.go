package outboundgroup

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// askedProxy is a node that answers its probes or leaves them hanging and
// keeps what was written to it.
type askedProxy struct {
	C.Proxy
	name, addr string
	answers    atomic.Bool
	// second: the node answers from its second probe on.
	second atomic.Bool
	// during runs while the node is probed.
	during func()
	// trojan: the node is of another kind than the rest.
	trojan bool
	dead   atomic.Bool
	asks   atomic.Int32

	mu      sync.Mutex
	written []C.DelayHistory
}

func (p *askedProxy) Name() string                { return p.name }
func (p *askedProxy) Addr() string                { return p.addr }
func (p *askedProxy) Adapter() C.ProxyAdapter     { return outbound.NewDirect() }
func (p *askedProxy) AliveForTestUrl(string) bool { return !p.dead.Load() }
func (p *askedProxy) ProxyInfo() C.ProxyInfo      { return C.ProxyInfo{} }
func (p *askedProxy) Type() C.AdapterType {
	if p.trojan {
		return C.Trojan
	}
	return C.Vless
}

func (p *askedProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	asks := p.asks.Add(1)
	if p.during != nil {
		p.during()
	}
	result := C.HeldProbe(ctx)
	if p.answers.Load() && (asks > 1 || !p.second.Load()) {
		*result = C.ProbeResult{Held: true, Time: time.Now(), Delay: 10, Satisfied: true}
		return 10, nil
	}
	<-ctx.Done()
	*result = C.ProbeResult{Held: true, Time: time.Now(), Err: ctx.Err(), Stage: C.ProbeStageRequest}
	return 0, ctx.Err()
}

func (p *askedProxy) RecordProbe(_ string, result *C.ProbeResult) {
	p.mu.Lock()
	p.written = append(p.written, C.DelayHistory{Time: result.Time, Delay: result.Delay})
	p.mu.Unlock()
	p.dead.Store(!result.OK())
}

func (p *askedProxy) DelayHistoryForTestUrl(string) []C.DelayHistory {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]C.DelayHistory(nil), p.written...)
}

func (p *askedProxy) RecordSoftFailure(_ string, at time.Time) {
	p.mu.Lock()
	p.written = append(p.written, C.DelayHistory{Time: at})
	p.mu.Unlock()
}
func (p *askedProxy) LastProbeElapsed(string) time.Duration { return 0 }

func (p *askedProxy) records() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.written)
}

func askedNode(name, host string, answers bool) *askedProxy {
	n := &askedProxy{name: name, addr: host + ":443"}
	n.answers.Store(answers)
	return n
}

func askedGroup(nodes ...*askedProxy) (*GroupBase, *countingProvider, doubt) {
	proxies := make([]C.Proxy, len(nodes))
	for i, n := range nodes {
		proxies[i] = n
	}
	provider := &countingProvider{proxies: proxies}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 300, Providers: []P.ProxyProvider{provider}})
	return gb, provider, doubt{url: "http://probe.invalid/", all: gb.healthCheck, next: func(but C.Proxy) []C.Proxy {
		return aliveBut(gb.GetProxies(false), but, "")
	}}
}

func quiet(gb *GroupBase) bool {
	gb.silence.mu.Lock()
	defer gb.silence.mu.Unlock()
	return time.Now().Before(gb.silence.quietUntil)
}

// A silent node beside one that answers at its first try is dead: its answer
// is written down, the witness's is not, and the rest of the group is not
// checked.
func TestASilentNodeBesideAnAnsweringOneIsMarkedDead(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	gb, provider, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if !a.dead.Load() || a.records() != 1 {
		t.Fatalf("the silent node: dead %v, records %d", a.dead.Load(), a.records())
	}
	if b.dead.Load() || b.records() != 0 {
		t.Fatalf("the answering node: dead %v, records %d", b.dead.Load(), b.records())
	}
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked though a witness answered")
	}
	if quiet(gb) {
		t.Fatal("a node found dead must leave no pause")
	}
}

// A node that answers its probe was no cause of the silence: nothing is
// written to it or to the witnesses, and the group is not asked again for the
// pause.
func TestANodeThatAnswersLeavesNothingWritten(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, provider, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if a.records() != 0 || b.records() != 0 {
		t.Fatalf("written to the node %d, to the witness %d, want nothing", a.records(), b.records())
	}
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked")
	}
	if !quiet(gb) {
		t.Fatal("no pause after a node found alive")
	}
}

// With nobody answering it is the network of the device: nothing is written,
// the rest of the group is left alone, and the group is not asked again for
// the pause.
func TestNobodyAnsweringLeavesNothingWritten(t *testing.T) {
	a, b, c := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", false), askedNode("C", "192.0.2.3", false)
	gb, provider, d := askedGroup(a, b, c)
	gb.askOf(a, d, false)
	if a.records() != 0 || b.records() != 0 || c.records() != 0 {
		t.Fatalf("written %d, %d, %d, want nothing", a.records(), b.records(), c.records())
	}
	if a.dead.Load() || b.dead.Load() || c.dead.Load() {
		t.Fatal("a node was marked dead with nobody answering")
	}
	if got := provider.checks.Load(); got != 0 {
		t.Fatalf("the whole group was checked %d times, want none", got)
	}
	if !quiet(gb) {
		t.Fatal("no pause after nobody answered")
	}
}

// A node with no other node alive has nobody to be told from the network by:
// the whole group is checked, as it always was.
func TestANodeWithNobodyBesideHasTheWholeGroupChecked(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	b.dead.Store(true)
	gb, provider, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if got := provider.checks.Load(); got != 1 {
		t.Fatalf("the whole group was checked %d times, want once", got)
	}
	if a.asks.Load() != 0 || b.asks.Load() != 0 {
		t.Fatal("the nodes were asked apart from the check of the group")
	}
}

// No more than silenceWitnesses nodes are asked beside the one in doubt: the
// first the group would go to.
func TestWitnessesAreTheFirstFewTheGroupWouldGoTo(t *testing.T) {
	a := askedNode("A", "192.0.2.1", false)
	rest := []*askedProxy{askedNode("B", "192.0.2.2", true), askedNode("C", "192.0.2.3", true), askedNode("D", "192.0.2.4", true), askedNode("E", "192.0.2.5", true)}
	gb, _, d := askedGroup(append([]*askedProxy{a}, rest...)...)
	gb.askOf(a, d, false)
	for i, n := range rest {
		if asked := n.asks.Load() > 0; asked != (i < silenceWitnesses) {
			t.Fatalf("node %s: asked %v", n.name, asked)
		}
	}
}

// Nodes on the server of the node in doubt go down with it and say little of
// the network: when the first few are all there, the last of them gives its
// place to the best node of another server. Two of the first silent is no
// proof yet: the second look adds a node of a server not asked (G).
func TestOneWitnessIsOfAnotherServer(t *testing.T) {
	nodes := []*askedProxy{askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.1", false), askedNode("C", "192.0.2.1", false), askedNode("D", "192.0.2.1", false), askedNode("E", "192.0.2.1", false), askedNode("F", "192.0.2.2", true), askedNode("G", "192.0.2.3", true)}
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	asked := ""
	for _, n := range nodes[1:] {
		if n.asks.Load() > 0 {
			asked += n.name
		}
	}
	if asked != "BCFG" {
		t.Fatalf("asked beside the node: %q, want BCFG", asked)
	}
	if !nodes[0].dead.Load() || !nodes[1].dead.Load() || !nodes[2].dead.Load() || nodes[5].dead.Load() {
		t.Fatal("the server that is down was not told from the one that answers")
	}
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked")
	}
}

// A group whose nodes are all on one server asks the first few as they are.
func TestWitnessesOfOneServer(t *testing.T) {
	nodes := []*askedProxy{askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.1", true), askedNode("C", "192.0.2.1", true), askedNode("D", "192.0.2.1", true), askedNode("E", "192.0.2.1", true)}
	gb, _, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if !nodes[0].dead.Load() || nodes[4].asks.Load() != 0 {
		t.Fatalf("dead %v, the fifth asked %d times", nodes[0].dead.Load(), nodes[4].asks.Load())
	}
}

// The connections opened through a node before it was marked dead go silent
// after it: they have nobody asked again.
func TestNoAskingAboutANodeAlreadyDead(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	a.dead.Store(true)
	gb.askOf(a, d, false)
	if a.asks.Load() != 0 || b.asks.Load() != 0 || a.records() != 0 {
		t.Fatalf("asked the dead node %d times, the witness %d, written %d", a.asks.Load(), b.asks.Load(), a.records())
	}
}

// A check of the group that is already running asks about the node itself.
func TestNoAskingWhileTheGroupIsChecked(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	gb.failedTesting.Store(true)
	gb.askOf(a, d, false)
	if a.asks.Load() != 0 || a.dead.Load() {
		t.Fatal("the node was asked about beside a running check")
	}
}

func failures(gb *GroupBase) int {
	gb.failedTestMux.Lock()
	defer gb.failedTestMux.Unlock()
	return gb.failedTimes
}

// Failed connections through a node ask about it the same way: with nobody
// answering nothing is written and the group is not checked, and the
// failures of those seconds are answered.
func TestFailedConnectionsWithNobodyAnsweringMarkNothing(t *testing.T) {
	a, b, c := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", false), askedNode("C", "192.0.2.3", false)
	gb, provider, d := askedGroup(a, b, c)
	gb.failedTimes = 3
	gb.failedOf(a, time.Now(), d)
	if a.dead.Load() || b.dead.Load() || c.dead.Load() || a.records()+b.records()+c.records() != 0 {
		t.Fatal("failed connections with the network down marked a node")
	}
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked")
	}
	if failures(gb) != 0 {
		t.Fatal("the failures counted before the answer were kept")
	}
	asks := a.asks.Load()
	gb.failedOf(a, time.Now(), d)
	if a.asks.Load() != asks {
		t.Fatal("asked again within the pause")
	}
}

// A node whose connections fail beside one that answers is dead.
func TestFailedConnectionsBesideAnAnsweringNodeMarkTheNodeDead(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	gb, provider, d := askedGroup(a, b)
	gb.failedOf(a, time.Now(), d)
	if !a.dead.Load() || b.dead.Load() || provider.checks.Load() != 0 {
		t.Fatalf("dead: the node %v, the other %v; checks of the group %d", a.dead.Load(), b.dead.Load(), provider.checks.Load())
	}
}

// A node that answers while its connections fail is asked about again soon,
// and later each time in a row: its wait starts short and stops at the pause
// of a silent site, whatever that pause has grown to.
func TestANodeThatAnswersWhileItsConnectionsFailIsAskedAgainSoon(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	wait := func() time.Duration {
		gb.silence.mu.Lock()
		defer gb.silence.mu.Unlock()
		return time.Until(gb.silence.failedQuietUntil).Round(time.Second)
	}
	for _, want := range []time.Duration{silenceAfter, 2 * silenceAfter, 4 * silenceAfter, 8 * silenceAfter, silencePause, silencePause} {
		gb.failedOf(a, time.Now(), d)
		if got := wait(); got != want {
			t.Fatalf("asked again in %s, want %s", got, want)
		}
		gb.silence.mu.Lock()
		gb.silence.failedQuietUntil = time.Time{}
		gb.silence.mu.Unlock()
	}
	if a.records() != 0 {
		t.Fatal("the answer was written down")
	}
	gb.noteAnswer(a, time.Now())
	gb.failedOf(a, time.Now(), d)
	if got := wait(); got != silenceAfter {
		t.Fatalf("after an answered connection asked again in %s, want %s", got, silenceAfter)
	}
}

// A group that is on a dead node with no other node alive has every node
// checked.
func TestFailedConnectionsThroughADeadNodeCheckTheWholeGroup(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	a.dead.Store(true)
	b.dead.Store(true)
	gb, provider, d := askedGroup(a, b)
	gb.failedOf(a, time.Now(), d)
	if provider.checks.Load() != 1 || a.asks.Load() != 0 {
		t.Fatalf("checks of the group %d, the node asked %d times", provider.checks.Load(), a.asks.Load())
	}
}

// With every node at its second try it is the network that stumbled: nothing
// is written.
func TestSecondTriesAllRoundLeaveNothingWritten(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	a.second.Store(true)
	b.second.Store(true)
	gb, _, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if a.records() != 0 || b.records() != 0 {
		t.Fatalf("written %d and %d, want nothing", a.records(), b.records())
	}
}

// A site that keeps silent has the group wait before it asks about the node
// again; a node that then refuses connections is asked about all the same.
func TestThePauseOfASilentSiteDoesNotHoldFailedConnections(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if !quiet(gb) {
		t.Fatal("no pause after a node found alive")
	}
	a.answers.Store(false)
	gb.failedOf(a, time.Now(), d)
	if !a.dead.Load() {
		t.Fatal("failed connections were not answered within the pause of the silent site")
	}
}

// A probe written to the node by someone else while it was asked about is
// newer than the answer: it stands, and the answer is dropped.
func TestANewerProbeOfTheNodeStandsOverTheAnswer(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	var once sync.Once
	a.during = func() {
		once.Do(func() {
			a.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Delay: 10, Satisfied: true})
		})
	}
	gb, _, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if a.dead.Load() || a.records() != 1 {
		t.Fatalf("dead %v, records %d: the answer was written over a newer probe", a.dead.Load(), a.records())
	}
}

// A newer probe that leaves the node alive means the answer found nothing
// out: the node is not taken for dead, and the nodes beside it are left as
// they were.
func TestAnAnswerOverANewerProbeIsNoVerdict(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	var once sync.Once
	a.during = func() {
		once.Do(func() {
			a.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Delay: 10, Satisfied: true})
		})
	}
	gb, _, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	if b.records() != 0 {
		t.Fatalf("written to the node beside: %d", b.records())
	}
	if gb.rechecks.Load() != 0 {
		t.Fatal("the node was taken for one the group left")
	}
}

// A check of the whole group that did not start, another one was running,
// found nothing out: it leaves no pause.
func TestACheckOfTheGroupThatDidNotStartLeavesNoPause(t *testing.T) {
	a := askedNode("A", "192.0.2.1", false)
	gb, provider, d := askedGroup(a)
	gb.askOf(a, d, false)
	if provider.checks.Load() != 1 {
		t.Fatalf("checks of the group: %d", provider.checks.Load())
	}
	gb.silence.mu.Lock()
	wait := time.Until(gb.silence.quietUntil)
	gb.silence.mu.Unlock()
	if wait > silenceAfter {
		t.Fatalf("a check that did not reach the node left a pause of %s", wait)
	}
}

// A node that refuses everything has no answer to give, and a group inside
// the group answers for whatever node it is on: neither is asked beside the
// node in doubt.
func TestANodeThatRefusesEverythingIsNoWitness(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	reject := &typedProxy{askedProxy: askedNode("REJECT", "", false), kind: C.Reject}
	inner := &typedProxy{askedProxy: askedNode("inner", "", true), kind: C.URLTest}
	if got, more := witnesses(a, []C.Proxy{reject, inner, b}); len(got) != 1 || got[0] != C.Proxy(b) || len(more) != 0 {
		t.Fatalf("witnesses: %d and %d", len(got), len(more))
	}
}

type typedProxy struct {
	*askedProxy
	kind C.AdapterType
}

func (p *typedProxy) Type() C.AdapterType { return p.kind }

func asked(nodes []*askedProxy) string {
	names := ""
	for _, n := range nodes {
		if n.asks.Load() > 0 {
			names += n.name
		}
	}
	return names
}

// Nodes of one kind stop together: when the first the group would go to are
// all of the kind of the node in doubt, one of them gives its place to the
// best node of another kind. Two of the first silent is no proof yet: the
// second look adds the servers not asked (D, E).
func TestOneWitnessIsOfAnotherKind(t *testing.T) {
	nodes := []*askedProxy{askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", false), askedNode("C", "192.0.2.3", false), askedNode("D", "192.0.2.4", false), askedNode("E", "192.0.2.5", false), askedNode("F", "192.0.2.6", true)}
	nodes[5].trojan = true
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if got := asked(nodes[1:]); got != "BCDEF" {
		t.Fatalf("asked beside the node: %q, want BCDEF", got)
	}
	if !nodes[0].dead.Load() || nodes[5].dead.Load() || provider.checks.Load() != 0 {
		t.Fatal("the kind that stopped was not told from the one that works")
	}
}

// With none of the first few answering the group does not take it for the
// network yet: it asks a few more, each of a kind or of a server not asked
// before, and one of them answering shows the node dead.
func TestSilentWitnessesHaveMoreNodesAsked(t *testing.T) {
	// Three servers of three nodes each, and a node of another kind on the
	// first of them: the node in doubt and the first asked are silent.
	nodes := []*askedProxy{
		askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.1", false), askedNode("C", "192.0.2.1", false),
		askedNode("D", "192.0.2.2", false), askedNode("E", "192.0.2.2", true), askedNode("F", "192.0.2.2", true),
		askedNode("G", "192.0.2.3", true), askedNode("H", "192.0.2.3", true), askedNode("I", "192.0.2.3", true),
		askedNode("J", "192.0.2.1", false),
	}
	nodes[9].trojan = true
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	// First: the best (B), one of another kind (J), one of another server
	// (D). Then: the third server (G) has not been asked.
	if got := asked(nodes[1:]); got != "BDGJ" {
		t.Fatalf("asked beside the node: %q, want BDGJ", got)
	}
	if !nodes[0].dead.Load() || !nodes[1].dead.Load() || !nodes[3].dead.Load() || !nodes[9].dead.Load() || nodes[6].dead.Load() {
		t.Fatal("the silent nodes were not told from the one that answers")
	}
	if nodes[6].records() != 0 || provider.checks.Load() != 0 {
		t.Fatalf("the answer was written %d times, the whole group checked %d times", nodes[6].records(), provider.checks.Load())
	}
	if gb.rechecks.Load() != 1 {
		t.Fatal("the node the group left is not probed again")
	}
}

// With nobody answering among the first and the more it is the network:
// nothing is written, and no more than silenceMoreWitnesses were asked on top
// of the first.
func TestNobodyAnsweringAmongTheMoreLeavesNothingWritten(t *testing.T) {
	var nodes []*askedProxy
	for i := 0; i < 14; i++ {
		nodes = append(nodes, askedNode(string(rune('A'+i)), "192.0.2."+strconv.Itoa(i+1), false))
	}
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if got := len(asked(nodes[1:])); got != silenceWitnesses+silenceMoreWitnesses {
		t.Fatalf("asked %d nodes beside the node, want %d", got, silenceWitnesses+silenceMoreWitnesses)
	}
	for _, n := range nodes {
		if n.dead.Load() || n.records() != 0 {
			t.Fatalf("%s: dead %v, records %d with nobody answering", n.name, n.dead.Load(), n.records())
		}
	}
	if provider.checks.Load() != 0 || !quiet(gb) {
		t.Fatal("the whole group was checked, or no pause was left")
	}
}

// Failed connections that find another check running start none: the wait
// before they ask again does not grow for it.
func TestFailedConnectionsThatStartedNoCheckMakeNoWaitLonger(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	gb.failedTesting.Store(true)
	for i := 0; i < 3; i++ {
		gb.failedOf(a, time.Now(), d)
		gb.silence.mu.Lock()
		wait, grown := time.Until(gb.silence.failedQuietUntil), gb.silence.failedPause
		gb.silence.failedQuietUntil = time.Time{}
		gb.silence.mu.Unlock()
		if wait > silenceAfter || grown != 0 {
			t.Fatalf("a check that did not start left a wait of %s, grown to %s", wait, grown)
		}
	}
	if a.asks.Load() != 0 {
		t.Fatal("the node was asked beside a running check")
	}
}

// A group inside the group watches its own nodes: the outer group neither
// watches the connection once more nor asks about the inner one, and failed
// connections through it have the whole group checked, as they always had.
func TestAGroupInsideTheGroupIsNotAskedAbout(t *testing.T) {
	inner := &typedProxy{askedProxy: askedNode("inner", "", true), kind: C.URLTest}
	b := askedNode("B", "192.0.2.2", true)
	provider := &countingProvider{proxies: []C.Proxy{inner, b}}
	gb := NewGroupBase(GroupBaseOption{Name: "outer", TestTimeout: 300, Providers: []P.ProxyProvider{provider}})
	d := doubt{url: "http://probe.invalid/", all: gb.healthCheck, next: func(but C.Proxy) []C.Proxy {
		return aliveBut(gb.GetProxies(false), but, "")
	}}
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	conn := outbound.NewConn(near, outbound.NewDirect())
	if got := gb.watch(conn, inner, time.Now(), d, nil); got != C.Conn(conn) {
		t.Fatal("a connection through a group inside the group is watched once more")
	}
	gb.failedOf(inner, time.Now(), d)
	if provider.checks.Load() != 1 || inner.asks.Load() != 0 || b.asks.Load() != 0 {
		t.Fatalf("checks of the group %d, the inner group asked %d times, the node beside %d", provider.checks.Load(), inner.asks.Load(), b.asks.Load())
	}
}

// A network that comes back while the more are asked has everybody asked
// anew: the silence of the node and of the first few is seconds old by then,
// and it is not held against them beside the answers of the more.
func TestANetworkBackWhileTheMoreAreAskedMarksNothingDead(t *testing.T) {
	var nodes []*askedProxy
	for i := 0; i < 8; i++ {
		nodes = append(nodes, askedNode(string(rune('A'+i)), "192.0.2."+strconv.Itoa(i+1), false))
	}
	// The more are asked only after the first few kept silent: the network
	// is back by the time one of them is, a moment after the node was asked
	// again, so that the node needs its second try and the other does not.
	nodes[4].during = func() {
		for nodes[0].asks.Load() < 3 {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		for _, n := range nodes {
			n.answers.Store(true)
		}
	}
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if nodes[4].asks.Load() == 0 {
		t.Fatal("the more were not asked")
	}
	for _, n := range nodes {
		if n.dead.Load() {
			t.Fatalf("%s is marked dead for the silence of a network that is back", n.name)
		}
	}
	if gb.rechecks.Load() != 0 || provider.checks.Load() != 0 {
		t.Fatal("the group left its node, or was checked whole")
	}
	if nodes[0].records() != 0 {
		t.Fatal("the second try the node needed while the network came back was written to it")
	}
}

// The failed connections of a node the group went to without one that
// opened are counted to that node: the wait its check leaves holds the next
// ones, and they do not have it probed again and again.
func TestFailedConnectionsOfANodeTheGroupWentToLeaveTheirWait(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	gb.noteAnswer(a, time.Now())
	time.Sleep(time.Millisecond)
	gb.failedOf(b, time.Now(), d)
	asks := b.asks.Load()
	if asks == 0 {
		t.Fatal("the node was not asked about")
	}
	gb.failedOf(b, time.Now(), d)
	if b.asks.Load() != asks {
		t.Fatal("asked again at once: the wait was not left to the node the group is on")
	}
}

// A connection through a node the group has left that fails late asks
// nothing, and what is known of the node the group is on stays.
func TestALateFailureOfANodeTheGroupLeftAsksNothing(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	gb, _, d := askedGroup(a, b)
	early := time.Now()
	time.Sleep(time.Millisecond)
	gb.noteAnswer(b, time.Now())
	gb.failedOf(a, early, d)
	if a.asks.Load() != 0 || b.asks.Load() != 0 {
		t.Fatal("a node the group has left was asked about")
	}
	if gb.silence.node != C.Proxy(b) {
		t.Fatal("the group no longer counts the node it is on")
	}
}
