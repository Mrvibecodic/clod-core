package outboundgroup

import (
	"sync"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// keptProxy is an asked node that keeps shared connections: it notes how many
// probes it had seen each time they were set aside.
type keptProxy struct {
	*askedProxy
	refuses bool

	mu      sync.Mutex
	retired []int32
}

type keptAdapter struct {
	C.ProxyAdapter
	node *keptProxy
}

func (a keptAdapter) KeepsConns() bool { return true }

func (a keptAdapter) RetireConns() bool {
	a.node.mu.Lock()
	a.node.retired = append(a.node.retired, a.node.asks.Load())
	a.node.mu.Unlock()
	return !a.node.refuses
}

func (p *keptProxy) Adapter() C.ProxyAdapter {
	return keptAdapter{ProxyAdapter: outbound.NewDirect(), node: p}
}

func (p *keptProxy) retiredAt() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int32(nil), p.retired...)
}

func lookGroup(p C.Proxy, nodes ...*askedProxy) (*GroupBase, *countingProvider, doubt) {
	proxies := []C.Proxy{p}
	for _, n := range nodes {
		proxies = append(proxies, n)
	}
	gb, provider, d := askedGroup(nodes...)
	provider.proxies = proxies
	return gb, provider, d
}

func sameAsks(got []int32, want ...int32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// A node in doubt that keeps shared connections is probed over a new one from
// its first try, as the witnesses are: the connection it keeps is the one in
// doubt.
func TestTheNodeInDoubtIsProbedOverFreshConnections(t *testing.T) {
	p := &keptProxy{askedProxy: askedNode("A", "192.0.2.1", true)}
	b := askedNode("B", "192.0.2.2", true)
	gb, _, d := lookGroup(p, b)
	gb.askOf(p, d, false)
	if got := p.retiredAt(); len(got) == 0 || got[0] != 0 {
		t.Fatalf("set aside after %v probes, want before the first", got)
	}
}

// A transport that refused to set its connections aside before the first
// batch is asked again before the second; one that did is not.
func TestFreshConnectionsAreAskedForAgainOnlyAfterARefusal(t *testing.T) {
	for _, refuses := range []bool{false, true} {
		p := &keptProxy{askedProxy: askedNode("A", "192.0.2.1", false), refuses: refuses}
		rest := []*askedProxy{askedNode("B", "192.0.2.2", false), askedNode("C", "192.0.2.3", false), askedNode("D", "192.0.2.4", false), askedNode("E", "192.0.2.5", false)}
		gb, _, d := lookGroup(p, rest...)
		gb.askOf(p, d, false)
		// Each batch probes the node twice; its second try sets them aside
		// on its own.
		want := []int32{0, 1, 3}
		if refuses {
			want = []int32{0, 1, 2, 3}
		}
		if got := p.retiredAt(); !sameAsks(got, want...) {
			t.Fatalf("refuses %v: set aside after %v probes, want %v", refuses, got, want)
		}
	}
}

// answersFrom has the node answer from its given probe on.
func answersFrom(n *askedProxy, ask int32) {
	n.answers.Store(false)
	n.during = func() {
		if n.asks.Load() >= ask {
			n.answers.Store(true)
		}
	}
}

func noneDead(t *testing.T, nodes ...*askedProxy) {
	t.Helper()
	for _, n := range nodes {
		if n.dead.Load() || n.records() != 0 {
			t.Fatalf("%s: dead %v, records %d, want untouched", n.name, n.dead.Load(), n.records())
		}
	}
}

// A network that stalls and comes back within the first look lets a witness
// answer at its second try while the node in doubt does not: that is no
// proof, and the node, asked again, answers.
func TestANetworkBackWithinTheFirstLookMarksNothing(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true)
	answersFrom(a, 3)
	b.second.Store(true)
	gb, provider, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	noneDead(t, a, b)
	if a.asks.Load() < 3 {
		t.Fatal("the node was not asked again")
	}
	if gb.rechecks.Load() != 0 || provider.checks.Load() != 0 {
		t.Fatal("the group left its node, or was checked whole")
	}
}

// A silent node beside witnesses that all answer at their first try is dead
// after one look, and what the witnesses answered is not written.
func TestASilentNodeBesideSteadyWitnessesIsDeadAtOnce(t *testing.T) {
	a, b, c := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true), askedNode("C", "192.0.2.3", true)
	gb, _, d := askedGroup(a, b, c)
	gb.askOf(a, d, false)
	if !a.dead.Load() || a.asks.Load() != 2 {
		t.Fatalf("the node: dead %v, asked %d times, want dead after one look", a.dead.Load(), a.asks.Load())
	}
	noneDead(t, b, c)
}

// A witness that answers only at its second try leaves the evidence mixed:
// the node is asked again before it is marked, and the slow witness is not
// marked at all. With no more nodes the second look asks the same ones.
func TestASlowWitnessHasTheNodeLookedAtTwice(t *testing.T) {
	a, b, c := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", true), askedNode("C", "192.0.2.3", true)
	b.second.Store(true)
	gb, _, d := askedGroup(a, b, c)
	gb.askOf(a, d, false)
	if !a.dead.Load() || a.asks.Load() != 4 {
		t.Fatalf("the node: dead %v, asked %d times, want dead after two looks", a.dead.Load(), a.asks.Load())
	}
	if c.asks.Load() != 2 {
		t.Fatalf("the steady witness was asked %d times, want once a look", c.asks.Load())
	}
	noneDead(t, b, c)
}

// Witnesses silent at both looks are marked dead beside the node; the nodes
// asked only at the second look, and those that answered, are left as they
// were.
func TestWitnessesSilentAtBothLooksAreMarkedDead(t *testing.T) {
	var nodes []*askedProxy
	for i := 0; i < 7; i++ {
		nodes = append(nodes, askedNode(string(rune('A'+i)), "192.0.2."+string(rune('1'+i)), i > 2))
	}
	gb, provider, d := askedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	for _, n := range nodes[:3] {
		if !n.dead.Load() {
			t.Fatalf("%s is not marked dead", n.name)
		}
	}
	if nodes[0].asks.Load() != 4 {
		t.Fatalf("the node was asked %d times, want two looks", nodes[0].asks.Load())
	}
	if nodes[4].asks.Load() == 0 {
		t.Fatal("the more were not asked")
	}
	noneDead(t, nodes[3:]...)
	if provider.checks.Load() != 0 {
		t.Fatal("the whole group was checked")
	}
}

// A witness silent at the first look only is not marked.
func TestAWitnessSilentOnceIsNotMarked(t *testing.T) {
	a, b, c := askedNode("A", "192.0.2.1", false), askedNode("B", "192.0.2.2", false), askedNode("C", "192.0.2.3", true)
	answersFrom(b, 3)
	gb, _, d := askedGroup(a, b, c)
	gb.askOf(a, d, false)
	if !a.dead.Load() {
		t.Fatal("the node is not marked dead")
	}
	noneDead(t, b, c)
}

// A witness that keeps shared connections and answers at its second try, over
// a new connection, has no failure of its hung first try written: it is slow
// all the same, and the node in doubt is looked at twice.
func TestAWitnessThatAnsweredOverANewConnectionIsNotSteady(t *testing.T) {
	a := askedNode("A", "192.0.2.1", false)
	b := &keptProxy{askedProxy: askedNode("B", "192.0.2.2", true)}
	b.second.Store(true)
	c := askedNode("C", "192.0.2.3", true)
	gb, _, d := askedGroup(a, c)
	provider := gb.providers[0].(*countingProvider)
	provider.proxies = []C.Proxy{a, b, c}
	// Long enough for the first try to be still hanging when the second,
	// started at half of it, answers.
	gb.testTimeout = 2400
	gb.askOf(a, d, false)
	if !a.dead.Load() || a.asks.Load() != 4 {
		t.Fatalf("the node: dead %v, asked %d times, want dead after two looks", a.dead.Load(), a.asks.Load())
	}
	if b.dead.Load() || b.records() != 0 {
		t.Fatalf("the slow witness: dead %v, records %d", b.dead.Load(), b.records())
	}
}

// A node that needed its second try where a node beside it answered at the
// first is alive, and nothing is written to it: a check made when the node is
// at its worst costs it no rank.
func TestANodeThatAnswersAtItsSecondTryIsWrittenNothing(t *testing.T) {
	a, b := askedNode("A", "192.0.2.1", true), askedNode("B", "192.0.2.2", true)
	a.second.Store(true)
	gb, _, d := askedGroup(a, b)
	gb.askOf(a, d, false)
	noneDead(t, a, b)
	if !quiet(gb) {
		t.Fatal("no pause after a node found alive")
	}
}
