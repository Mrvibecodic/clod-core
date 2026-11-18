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
