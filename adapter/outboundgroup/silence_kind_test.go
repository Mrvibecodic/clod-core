package outboundgroup

import (
	"strconv"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// describedProxy is a node from a subscription: its protocol, transport and
// security are known.
type describedProxy struct {
	*askedProxy
	tp   C.AdapterType
	node adapter.Node
}

func (p *describedProxy) Type() C.AdapterType { return p.tp }
func (p *describedProxy) Node() *adapter.Node { return &p.node }

var (
	realityNode = adapter.Node{TLS: true, RealityKey: true}
	xhttpNode   = adapter.Node{Network: "xhttp", TLS: true, RealityKey: true}
	grpcNode    = adapter.Node{Network: "grpc", TLS: true, RealityKey: true}
)

func described(name, host string, node adapter.Node, answers bool) *describedProxy {
	return &describedProxy{askedProxy: askedNode(name, host, answers), tp: C.Vless, node: node}
}

func describedGroup(nodes ...*describedProxy) (*GroupBase, doubt) {
	proxies := make([]C.Proxy, len(nodes))
	for i, n := range nodes {
		proxies[i] = n
	}
	gb := NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 300, Providers: []P.ProxyProvider{&countingProvider{proxies: proxies}}})
	return gb, doubt{url: "http://probe.invalid/", all: gb.healthCheck, next: func(but C.Proxy) []C.Proxy {
		return aliveBut(gb.GetProxies(false), but, "")
	}}
}

// Four servers with REALITY and XHTTP each, REALITY the quicker. REALITY
// stops on every server: it is one kind and XHTTP another, so the node in
// doubt is dead beside an XHTTP node that answers, not the network.
func TestOneTransportDownOnEveryServerIsNoLostNetwork(t *testing.T) {
	var nodes []*describedProxy
	for i := 1; i <= 4; i++ {
		nodes = append(nodes, described("R"+strconv.Itoa(i), "192.0.2."+strconv.Itoa(i), realityNode, false))
	}
	for i := 1; i <= 4; i++ {
		nodes = append(nodes, described("X"+strconv.Itoa(i), "192.0.2."+strconv.Itoa(i), xhttpNode, true))
	}
	gb, d := describedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if !nodes[0].dead.Load() || quiet(gb) {
		t.Fatalf("the node in doubt: dead %v, taken for a lost network %v", nodes[0].dead.Load(), quiet(gb))
	}
	for _, n := range nodes[4:] {
		if n.dead.Load() {
			t.Fatalf("%s answers and is marked dead", n.name)
		}
	}
}

// The server of the node in doubt went down whole: the witness of another
// kind is taken from another server, and one look is enough, as it was
// before kinds told transports apart.
func TestAWitnessOfAnotherKindIsOfAnotherServer(t *testing.T) {
	var nodes []*describedProxy
	for _, kind := range []adapter.Node{realityNode, xhttpNode} {
		for i := 1; i <= 4; i++ {
			nodes = append(nodes, described("H"+strconv.Itoa(i)+kind.Network, "192.0.2."+strconv.Itoa(i), kind, i != 1))
		}
	}
	gb, d := describedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	if !nodes[0].dead.Load() {
		t.Fatal("the node of the server that went down is not dead")
	}
	if nodes[4].asks.Load() != 0 {
		t.Fatal("the other kind was asked on the server that went down")
	}
	asked := 0
	for _, n := range nodes[1:] {
		if a := n.asks.Load(); a > 1 {
			t.Fatalf("%s was asked %d times: a second look", n.name, a)
		} else if a == 1 {
			asked++
		}
	}
	if asked != silenceWitnesses || nodes[0].asks.Load() > 2 {
		t.Fatalf("%d asked beside the node, the node %d times", asked, nodes[0].asks.Load())
	}
}

func witnessNames(list []C.Proxy) string {
	var names []string
	for _, n := range list {
		names = append(names, n.Name())
	}
	return strings.Join(names, " ")
}

// Where every server has one kind the witnesses are what they were by
// protocol alone.
func TestWitnessesOfOneKindPerServerStayAsTheyWere(t *testing.T) {
	trojan := adapter.Node{TLS: true}
	hy2 := adapter.Node{}
	layout := func(n int, host func(i int) string, kind func(i int) (C.AdapterType, adapter.Node)) []C.Proxy {
		var list []C.Proxy
		for i := 0; i < n; i++ {
			tp, node := kind(i)
			p := described("n"+strconv.Itoa(i), host(i), node, true)
			p.tp = tp
			list = append(list, p)
		}
		return list
	}
	each := func(i int) string { return "192.0.2." + strconv.Itoa(i+1) }
	reality := func(int) (C.AdapterType, adapter.Node) { return C.Vless, realityNode }
	for _, c := range []struct {
		name        string
		nodes       []C.Proxy
		first, more string
	}{
		{"own servers", layout(10, each, reality), "n1 n2 n3", "n4 n5 n6 n7 n8 n9"},
		{"three per server", layout(12, func(i int) string { return each(i / 3) }, reality), "n1 n3 n2", "n6 n9"},
		{"one server", layout(6, func(int) string { return "192.0.2.1" }, reality), "n1 n2 n3", ""},
		{"protocols by turns", layout(9, each, func(i int) (C.AdapterType, adapter.Node) {
			return []C.AdapterType{C.Vless, C.Hysteria2, C.Trojan}[i%3], []adapter.Node{realityNode, hy2, trojan}[i%3]
		}), "n1 n2 n3", "n4 n5 n6 n7 n8"},
		{"transports by turns, own servers", layout(9, each, func(i int) (C.AdapterType, adapter.Node) {
			return C.Vless, []adapter.Node{realityNode, xhttpNode, grpcNode}[i%3]
		}), "n1 n2 n3", "n4 n5 n6 n7 n8"},
		{"two nodes", layout(2, each, reality), "n1", ""},
	} {
		first, more := witnesses(c.nodes[0], c.nodes[1:])
		if got := witnessNames(first); got != c.first {
			t.Fatalf("%s: first %q, want %q", c.name, got, c.first)
		}
		if got := witnessNames(more); got != c.more {
			t.Fatalf("%s: more %q, want %q", c.name, got, c.more)
		}
	}
}

// With nobody answering, the two looks probe no more nodes than they did
// before kinds told transports apart, however many kinds the servers have.
func TestNobodyAnsweringAmongManyKindsStaysWithinTheBound(t *testing.T) {
	var nodes []*describedProxy
	for _, kind := range []adapter.Node{realityNode, xhttpNode, grpcNode} {
		for i := 1; i <= 6; i++ {
			nodes = append(nodes, described(kind.Network+strconv.Itoa(i), "192.0.2."+strconv.Itoa(i), kind, false))
		}
	}
	gb, d := describedGroup(nodes...)
	gb.askOf(nodes[0], d, false)
	asked, tries := 0, int32(0)
	for _, n := range nodes[1:] {
		if n.asks.Load() > 0 {
			asked++
		}
	}
	for _, n := range nodes {
		tries += n.asks.Load()
	}
	// A probe is two tries at most: the first and the second.
	if bound := 2 + 2*silenceWitnesses + silenceMoreWitnesses; asked > silenceWitnesses+silenceMoreWitnesses || tries > int32(2*bound) {
		t.Fatalf("%d nodes asked beside the node, %d tries", asked, tries)
	}
	for _, n := range nodes {
		if n.dead.Load() || n.records() != 0 {
			t.Fatalf("%s: dead %v, records %d with nobody answering", n.name, n.dead.Load(), n.records())
		}
	}
	if !quiet(gb) {
		t.Fatal("no pause was left")
	}
}
