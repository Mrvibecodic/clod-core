package provider

import (
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

// pooledNode is a node that keeps a connection for all its streams.
type pooledNode struct {
	C.ProxyAdapter
	keeps   bool
	retired atomic.Int32
}

func (n *pooledNode) RetireConns() bool {
	n.retired.Add(1)
	return n.keeps
}

func probeThrough(t *testing.T, node *pooledNode, target *probeTarget) (probeOutcome, *adapter.Proxy) {
	t.Helper()
	proxy := adapter.NewProxy(node)
	expected, err := utils.NewUnsignedRanges[uint16]("204")
	if err != nil {
		t.Fatal(err)
	}
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 3000, 0, false, expected)
	t.Cleanup(hc.close)
	outcome, _ := hc.probe(proxy, target.url(), expected)
	return outcome, proxy
}

// The second probe of a node that keeps one connection for everything goes
// over a new one, and the first, left hanging on the connection set aside, is
// no mark against the node: it was the connection that was stuck.
func TestTheSecondProbeGoesOverANewConnection(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	outcome, proxy := probeThrough(t, node, target)
	if outcome != probeRecovered || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if got := node.retired.Load(); got != 1 {
		t.Fatalf("the kept connections were set aside %d times, want once, before the second probe", got)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 || history[0] == 0 {
		t.Fatalf("the stuck connection must leave no failure in the history: %v", history)
	}
}

// A node that kept no connection opened one for each probe: its hung first
// probe stays in the history as before.
func TestAHungProbeOfANodeWithoutKeptConnectionsStaysInTheHistory(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect()}
	outcome, proxy := probeThrough(t, node, target)
	if outcome != probeRecovered {
		t.Fatalf("outcome %v", outcome)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 2 || history[0] != 0 || history[1] == 0 {
		t.Fatalf("the stall must stay in the history before the answer: %v", history)
	}
}

// A first probe that failed by itself is the node's failure whatever
// connection the second one went over: it stays in the history.
func TestAFailedFirstProbeStaysInTheHistoryOfANodeWithKeptConnections(t *testing.T) {
	target := newProbeTarget(t, 0, "bad", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	outcome, proxy := probeThrough(t, node, target)
	if outcome != probeRecovered {
		t.Fatalf("outcome %v", outcome)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 2 || history[0] != 0 || history[1] == 0 {
		t.Fatalf("the failure must stay in the history before the answer: %v", history)
	}
}

// A node that answers its first probe keeps its connections.
func TestASteadyNodeKeepsItsConnections(t *testing.T) {
	target := newProbeTarget(t, 0, "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	if outcome, _ := probeThrough(t, node, target); outcome != probePassed {
		t.Fatalf("outcome %v", outcome)
	}
	if got := node.retired.Load(); got != 0 {
		t.Fatalf("the kept connections were set aside %d times", got)
	}
}
