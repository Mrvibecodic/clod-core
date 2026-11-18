package provider

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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

func (n *pooledNode) KeepsConns() bool {
	return n.keeps
}

// secondProbeAfter is how long after the first probe the second one reached
// the target.
func secondProbeAfter(t *testing.T, target *probeTarget) time.Duration {
	t.Helper()
	target.mu.Lock()
	defer target.mu.Unlock()
	if len(target.at) < 2 {
		t.Fatalf("the target saw %d probes, want two", len(target.at))
	}
	return target.at[1].Sub(target.at[0])
}

func probeThrough(t *testing.T, node *pooledNode, target *probeTarget) (probeOutcome, *adapter.Proxy) {
	t.Helper()
	return probeThroughWithin(t, node, target, 3000)
}

// probeThroughWithin is probeThrough with a probe timeout of its own, in
// milliseconds.
func probeThroughWithin(t *testing.T, node *pooledNode, target *probeTarget, timeout uint) (probeOutcome, *adapter.Proxy) {
	t.Helper()
	proxy := adapter.NewProxy(node)
	expected, err := utils.NewUnsignedRanges[uint16]("204")
	if err != nil {
		t.Fatal(err)
	}
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), timeout, 0, false, expected)
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

// A first probe that failed while the node keeps shared connections says the
// connection it went over was broken: the second one, over a new connection,
// answered, and the node gets no failure in its history for the old one.
func TestAFailedFirstProbeOverAConnectionSetAsideIsNoMark(t *testing.T) {
	target := newProbeTarget(t, 0, "bad", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	outcome, proxy := probeThrough(t, node, target)
	if outcome != probeRecovered {
		t.Fatalf("outcome %v", outcome)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 || history[0] == 0 {
		t.Fatalf("the failure over the old connection must leave no mark: %v", history)
	}
}

// A node that keeps nothing went over a new connection both times: its failed
// first probe stays in the history before the answer.
func TestAFailedFirstProbeOfANodeWithoutKeptConnectionsStaysInTheHistory(t *testing.T) {
	target := newProbeTarget(t, 0, "bad", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect()}
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

// A node with shared connections that is only slow (the probe timeout is 4 s,
// so its second probe would come at 2 s) answers its first probe at 1.1 s,
// well before the second one starts: its connections are not set aside. With
// the second probe at the hedge delay it would come before the answer.
func TestASlowAnswerOfANodeWithSharedConnectionsGetsNoSecondProbe(t *testing.T) {
	target := newProbeTarget(t, 1100*time.Millisecond, "slow")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	outcome, proxy := probeThroughWithin(t, node, target, 4000)
	if outcome != probePassed || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if got := node.retired.Load(); got != 0 {
		t.Fatalf("the shared connections were set aside %d times", got)
	}
	if n := target.connections(); n != 1 {
		t.Fatalf("the target saw %d probes, want one", n)
	}
}

// A node with shared connections whose first probe hangs gets its second
// probe at half the timeout and recovers over the new connection.
func TestTheSecondProbeOfANodeWithSharedConnectionsComesAtHalfTheTimeout(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect(), keeps: true}
	outcome, proxy := probeThrough(t, node, target)
	if outcome != probeRecovered || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if after := secondProbeAfter(t, target); after < 1400*time.Millisecond || after > 2200*time.Millisecond {
		t.Fatalf("the second probe came %s after the first, want about 1.5 s", after)
	}
	if got := node.retired.Load(); got != 1 {
		t.Fatalf("the shared connections were set aside %d times, want once", got)
	}
}

// A node without shared connections still gets its second probe at the
// hedge delay.
func TestTheSecondProbeOfANodeWithoutSharedConnectionsComesAtTheHedgeDelay(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "ok")
	node := &pooledNode{ProxyAdapter: outbound.NewDirect()}
	if outcome, _ := probeThrough(t, node, target); outcome != probeRecovered {
		t.Fatalf("outcome %v", outcome)
	}
	if after := secondProbeAfter(t, target); after < probeHedgeDelay-100*time.Millisecond || after > probeHedgeDelay+400*time.Millisecond {
		t.Fatalf("the second probe came %s after the first, want about %s", after, probeHedgeDelay)
	}
}

// A node with shared connections whose last probe was slower gets its second
// probe no earlier than a quicker one: at half the timeout on both sides of
// the hedge delay, unless that leaves the second probe less than one and a
// half of its usual time. Other nodes keep their hedge as it was.
func TestTheSecondProbeOfANodeWithSharedConnectionsIsNeverEarlierForASlowerNode(t *testing.T) {
	old := func(timeout, elapsed time.Duration) time.Duration {
		if elapsed < probeHedgeDelay {
			return probeHedgeDelay
		}
		at := 2 * elapsed
		if latest := probeHedgeDelay + timeout - elapsed*3/2; latest < at {
			at = latest
		}
		if at < probeHedgeDelay {
			at = probeHedgeDelay
		}
		return at
	}
	for _, timeout := range []time.Duration{1000, 3000, 5000, 10000} {
		timeout *= time.Millisecond
		hc := &HealthCheck{timeout: timeout}
		latest := func(elapsed time.Duration) time.Duration {
			return probeHedgeDelay + timeout - elapsed*3/2
		}
		for _, elapsed := range []time.Duration{500, 950, 1050, 2000, 4000} {
			elapsed *= time.Millisecond
			for _, keeps := range []bool{true, false} {
				at := hc.hedgeAt(elapsed, keeps)
				if !keeps && at != old(timeout, elapsed) {
					t.Errorf("timeout %s, last probe %s, no shared connections: hedge at %s, want %s", timeout, elapsed, at, old(timeout, elapsed))
				}
				if at < probeHedgeDelay {
					t.Errorf("timeout %s, last probe %s, keeps %v: hedge at %s, before the hedge delay", timeout, elapsed, keeps, at)
				}
				if elapsed >= probeHedgeDelay {
					bound := latest(elapsed)
					if bound < probeHedgeDelay {
						bound = probeHedgeDelay
					}
					if at > bound {
						t.Errorf("timeout %s, last probe %s, keeps %v: hedge at %s, after %s", timeout, elapsed, keeps, at, bound)
					}
				}
			}
		}
		// Around the hedge delay the hedge of a node with shared connections
		// does not jump earlier for the slower node.
		quick, slow := hc.hedgeAt(950*time.Millisecond, true), hc.hedgeAt(1050*time.Millisecond, true)
		floor := timeout / 2
		if l := latest(1050 * time.Millisecond); l < floor {
			floor = l
		}
		if slow < floor {
			t.Errorf("timeout %s: a last probe of 1.05 s gets its hedge at %s, before %s", timeout, slow, floor)
		}
		if slow < quick {
			t.Errorf("timeout %s: hedge at %s after 0.95 s but at %s after 1.05 s", timeout, quick, slow)
		}
	}
}

// stuckPoolNode keeps one connection for all its streams, and that
// connection is stuck: nothing over it answers until it is set aside.
type stuckPoolNode struct {
	C.ProxyAdapter
	retired atomic.Int32
}

func (n *stuckPoolNode) RetireConns() bool {
	n.retired.Add(1)
	return true
}

func (n *stuckPoolNode) KeepsConns() bool {
	return true
}

func (n *stuckPoolNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if n.retired.Load() == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return n.ProxyAdapter.DialContext(ctx, metadata)
}

func deadStuckPoolNode(t *testing.T, url string) (*stuckPoolNode, *adapter.Proxy) {
	t.Helper()
	node := &stuckPoolNode{ProxyAdapter: outbound.NewDirect()}
	proxy := adapter.NewProxy(node)
	proxy.RecordProbe(url, &C.ProbeResult{Held: true, Time: time.Now(), Err: errProbeFailed, Stage: C.ProbeStageDial})
	if proxy.AliveForTestUrl(url) {
		t.Fatal("the node must start dead")
	}
	return node, proxy
}

// A dead node that keeps shared connections is probed over a new one: over
// the stuck one it could not answer until its transport dropped it.
func TestADeadNodeWithSharedConnectionsIsProbedOverANewOne(t *testing.T) {
	target := newProbeTarget(t, 0)
	node, proxy := deadStuckPoolNode(t, target.url())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 500, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if got := node.retired.Load(); got != 1 {
		t.Fatalf("the kept connections were set aside %d times, want once", got)
	}
}

// The same for a probe asked out of schedule, a recheck of the dead node.
func TestARecheckOfADeadNodeWithSharedConnectionsGoesOverANewOne(t *testing.T) {
	target := newProbeTarget(t, 0)
	node, proxy := deadStuckPoolNode(t, target.url())
	const timeout = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), ProbeHedgeDelay+timeout+C.ProbeReserve)
	defer cancel()
	if _, err := ProbeNode(ctx, proxy, target.url(), nil, timeout); err != nil || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("err %v, alive %v", err, proxy.AliveForTestUrl(target.url()))
	}
	if got := node.retired.Load(); got != 1 {
		t.Fatalf("the kept connections were set aside %d times, want once", got)
	}
}
