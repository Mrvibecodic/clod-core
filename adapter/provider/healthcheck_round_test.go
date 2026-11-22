package provider

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// deviceNet is the network of the device the nodes are reached over: the
// first failing connections through any of its nodes hang until their
// deadline, the way they do while the network switches, the rest go through.
type deviceNet struct {
	failing int32
	dials   atomic.Int32
	// downUntil: connections hang until then whatever their count.
	downUntil time.Time
}

// netNode is a node reached over a deviceNet; a dead one never answers.
type netNode struct {
	C.ProxyAdapter
	net   *deviceNet
	dead  bool
	dials atomic.Int32
}

func (n *netNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	n.dials.Add(1)
	if n.dead || time.Now().Before(n.net.downUntil) || n.net.dials.Add(1) <= n.net.failing {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return n.ProxyAdapter.DialContext(ctx, metadata)
}

const roundTestTimeout = 500

// shortRoundRetry has the rounds of the test wait less before their retry.
func shortRoundRetry(t *testing.T) {
	old := roundRetryDelay
	roundRetryDelay = 300 * time.Millisecond
	t.Cleanup(func() { roundRetryDelay = old })
}

// roundOf makes count nodes of net, the ones listed in dead never answering,
// and a target for their probes, with a short retry delay for the rounds.
func roundOf(t *testing.T, net *deviceNet, count int, dead ...int) ([]*netNode, []C.Proxy, string) {
	t.Helper()
	target := newProbeTarget(t, 0)
	shortRoundRetry(t)
	nodes := make([]*netNode, count)
	proxies := make([]C.Proxy, count)
	for i := range nodes {
		nodes[i] = &netNode{ProxyAdapter: outbound.NewDirect(), net: net}
		proxies[i] = adapter.NewProxy(nodes[i])
	}
	for _, i := range dead {
		nodes[i].dead = true
	}
	return nodes, proxies, target.url()
}

func runRoundOf(t *testing.T, proxies []C.Proxy, url string) {
	t.Helper()
	hc := NewHealthCheck(proxies, url, roundTestTimeout, 0, false, nil)
	t.Cleanup(hc.close)
	hc.check()
}

// A stall of the network at the round's time fails the probes in flight:
// the nodes probed first fail both their tries while the later ones pass.
// The round heard from the network, so the failed ones get a second look,
// and they answer it.
func TestAStallAtTheRoundsTimeMarksNoNodeDead(t *testing.T) {
	// Ten probes run at once: their first tries and the earliest second try
	// hang, the other second tries and the two nodes probed after them pass.
	net := &deviceNet{failing: 11}
	_, proxies, url := roundOf(t, net, 12)
	runRoundOf(t, proxies, url)
	for i, p := range proxies {
		if !p.AliveForTestUrl(url) {
			t.Fatalf("node %d was marked dead by a stall of the network: %v", i, delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(url)))
		}
	}
}

// A node that is really down fails its second look too and is marked dead;
// the nodes that answered are left as they were.
func TestADeadNodeIsMarkedAfterItsSecondLook(t *testing.T) {
	net := &deviceNet{}
	nodes, proxies, url := roundOf(t, net, 4, 2)
	runRoundOf(t, proxies, url)
	if proxies[2].AliveForTestUrl(url) {
		t.Fatal("a node that answered neither look stays alive")
	}
	if got := nodes[2].dials.Load(); got != 4 {
		t.Fatalf("the dead node got %d tries, want two for the round and two for the second look", got)
	}
	for i, p := range proxies {
		if i == 2 {
			continue
		}
		if history := delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(url)); !p.AliveForTestUrl(url) || len(history) != 1 || history[0] == 0 || nodes[i].dials.Load() != 1 {
			t.Fatalf("node %d: alive %t, history %v, %d tries", i, p.AliveForTestUrl(url), history, nodes[i].dials.Load())
		}
	}
}

// No node answered: the network of the device is gone for a moment. Nothing
// is recorded, the nodes are probed again, and they answer.
func TestARoundThatHeardFromNoNodeTriesAgain(t *testing.T) {
	const count = 4
	net := &deviceNet{failing: 2 * count}
	nodes, proxies, url := roundOf(t, net, count)
	runRoundOf(t, proxies, url)
	for i, p := range proxies {
		if history := delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(url)); !p.AliveForTestUrl(url) || len(history) != 1 || history[0] == 0 {
			t.Fatalf("node %d: alive %t, history %v: what the round found without a network must not be recorded", i, p.AliveForTestUrl(url), history)
		}
		if got := nodes[i].dials.Load(); got != 3 {
			t.Fatalf("node %d got %d tries, want two failed and one at the retry", i, got)
		}
	}
}

// No node answered the round nor its retry: the network is down for good,
// and the marks say so.
func TestARoundThatHeardFromNoNodeTwiceMarksThemDead(t *testing.T) {
	const count = 4
	net := &deviceNet{failing: 4 * count}
	nodes, proxies, url := roundOf(t, net, count)
	runRoundOf(t, proxies, url)
	for i, p := range proxies {
		if p.AliveForTestUrl(url) {
			t.Fatalf("node %d answered neither the round nor its retry and stays alive", i)
		}
		if history := delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(url)); len(history) != 1 {
			t.Fatalf("node %d: one outage, one failure in the history: %v", i, history)
		}
		if got := nodes[i].dials.Load(); got != 4 {
			t.Fatalf("node %d got %d tries, want two in the round and two at the retry", i, got)
		}
	}
}

// A round that starts under the probe hold (a wake-up, a network change)
// waits for its end: the probes begun under it fail and are set aside, and
// the round would end having heard from no node.
func TestARoundStartedUnderTheProbeHoldWaitsForItsEnd(t *testing.T) {
	end := time.Now().Add(4 * time.Second)
	C.SetProbeHoldUntil(end)
	t.Cleanup(func() { C.SetProbeHoldUntil(time.Time{}) })
	net := &deviceNet{downUntil: end}
	_, proxies, url := roundOf(t, net, 3)
	runRoundOf(t, proxies, url)
	for i, p := range proxies {
		if history := delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(url)); len(history) != 1 || history[0] == 0 {
			t.Fatalf("node %d: history %v; want one answer after the hold", i, history)
		}
	}
}

// A node that was dead before the round gets its one probe and keeps its
// verdict; the retry of a round that heard from no node is for the live ones.
func TestANodeDeadBeforeTheRoundIsProbedOnce(t *testing.T) {
	net := &deviceNet{failing: 4}
	nodes, proxies, url := roundOf(t, net, 3, 0)
	proxies[0].(*adapter.Proxy).RecordProbe(url, &C.ProbeResult{Held: true, Time: time.Now(), Err: errProbeFailed, Stage: C.ProbeStageDial})
	runRoundOf(t, proxies, url)
	if got := nodes[0].dials.Load(); got != 1 || proxies[0].AliveForTestUrl(url) {
		t.Fatalf("the dead node got %d tries, alive %t; want one try, dead", got, proxies[0].AliveForTestUrl(url))
	}
	for i := 1; i < 3; i++ {
		if !proxies[i].AliveForTestUrl(url) {
			t.Fatalf("node %d answered the retry and stays dead", i)
		}
	}
}
