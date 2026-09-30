package provider

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// probeTarget answers probe connections by a plan, one entry per connection
// in the order they arrive: "ok" answers 204 at once, "slow" answers 204
// after a pause, "stall" never answers.
type probeTarget struct {
	listener net.Listener
	plan     []string
	slow     time.Duration
	mu       sync.Mutex
	held     []net.Conn
	conns    int
	firstAt  time.Time
}

func newProbeTarget(t *testing.T, slow time.Duration, plan ...string) *probeTarget {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := &probeTarget{listener: listener, plan: plan, slow: slow}
	go target.serve()
	t.Cleanup(target.close)
	return target
}

func (pt *probeTarget) port() int {
	return pt.listener.Addr().(*net.TCPAddr).Port
}

func (pt *probeTarget) url() string {
	return "http://" + pt.listener.Addr().String() + "/generate_204"
}

func (pt *probeTarget) connections() int {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.conns
}

func (pt *probeTarget) serve() {
	for {
		conn, err := pt.listener.Accept()
		if err != nil {
			return
		}
		pt.mu.Lock()
		n := pt.conns
		if n == 0 {
			pt.firstAt = time.Now()
		}
		pt.conns++
		action := "ok"
		if n < len(pt.plan) {
			action = pt.plan[n]
		}
		if action == "stall" {
			pt.held = append(pt.held, conn)
		}
		pt.mu.Unlock()
		if action == "stall" {
			continue
		}
		go func() {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			request, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			// A node of type http reaches the target through CONNECT first.
			if request.Method == http.MethodConnect {
				if _, err := conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
					return
				}
				if _, err := http.ReadRequest(reader); err != nil {
					return
				}
			}
			// Loopback answers within a millisecond, which URLTest rounds to a
			// delay of 0 — the value that means "failed"; a real node is slower.
			pause := 2 * time.Millisecond
			if action == "slow" {
				pause = pt.slow
			}
			time.Sleep(pause)
			_, _ = conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
		}()
	}
}

func (pt *probeTarget) close() {
	_ = pt.listener.Close()
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, conn := range pt.held {
		_ = conn.Close()
	}
}

func probeOnce(t *testing.T, target *probeTarget, timeout uint) (probeOutcome, time.Duration, *adapter.Proxy) {
	t.Helper()
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), timeout, 0, false, nil)
	t.Cleanup(hc.close)
	began := time.Now()
	outcome, _ := hc.probe(proxy, target.url(), nil)
	return outcome, time.Since(began), proxy
}

func delays(history []C.DelayHistory) []uint16 {
	out := make([]uint16, 0, len(history))
	for _, h := range history {
		out = append(out, h.Delay)
	}
	return out
}

func TestASteadyNodeGetsOneProbe(t *testing.T) {
	target := newProbeTarget(t, 0, "ok")
	outcome, elapsed, proxy := probeOnce(t, target, 1000)
	if outcome != probePassed || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if elapsed >= probeHedgeDelay {
		t.Fatalf("a steady node must be judged before the hedge: %s", elapsed)
	}
	if target.connections() != 1 {
		t.Fatalf("a steady node must see one probe, saw %d", target.connections())
	}
}

func TestAStalledFirstProbeIsHedgedWithinTheDelay(t *testing.T) {
	target := newProbeTarget(t, 0, "stall", "ok")
	outcome, elapsed, proxy := probeOnce(t, target, 3000)
	if outcome != probeRecovered || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if elapsed > probeHedgeDelay+500*time.Millisecond {
		t.Fatalf("the hedge must answer right after the delay, not after the timeout: %s", elapsed)
	}
	history := delays(proxy.DelayHistoryForTestUrl(target.url()))
	if len(history) != 2 || history[0] != 0 || history[1] == 0 {
		t.Fatalf("the stall must stay in the history before the answer: %v", history)
	}
}

func TestTwoStalledProbesCostOneTimeoutPlusTheHedge(t *testing.T) {
	const timeout = 500
	target := newProbeTarget(t, 0, "stall", "stall")
	outcome, elapsed, proxy := probeOnce(t, target, timeout)
	if outcome != probeStalled || proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	worst := probeHedgeDelay + timeout*time.Millisecond
	if elapsed > worst+300*time.Millisecond {
		t.Fatalf("a dead node must be judged within hedge+timeout (%s), took %s", worst, elapsed)
	}
	if elapsed < worst-100*time.Millisecond {
		t.Fatalf("the second probe must get its full timeout (%s), took %s", worst, elapsed)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 {
		t.Fatalf("one outage, one failure in the history: %v", history)
	}
}

func TestASlowAnswerToTheFirstProbeWins(t *testing.T) {
	target := newProbeTarget(t, probeHedgeDelay+300*time.Millisecond, "slow", "stall")
	outcome, elapsed, proxy := probeOnce(t, target, 3000)
	if outcome != probePassed || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if elapsed > probeHedgeDelay+800*time.Millisecond {
		t.Fatalf("the slow answer must end the wait, not the hedge's timeout: %s", elapsed)
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 || history[0] == 0 {
		t.Fatalf("only the node's own answer belongs in the history: %v", history)
	}
}

func TestAFirstProbeJustSlowerThanTheSecondIsNotAFailure(t *testing.T) {
	// A connect packet resent at about the hedge: the first probe answers
	// right after the second one, and the node has not failed anything.
	target := newProbeTarget(t, probeHedgeDelay+probeHedgeGrace/2, "slow", "ok")
	outcome, _, proxy := probeOnce(t, target, 3000)
	if outcome != probePassed || !proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 || history[0] == 0 {
		t.Fatalf("a slower first answer is no failure: %v", history)
	}
}

func TestAClosedPortIsNotHedged(t *testing.T) {
	target := newProbeTarget(t, 0)
	target.close()
	outcome, elapsed, proxy := probeOnce(t, target, 1000)
	if outcome != probeRefused || proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("outcome %v, alive %v", outcome, proxy.AliveForTestUrl(target.url()))
	}
	if elapsed >= probeHedgeDelay {
		t.Fatalf("a refused connection is an answer, no second probe: %s", elapsed)
	}
}

func TestANodeInTwoGroupsIsProbedOnce(t *testing.T) {
	target := newProbeTarget(t, 0, "ok", "ok")
	proxy := adapter.NewProxy(outbound.NewDirect())
	first := NewHealthCheck([]C.Proxy{proxy}, target.url(), 1000, 0, false, nil)
	second := NewHealthCheck([]C.Proxy{proxy}, target.url(), 1000, 0, false, nil)
	t.Cleanup(first.close)
	t.Cleanup(second.close)

	outcomes := make(chan probeOutcome, 2)
	for _, hc := range []*HealthCheck{first, second} {
		hc := hc
		go func() {
			outcome, _ := hc.probe(proxy, target.url(), nil)
			outcomes <- outcome
		}()
	}
	got := map[probeOutcome]int{}
	for i := 0; i < 2; i++ {
		got[<-outcomes]++
	}
	if got[probePassed] != 1 || got[probeShared] != 1 {
		t.Fatalf("one group probes, the other takes the outcome: %v", got)
	}
	if target.connections() != 1 {
		t.Fatalf("the node must see one probe, saw %d", target.connections())
	}
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 1 {
		t.Fatalf("one probe, one history record: %v", history)
	}
}

func TestProbesToOneHostDoNotBurst(t *testing.T) {
	target := newProbeTarget(t, 0)
	// Several nodes on one host: the probes go through the same pacer, so the
	// target sees their starts at least one spacing apart.
	const nodes = 4
	proxies := make([]C.Proxy, 0, nodes)
	for i := 0; i < nodes; i++ {
		node, err := outbound.NewHttp(outbound.HttpOption{
			Name:   fmt.Sprintf("paced-%d", i),
			Server: "127.0.0.1",
			Port:   target.port(),
		})
		if err != nil {
			t.Fatal(err)
		}
		proxies = append(proxies, adapter.NewProxy(node))
	}
	hc := NewHealthCheck(proxies, target.url(), 5000, 0, false, nil)
	t.Cleanup(hc.close)
	began := time.Now()
	hc.check()
	if elapsed := time.Since(began); elapsed < (nodes-1)*C.ProbeSpacing {
		t.Fatalf("%d probes to one host must be spaced out over at least %s, took %s", nodes, (nodes-1)*C.ProbeSpacing, elapsed)
	}
	if target.connections() < nodes {
		t.Fatalf("every node still gets its own probe, saw %d connections", target.connections())
	}
}

func TestAPacedProbeOfAClosedPortIsStillNotHedged(t *testing.T) {
	target := newProbeTarget(t, 0)
	target.close()
	// Eight nodes on one closed port: the last waits for its slot longer than
	// the hedge delay, then gets its answer at once — the hedge must count
	// from the probe's start, not from the wait, or the node is probed twice.
	const nodes = 8
	proxies := make([]C.Proxy, 0, nodes)
	for i := 0; i < nodes; i++ {
		node, err := outbound.NewHttp(outbound.HttpOption{
			Name:   fmt.Sprintf("closed-%d", i),
			Server: "127.0.0.1",
			Port:   target.port(),
		})
		if err != nil {
			t.Fatal(err)
		}
		proxies = append(proxies, adapter.NewProxy(node))
	}
	hc := NewHealthCheck(proxies, target.url(), 5000, 0, false, nil)
	t.Cleanup(hc.close)
	hc.check()
	for _, p := range proxies {
		if history := delays(p.(*adapter.Proxy).DelayHistoryForTestUrl(target.url())); len(history) != 1 {
			t.Fatalf("%s: a refused connection is one probe, got %v", p.Name(), history)
		}
	}
}

func TestTheHedgeWaitingForItsTurnDoesNotHoldBackTheFirstAnswer(t *testing.T) {
	// Other nodes of the host have booked its start slots ahead: the second
	// probe waits for one, and the first probe's answer meanwhile must end
	// the probe at once rather than after that wait.
	target := newProbeTarget(t, probeHedgeDelay+300*time.Millisecond, "slow", "ok")
	node, _ := outbound.NewHttp(outbound.HttpOption{Name: "n", Server: "127.0.0.1", Port: target.port()})
	proxy := adapter.NewProxy(node)
	var booked sync.WaitGroup
	for i := 0; i < 8; i++ {
		booked.Add(1)
		go func() {
			defer booked.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = C.ProbePace(ctx, "127.0.0.1")
		}()
	}
	// The slots are used up before the next test probes the same host.
	t.Cleanup(booked.Wait)
	time.Sleep(50 * time.Millisecond)
	began := time.Now()
	_, err := ProbeNode(C.MarkProbePaced(context.Background()), proxy, target.url(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("the node answered its first probe: %v", err)
	}
	if elapsed := time.Since(began); elapsed > probeHedgeDelay+700*time.Millisecond {
		t.Fatalf("the answer waited for the second probe's turn: %s", elapsed)
	}
}

func TestADeadNodeGetsOneProbe(t *testing.T) {
	// A second probe guards a live node; a dead one has no verdict to lose.
	target := newProbeTarget(t, 0, "stall", "stall", "stall", "stall")
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 500, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probeStalled {
		t.Fatalf("first round: %v", outcome)
	}
	began := time.Now()
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probeFailed {
		t.Fatalf("second round: %v", outcome)
	}
	if elapsed := time.Since(began); elapsed >= probeHedgeDelay {
		t.Fatalf("a dead node must be judged within its timeout: %s", elapsed)
	}
	if got := target.connections(); got != 3 {
		t.Fatalf("two probes for the live node, one for the dead: saw %d", got)
	}
}

func TestANodeKnownToBeSlowGetsOneProbe(t *testing.T) {
	target := newProbeTarget(t, probeHedgeDelay+300*time.Millisecond, "slow", "slow", "slow")
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 3000, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("first round: %v", outcome)
	}
	before := target.connections()
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("second round: %v", outcome)
	}
	if got := target.connections() - before; got != 1 {
		t.Fatalf("a node slower than the hedge delay gets one probe a round, saw %d", got)
	}
}

func TestAlternateHostsSpreadsTheNodesOfOneHost(t *testing.T) {
	var proxies []C.Proxy
	for i, host := range []string{"10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.2", "10.0.0.3"} {
		node, err := outbound.NewHttp(outbound.HttpOption{Name: fmt.Sprintf("%d", i), Server: host, Port: 443})
		if err != nil {
			t.Fatal(err)
		}
		proxies = append(proxies, adapter.NewProxy(node))
	}
	got := ""
	for _, p := range alternateHosts(proxies) {
		got += p.Name()
	}
	if got != "035142" {
		t.Fatalf("order %s, want 035142", got)
	}
}

func TestTheCurrentNodeIsProbedBeforeTheOtherNodesOfItsHost(t *testing.T) {
	// Six nodes on one host, the group uses the last one: its probe takes the
	// first turn to the host, whichever probe the scheduler runs first.
	var proxies []C.Proxy
	var targets []*probeTarget
	for i := 0; i < 6; i++ {
		target := newProbeTarget(t, 0)
		node, err := outbound.NewHttp(outbound.HttpOption{Name: fmt.Sprintf("host-%d", i), Server: "127.0.0.1", Port: target.port()})
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
		proxies = append(proxies, adapter.NewProxy(node))
	}
	current := proxies[5]
	C.SetProbeFirst(func() []C.Proxy { return []C.Proxy{current} })
	t.Cleanup(func() { C.SetProbeFirst(nil) })
	hc := NewHealthCheck(proxies, targets[0].url(), 5000, 0, false, nil)
	t.Cleanup(hc.close)
	hc.check()
	for i, target := range targets[:5] {
		if !targets[5].firstAt.Before(target.firstAt) {
			t.Fatalf("node %d was probed before the current node", i)
		}
	}
}

func TestAProbeThatWaitedForItsTurnIsNotHedgedAtOnce(t *testing.T) {
	// The hedge delay counts from the probe's start: a first probe that
	// waited more than the delay for its turn to the host still gets it.
	target := newProbeTarget(t, 300*time.Millisecond, "slow", "slow")
	node, _ := outbound.NewHttp(outbound.HttpOption{Name: "queued", Server: "127.0.0.1", Port: target.port()})
	proxy := adapter.NewProxy(node)
	var booked sync.WaitGroup
	for i := 0; i < 6; i++ {
		booked.Add(1)
		go func() {
			defer booked.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = C.ProbePace(ctx, "127.0.0.1")
		}()
	}
	t.Cleanup(booked.Wait)
	time.Sleep(50 * time.Millisecond)
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 5000, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("outcome %v", outcome)
	}
	if got := target.connections(); got != 1 {
		t.Fatalf("a node that answers within the delay gets one probe, saw %d", got)
	}
}

func TestASlowNodeThatStallsGetsOneProbe(t *testing.T) {
	// The Android build runs Go timers of 1.23 and later: Stop on a timer that
	// fired unread still reports true. A slow node never reads its hedge timer,
	// and its stalled probe must not be followed by a second one.
	target := newProbeTarget(t, probeHedgeDelay+300*time.Millisecond, "slow", "slow", "stall", "ok")
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 2000, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("first round: %v", outcome)
	}
	before := target.connections()
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probeFailed {
		t.Fatalf("a slow node that stalled: %v, want failed", outcome)
	}
	if got := target.connections() - before; got != 1 {
		t.Fatalf("a slow node that stalled gets one probe, saw %d", got)
	}
}

func TestASlowNodeThatStallsGetsALaterSecondProbe(t *testing.T) {
	// The node's probes take longer than the hedge delay: its second probe
	// comes at twice that time rather than never.
	const slow = probeHedgeDelay + 200*time.Millisecond
	target := newProbeTarget(t, slow, "slow", "slow", "stall", "ok")
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), 5000, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("first round: %v", outcome)
	}
	before := target.connections()
	began := time.Now()
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probeRecovered {
		t.Fatalf("a slow node that stalled: %v, want recovered", outcome)
	}
	if elapsed := time.Since(began); elapsed < 2*slow-50*time.Millisecond || elapsed > 2*slow+500*time.Millisecond {
		t.Fatalf("the second probe of a slow node comes at twice its last probe (%s), took %s", 2*slow, elapsed)
	}
	if got := target.connections() - before; got != 2 {
		t.Fatalf("a slow node that stalled gets a second probe, saw %d", got)
	}
}

func TestTheLaterSecondProbeEndsWithinTheHedgeAndTimeout(t *testing.T) {
	const slow = probeHedgeDelay + 200*time.Millisecond
	const timeout = 3000
	target := newProbeTarget(t, slow, "slow", "slow", "stall", "stall")
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, target.url(), timeout, 0, false, nil)
	t.Cleanup(hc.close)
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probePassed {
		t.Fatalf("first round: %v", outcome)
	}
	before := target.connections()
	began := time.Now()
	if outcome, _ := hc.probe(proxy, target.url(), nil); outcome != probeStalled {
		t.Fatalf("a slow node whose probes stalled: %v, want stalled", outcome)
	}
	worst := probeHedgeDelay + timeout*time.Millisecond
	if elapsed := time.Since(began); elapsed > worst+300*time.Millisecond || elapsed < worst-100*time.Millisecond {
		t.Fatalf("the later second probe ends by hedge+timeout (%s), took %s", worst, elapsed)
	}
	if got := target.connections() - before; got != 2 {
		t.Fatalf("two probes, saw %d", got)
	}
}

// keepAliveTarget answers every request of a connection after pause and keeps
// the connection: with unified-delay the recorded delay is the second request
// only.
func keepAliveTarget(t *testing.T, pause time.Duration) (string, func() int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	conns := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns++
			mu.Unlock()
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					if _, err := http.ReadRequest(reader); err != nil {
						return
					}
					time.Sleep(pause)
					if _, err := conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return "http://" + listener.Addr().String() + "/generate_204", func() int {
		mu.Lock()
		defer mu.Unlock()
		return conns
	}
}

func TestAFarNodeWithUnifiedDelayGetsOneProbe(t *testing.T) {
	// Each request takes 600 ms: the recorded delay is below the hedge delay,
	// the whole probe above it.
	adapter.UnifiedDelay.Store(true)
	t.Cleanup(func() { adapter.UnifiedDelay.Store(false) })
	url, conns := keepAliveTarget(t, 600*time.Millisecond)
	proxy := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{proxy}, url, 5000, 0, false, nil)
	t.Cleanup(hc.close)
	for round := 0; round < 3; round++ {
		before := conns()
		if outcome, _ := hc.probe(proxy, url, nil); outcome != probePassed {
			t.Fatalf("round %d: %v", round, outcome)
		}
		if got := conns() - before; round > 0 && got != 1 {
			t.Fatalf("round %d: delay %d ms, %d connections; a node whose probe takes longer than the hedge delay gets one", round, proxy.LastDelayForTestUrl(url), got)
		}
	}
}

func TestProbeNodeJudgesAStalledNodeWithinTheCallersDeadline(t *testing.T) {
	// The host's turns are booked for 7 s ahead: the first probe must not
	// wait so long that the second one runs past the caller's deadline and
	// the verdict is lost.
	target := newProbeTarget(t, 0, "stall", "stall")
	node, _ := outbound.NewHttp(outbound.HttpOption{Name: "n", Server: "127.0.0.1", Port: target.port()})
	proxy := adapter.NewProxy(node)
	var booked sync.WaitGroup
	for i := 0; i < 28; i++ {
		booked.Add(1)
		go func() {
			defer booked.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = C.ProbePace(ctx, "127.0.0.1")
		}()
	}
	t.Cleanup(booked.Wait)
	time.Sleep(50 * time.Millisecond)
	const attempt = 7 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), attempt+ProbeHedgeDelay+C.ProbeReserve)
	defer cancel()
	began := time.Now()
	_, err := ProbeNode(ctx, proxy, target.url(), nil, attempt)
	if ctx.Err() != nil || errors.Is(err, ErrProbeDiscarded) || proxy.AliveForTestUrl(target.url()) {
		t.Fatalf("no verdict within the deadline: %v after %s, alive %t", err, time.Since(began), proxy.AliveForTestUrl(target.url()))
	}
}
