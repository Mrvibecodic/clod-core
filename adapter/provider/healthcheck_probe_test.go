package provider

import (
	"bufio"
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
	outcome := hc.probe(proxy, target.url(), nil)
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
	if history := delays(proxy.DelayHistoryForTestUrl(target.url())); len(history) != 2 {
		t.Fatalf("both failed probes belong in the history: %v", history)
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
		go func() { outcomes <- hc.probe(proxy, target.url(), nil) }()
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
