package outboundgroup

import (
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

var quick = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}

func deadNode(name string) *askedProxy {
	n := askedNode(name, "192.0.2.1", false)
	n.RecordProbe("", &C.ProbeResult{Held: true, Time: time.Now(), Err: C.ErrNotSupport})
	return n
}

func settle(d time.Duration) { time.Sleep(d) }

// The node the group left is probed again ahead of the scheduled check: once
// after each wait, and no more when the waits are over.
func TestTheNodeLeftIsProbedAfterEachWait(t *testing.T) {
	a := deadNode("A")
	gb, _, d := askedGroup(a)
	gb.recheck(a, d, quick)
	settle(3 * time.Second)
	if got := a.asks.Load(); got != 3 {
		t.Fatalf("the node was probed %d times, want one after each of the three waits", got)
	}
}

// A node that answers again has the group make its choice anew, and is
// probed no more.
func TestTheNodeThatAnswersAgainIsGivenBackToTheGroup(t *testing.T) {
	a := deadNode("A")
	gb, _, d := askedGroup(a)
	var again atomic.Int32
	d.again = func() { again.Add(1) }
	a.answers.Store(true)
	gb.recheck(a, d, quick)
	settle(600 * time.Millisecond)
	if a.dead.Load() || again.Load() != 1 || a.asks.Load() != 1 {
		t.Fatalf("dead %v, the choice made anew %d times, probes %d", a.dead.Load(), again.Load(), a.asks.Load())
	}
}

// Only the last node left is probed: a new series ends the one before, and a
// node that is alive starts none.
func TestOnlyTheLastNodeLeftIsProbed(t *testing.T) {
	a, b := deadNode("A"), deadNode("B")
	alive := askedNode("C", "192.0.2.3", true)
	gb, _, d := askedGroup(a, b, alive)
	once := []time.Duration{100 * time.Millisecond}
	gb.recheck(a, d, once)
	gb.recheck(alive, d, once)
	gb.recheck(b, d, once)
	settle(1500 * time.Millisecond)
	if a.asks.Load() != 0 || alive.asks.Load() != 0 || b.asks.Load() != 1 {
		t.Fatalf("probes: first left %d, alive %d, last left %d", a.asks.Load(), alive.asks.Load(), b.asks.Load())
	}
}
