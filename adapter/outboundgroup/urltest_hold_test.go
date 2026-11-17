package outboundgroup

import (
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// A check asked for by a doubt of the group's node measures it at its worst:
// the delays it left do not move the group to a faster node. The next probe
// of the node does, and a node found dead is left whatever is held.
func TestDelaysOfACheckByDoubtDoNotMoveTheGroup(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want A", got.Name())
	}

	a.delay, b.delay = 2000, 50
	a.probes = append(a.probes, C.DelayHistory{Time: time.Now(), Delay: 2000})
	time.Sleep(time.Millisecond)
	group.holdSpeed()
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("the group went to %s on the delays of the check", got.Name())
	}

	time.Sleep(time.Millisecond)
	a.probes = append(a.probes, C.DelayHistory{Time: time.Now(), Delay: 2000})
	group.fastSingle.Reset()
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s after the next probe of A, want the faster B", got.Name())
	}
}

func TestADeadNodeIsLeftWhateverIsHeld(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	group.fast(false)
	group.holdSpeed()
	a.alive, a.delay = false, 0xffff
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s, want B", got.Name())
	}
}

// A node that answers again is given back to the choice at once.
func TestANodeGivenBackEndsTheHold(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 300, alive: true}
	b := &measuredProxy{name: "B", delay: 320, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	group.fast(false)
	group.holdSpeed()
	b.delay = 50
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s while held, want A", got.Name())
	}
	group.release()
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s after the release, want B", got.Name())
	}
}
