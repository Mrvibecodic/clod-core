package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// The choice of a url-test group is kept for ten seconds. A node marked dead
// within them is left by the next connection, not when they run out.
func TestURLTestLeavesADeadNodeAtOnce(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})

	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("chosen %s, want A", got.Name())
	}
	// A dead node has no delay: the delay of a real one reads 0xffff.
	a.alive, a.delay = false, 0xffff
	if got := group.fast(false); got != C.Proxy(b) {
		t.Fatalf("chosen %s after A died, want B", got.Name())
	}
}

// With every node dead there is nowhere to go: the choice is not made anew on
// every connection.
func TestURLTestKeepsItsChoiceWhenEveryNodeIsDead(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 300, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	group.fast(false)
	a.alive, a.delay = false, 0xffff
	b.alive, b.delay = false, 0xffff
	first := group.fast(false)
	if again := group.fast(false); again != first {
		t.Fatalf("chosen %s, then %s", first.Name(), again.Name())
	}
}

// With every node dead there is nowhere to go: the group stays on its node
// rather than move to the first of the list.
func TestURLTestStaysOnItsNodeWhenEveryNodeIsDead(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 300, alive: true}
	b := &measuredProxy{name: "B", delay: 100, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	if got := group.fast(false); got != b {
		t.Fatalf("chosen %s, want B", got.Name())
	}
	a.alive, a.delay = false, 0xffff
	b.alive, b.delay = false, 0xffff
	group.fastSingle.Reset()
	if got := group.fast(false); got != b {
		t.Fatalf("with every node dead the group went to %s", got.Name())
	}
	a.alive, a.delay = true, 300
	if got := group.fast(false); got != a {
		t.Fatalf("the group stayed on a dead node beside an alive one: %s", got.Name())
	}
}
