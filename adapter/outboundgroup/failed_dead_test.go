package outboundgroup

import (
	"testing"
	"time"
)

func failedDeadState(gb *GroupBase) (time.Duration, time.Time) {
	gb.failedDeadMux.Lock()
	defer gb.failedDeadMux.Unlock()
	return gb.failedDeadPause, gb.failedDeadNext
}

// With every node dead failed connections have the whole group checked, but
// no more often than the pause allows: 5 s after the first check, then twice
// as long each time, up to a minute.
func TestFailuresWithEveryNodeDeadAreSpacedOut(t *testing.T) {
	a, b := deadNode("A"), deadNode("B")
	gb, provider, d := probingGroup(a, b)
	var spacing []time.Duration
	for _, want := range []time.Duration{5, 10, 20, 40, 60, 60} {
		checks := provider.checks.Load()
		gb.failedOf(a, time.Now(), d)
		if provider.checks.Load() != checks+1 {
			t.Fatalf("after %v: the failure did not have the group checked", spacing)
		}
		_, next := failedDeadState(gb)
		spacing = append(spacing, time.Until(next).Round(time.Second))
		if got := spacing[len(spacing)-1]; got != want*time.Second {
			t.Fatalf("spacing %v, want %s last", spacing, want*time.Second)
		}
		for i := 0; i < 3; i++ {
			gb.failedOf(b, time.Now(), d)
		}
		if provider.checks.Load() != checks+1 {
			t.Fatal("failures within the pause had the group checked")
		}
		// The pause is over.
		gb.failedDeadMux.Lock()
		gb.failedDeadNext = time.Now().Add(-time.Millisecond)
		gb.failedDeadMux.Unlock()
	}
}

// An answer through the group ends the pause: the next failures with every
// node dead have the group checked at once, and the pause starts short.
func TestAnAnswerEndsThePauseOfFailuresWithEveryNodeDead(t *testing.T) {
	a := deadNode("A")
	gb, provider, d := probingGroup(a)
	gb.failedOf(a, time.Now(), d)
	gb.failedOf(a, time.Now(), d)
	if provider.checks.Load() != 1 {
		t.Fatalf("checks %d, want 1", provider.checks.Load())
	}
	gb.noteAnswer(a, time.Now())
	gb.failedOf(a, time.Now(), d)
	if provider.checks.Load() != 2 {
		t.Fatal("a failure after an answer waited out the pause")
	}
	if _, next := failedDeadState(gb); time.Until(next).Round(time.Second) != failedDeadPauseFirst {
		t.Fatalf("the pause after an answer is %s", time.Until(next))
	}
}

// A check that finds a node alive ends the pause as well.
func TestACheckThatFindsANodeAliveEndsThePause(t *testing.T) {
	a := deadNode("A")
	gb, provider, d := probingGroup(a)
	a.answers.Store(true)
	gb.failedOf(a, time.Now(), d)
	if provider.checks.Load() != 1 || a.dead.Load() {
		t.Fatal("the check did not find the node alive")
	}
	if pause, next := failedDeadState(gb); pause != 0 || !next.IsZero() {
		t.Fatalf("pause %s, next %v after a node was found alive", pause, next)
	}
}
