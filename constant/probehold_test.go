package constant

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTheWakeHoldRunsFromTheFirstLookAfterASleep(t *testing.T) {
	t.Cleanup(func() {
		ProbeBeat(0)
		SetProbeHoldUntil(time.Time{})
	})
	SetProbeHoldUntil(time.Time{})
	ProbeBeat(time.Now().Add(-time.Minute).UnixNano())

	if !ProbeHolding(time.Now()) {
		t.Fatal("a probe right after a sleep must be held")
	}
	if gap := time.Since(time.Unix(0, ProbeLastBeat())); gap > time.Second {
		t.Fatalf("the sleep must be taken as over now, beat is %s old", gap)
	}
	if !ProbeHolding(time.Now().Add(ProbeWakeHold - time.Second)) {
		t.Fatal("the hold must last ProbeWakeHold")
	}
	if ProbeHolding(time.Now().Add(ProbeWakeHold + time.Second)) {
		t.Fatal("the hold must end ProbeWakeHold after the sleep was found, not at the next heartbeat")
	}
}

func TestAConfirmedNetworkCutsTheWakeHold(t *testing.T) {
	t.Cleanup(func() {
		ProbeBeat(0)
		SetProbeHoldUntil(time.Time{})
	})
	SetProbeHoldUntil(time.Time{})
	ProbeBeat(time.Now().Add(-time.Minute).UnixNano())
	if !ProbeHolding(time.Now()) {
		t.Fatal("a probe right after a sleep must be held")
	}
	CapProbeHoldUntil(time.Now().Add(time.Second))
	if ProbeHolding(time.Now().Add(2 * time.Second)) {
		t.Fatal("the confirmed network must end the hold")
	}
	CapProbeHoldUntil(time.Now().Add(time.Minute))
	if ProbeHolding(time.Now().Add(2 * time.Second)) {
		t.Fatal("a cap never extends the hold")
	}
}

func TestASleepIsNamedOnceWhoeverFindsIt(t *testing.T) {
	t.Cleanup(func() {
		ProbeBeat(0)
		SetProbeHoldUntil(time.Time{})
	})
	SetProbeHoldUntil(time.Time{})
	ProbeBeat(0)

	none := ProbeWake()
	if ProbeWake() != none {
		t.Fatal("without a heartbeat there is no sleep to find")
	}

	ProbeBeat(time.Now().UnixNano())
	ProbeBeat(time.Now().Add(ProbeFreezeGap / 2).UnixNano())
	ProbeBeat(time.Now().UnixNano())
	if ProbeWake() != none {
		t.Fatal("heartbeats of an awake process are not a sleep")
	}

	ProbeBeat(time.Now().Add(-time.Minute).UnixNano())
	first := ProbeWake()
	if first == none {
		t.Fatal("a look after a sleep must find it")
	}
	if !ProbeHolding(time.Now()) {
		t.Fatal("the look that found the sleep starts the hold, like a probe does")
	}
	ProbeBeat(time.Now().UnixNano())
	if ProbeWake() != first {
		t.Fatal("the heartbeat after a found sleep must not make another one of it")
	}

	ProbeBeat(time.Now().Add(-2 * time.Minute).UnixNano())
	ProbeBeat(time.Now().UnixNano())
	if second := ProbeWake(); second == first || second == none {
		t.Fatal("a sleep found by the heartbeat is a sleep too")
	}
}

func TestNoLookAfterASleepMissesIt(t *testing.T) {
	t.Cleanup(func() {
		ProbeBeat(0)
		SetProbeHoldUntil(time.Time{})
	})

	for round := 0; round < 200000; round++ {
		ProbeBeat(time.Now().UnixNano())
		before := ProbeWake()
		// every round sleeps from a beat of its own, whatever the step of the clock
		ProbeBeat(time.Now().Add(-time.Hour).UnixNano() + int64(round))

		var wg sync.WaitGroup
		var missed atomic.Int32
		if round%2 == 1 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ProbeBeat(time.Now().UnixNano())
			}()
		}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ProbeWake() == before {
					missed.Add(1)
				}
			}()
		}
		wg.Wait()

		if missed.Load() != 0 {
			t.Fatalf("round %d: %d look(s) went past a sleep another one was just finding", round, missed.Load())
		}
	}
}
