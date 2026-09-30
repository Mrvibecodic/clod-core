package constant

import (
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
