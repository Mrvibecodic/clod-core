package constant

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var testRuns atomic.Int64

// testHosts names hosts for one test run: the pacer is process-wide, and a
// repeated run (-count) must not see the bookings of the one before.
func testHosts() func(name string) string {
	run := strconv.FormatInt(testRuns.Add(1), 10)
	return func(name string) string { return name + "-" + run + ".example" }
}

func TestProbesToOneHostAreSpacedOut(t *testing.T) {
	host := testHosts()
	now := time.Now()
	if wait := reserveProbeStart(host("pace-a"), now, -1); wait != 0 {
		t.Fatalf("the first probe to a host starts at once, got %s", wait)
	}
	if wait := reserveProbeStart(host("pace-a"), now, -1); wait != ProbeSpacing {
		t.Fatalf("the second probe waits one spacing, got %s", wait)
	}
	if wait := reserveProbeStart(host("pace-a"), now, -1); wait != 2*ProbeSpacing {
		t.Fatalf("the third probe queues behind the second, got %s", wait)
	}
	if wait := reserveProbeStart(host("pace-b"), now, -1); wait != 0 {
		t.Fatalf("another host is not held back, got %s", wait)
	}
	if wait := reserveProbeStart(host("pace-a"), now.Add(time.Second), -1); wait != 0 {
		t.Fatalf("a host left alone for a while is free again, got %s", wait)
	}
}

func TestPacingNeverEatsTheProbeReserve(t *testing.T) {
	host := testHosts()
	for i := 0; i < 3; i++ {
		_ = reserveProbeStart(host("pace-tight"), time.Now(), -1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), ProbeReserve+100*time.Millisecond)
	defer cancel()
	began := time.Now()
	if err := ProbePace(ctx, host("pace-tight")); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(began); waited > 200*time.Millisecond {
		t.Fatalf("with %s left the wait must shrink to what the deadline allows, waited %s", ProbeReserve+100*time.Millisecond, waited)
	}
}

func TestAnEmptyHostAndACancelledContextDoNotWait(t *testing.T) {
	host := testHosts()
	if err := ProbePace(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	_ = reserveProbeStart(host("pace-c"), time.Now(), -1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ProbePace(ctx, host("pace-c")); err == nil {
		t.Fatal("a cancelled caller must get its error back instead of a wait")
	}
}

// A probe whose deadline leaves no time to wait starts at once and does not
// push back the probes behind it.
func TestAProbeThatCannotWaitBooksNothingAhead(t *testing.T) {
	host := testHosts()
	now := time.Now()
	pd := host("pace-d")
	if wait := reserveProbeStart(pd, now, -1); wait != 0 {
		t.Fatalf("first probe waits %s", wait)
	}
	for i := 0; i < 5; i++ {
		if wait := reserveProbeStart(pd, now, 0); wait != 0 {
			t.Fatalf("a probe that cannot wait waited %s", wait)
		}
	}
	if wait := reserveProbeStart(pd, now, -1); wait != ProbeSpacing {
		t.Fatalf("the next waiting probe queues one spacing behind the first, got %s", wait)
	}
}
