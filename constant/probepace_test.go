package constant

import (
	"context"
	"testing"
	"time"
)

func TestProbesToOneHostAreSpacedOut(t *testing.T) {
	now := time.Now()
	if wait := reserveProbeStart("pace-a.example", now); wait != 0 {
		t.Fatalf("the first probe to a host starts at once, got %s", wait)
	}
	if wait := reserveProbeStart("pace-a.example", now); wait != ProbeSpacing {
		t.Fatalf("the second probe waits one spacing, got %s", wait)
	}
	if wait := reserveProbeStart("pace-a.example", now); wait != 2*ProbeSpacing {
		t.Fatalf("the third probe queues behind the second, got %s", wait)
	}
	if wait := reserveProbeStart("pace-b.example", now); wait != 0 {
		t.Fatalf("another host is not held back, got %s", wait)
	}
	if wait := reserveProbeStart("pace-a.example", now.Add(time.Second)); wait != 0 {
		t.Fatalf("a host left alone for a while is free again, got %s", wait)
	}
}

func TestPacingNeverEatsTheProbeReserve(t *testing.T) {
	for i := 0; i < 3; i++ {
		_ = reserveProbeStart("pace-tight.example", time.Now())
	}
	ctx, cancel := context.WithTimeout(context.Background(), ProbeReserve+100*time.Millisecond)
	defer cancel()
	began := time.Now()
	if err := ProbePace(ctx, "pace-tight.example"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(began); waited > 200*time.Millisecond {
		t.Fatalf("with %s left the wait must shrink to what the deadline allows, waited %s", ProbeReserve+100*time.Millisecond, waited)
	}
}

func TestAnEmptyHostAndACancelledContextDoNotWait(t *testing.T) {
	if err := ProbePace(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	_ = reserveProbeStart("pace-c.example", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ProbePace(ctx, "pace-c.example"); err == nil {
		t.Fatal("a cancelled caller must get its error back instead of a wait")
	}
}
