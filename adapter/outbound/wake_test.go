package outbound

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func sleepFor(t *testing.T) func() {
	t.Cleanup(func() {
		C.ProbeBeat(0)
		C.SetProbeHoldUntil(time.Time{})
	})
	C.ProbeBeat(time.Now().UnixNano())

	// every sleep is from a beat of its own, whatever the step of the clock
	var slept int64
	return func() {
		slept++
		C.ProbeBeat(time.Now().Add(-time.Hour).UnixNano() + slept)
	}
}

func TestTheGuardDropsASessionOncePerSleep(t *testing.T) {
	sleep := sleepFor(t)

	var guard wakeGuard
	resets := 0
	renew := func() { guard.renew("node", func() { resets++ }) }

	sleep()
	renew()
	if resets != 0 {
		t.Fatal("a node that has not dialed yet has no session to drop, whatever happened before")
	}

	renew()
	if resets != 0 {
		t.Fatal("an awake process keeps its session")
	}

	sleep()
	renew()
	renew()
	if resets != 1 {
		t.Fatalf("one sleep drops the session once, dropped %d times", resets)
	}
}

func TestDialsRightAfterASleepWaitForTheDrop(t *testing.T) {
	sleep := sleepFor(t)

	var guard wakeGuard
	guard.renew("node", func() {})

	var dropped atomic.Bool
	var resets atomic.Int32
	reset := func() {
		resets.Add(1)
		time.Sleep(50 * time.Millisecond)
		dropped.Store(true)
	}

	sleep()

	var wg sync.WaitGroup
	var early atomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			guard.renew("node", reset)
			if !dropped.Load() {
				early.Add(1)
			}
		}()
	}
	wg.Wait()

	if resets.Load() != 1 {
		t.Fatalf("the session is dropped once, dropped %d times", resets.Load())
	}
	if early.Load() != 0 {
		t.Fatalf("%d dial(s) went on before the stale session was dropped", early.Load())
	}
}
