package constant

import (
	"sync/atomic"
	"time"
)

// ProbeFreezeGap is the longest pause between two heartbeats that still counts
// as the process being awake; a longer one means the device was asleep.
const ProbeFreezeGap = 45 * time.Second

// ProbeWakeHold is how long probes stay held once the process is found awake
// after a sleep: the network may still be coming back.
const ProbeWakeHold = 5 * time.Second

var (
	// probeHoldUntil is compared on the monotonic clock, so wall clock jumps
	// neither stretch nor cut the hold window.
	probeHoldUntil atomic.Pointer[time.Time]
	// probeLastBeat is wall clock nanoseconds: the monotonic clock stops during
	// sleep, and a stale beat is exactly how sleep is detected here.
	probeLastBeat atomic.Int64
)

func SetProbeHoldUntil(until time.Time) {
	probeHoldUntil.Store(&until)
}

// CapProbeHoldUntil ends the hold by until at the latest: the network has been
// confirmed, whoever started the hold (a network change, a wake-up).
func CapProbeHoldUntil(until time.Time) {
	for {
		cur := probeHoldUntil.Load()
		if cur == nil || !until.Before(*cur) {
			return
		}
		if probeHoldUntil.CompareAndSwap(cur, &until) {
			return
		}
	}
}

func ProbeBeat(nanos int64) {
	probeLastBeat.Store(nanos)
}

// ProbeLastBeat is the last heartbeat in wall clock nanoseconds, 0 if none.
func ProbeLastBeat() int64 {
	return probeLastBeat.Load()
}

func ProbeHolding(at time.Time) bool {
	if until := probeHoldUntil.Load(); until != nil && at.Before(*until) {
		return true
	}

	beat := probeLastBeat.Load()
	if beat == 0 || at.UnixNano()-beat <= int64(ProbeFreezeGap) {
		return false
	}

	// The first one to find the process awake after a sleep starts the hold
	// from now: waiting for the next heartbeat stretched it by up to its
	// interval, since the heartbeat's clock stands still during sleep.
	now := time.Now()
	if probeLastBeat.CompareAndSwap(beat, now.UnixNano()) {
		SetProbeHoldUntil(now.Add(ProbeWakeHold))
	}

	return true
}
