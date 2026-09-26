package constant

import (
	"sync/atomic"
	"time"
)

// ProbeFreezeGap is the longest pause between two heartbeats that still counts
// as the process being awake; a longer one means the device was asleep.
const ProbeFreezeGap = 45 * time.Second

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

func ProbeBeat(nanos int64) {
	probeLastBeat.Store(nanos)
}

func ProbeHolding(at time.Time) bool {
	if until := probeHoldUntil.Load(); until != nil && at.Before(*until) {
		return true
	}

	beat := probeLastBeat.Load()

	return beat != 0 && at.UnixNano()-beat > int64(ProbeFreezeGap)
}
