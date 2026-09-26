package constant

import (
	"context"
	"net"
	"sync"
	"time"
)

// ProbeSpacing is the least time between the starts of two probes to the same
// server host. A group of nodes on one host used to be probed as a burst of
// handshakes to one address; a censor that counts handshakes per address
// (doctormobile measured a minute-long block after about six in a row) takes
// the burst for an attack and blocks the address, and the probes themselves
// produce the "dead" nodes they were meant to find.
const ProbeSpacing = 250 * time.Millisecond

// ProbeReserve is how much of the caller's deadline the pacing leaves for the
// probe itself: waiting for a slot never eats the time the probe needs, so a
// client's timeout keeps meaning "the node did not answer".
const ProbeReserve = 3 * time.Second

// ProbeHost is the host a probe's handshake goes to, from a node's Addr():
// the port is left out, the censor sees an address.
func ProbeHost(addr string) string {
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

type probePacedKey struct{}

// MarkProbePaced says that the caller has already waited for the probe's
// start slot, so URLTest must not book another one.
func MarkProbePaced(ctx context.Context) context.Context {
	return context.WithValue(ctx, probePacedKey{}, struct{}{})
}

var probePacer = struct {
	sync.Mutex
	next map[string]time.Time
}{next: map[string]time.Time{}}

// ProbePace waits until a probe to host may start: ProbeSpacing after the
// previous start to the same host, at most until ProbeReserve before the
// deadline of ctx. It returns ctx's error if ctx ends while waiting. An empty
// host is not paced.
func ProbePace(ctx context.Context, host string) error {
	if host == "" || ctx.Value(probePacedKey{}) != nil {
		return nil
	}
	wait := reserveProbeStart(host, time.Now())
	if deadline, ok := ctx.Deadline(); ok {
		if room := time.Until(deadline) - ProbeReserve; wait > room {
			wait = room
		}
	}
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reserveProbeStart books the next start slot for host and returns how long
// the caller has to wait for it.
func reserveProbeStart(host string, now time.Time) time.Duration {
	probePacer.Lock()
	defer probePacer.Unlock()
	if len(probePacer.next) > 1024 {
		for h, next := range probePacer.next {
			if now.After(next) {
				delete(probePacer.next, h)
			}
		}
	}
	start := now
	if next, ok := probePacer.next[host]; ok && next.After(now) {
		start = next
	}
	probePacer.next[host] = start.Add(ProbeSpacing)
	return start.Sub(now)
}
