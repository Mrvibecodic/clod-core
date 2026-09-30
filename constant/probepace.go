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

var probeHostOf func(Proxy) string

// SetProbeHostOf is set by the tunnel, which knows the groups and the proxies
// named in dialer-proxy.
func SetProbeHostOf(fn func(Proxy) string) {
	probeHostOf = fn
}

// ProbeHostOf is the host the first handshake of a probe through p goes to,
// the one a censor sees: a group's current node's, a dialer-proxy's, and only
// then the node's own server. Probes are spaced out by it.
func ProbeHostOf(p Proxy) string {
	if probeHostOf != nil {
		return probeHostOf(p)
	}
	return ProbeHost(p.Addr())
}

type probePacedKey struct{}

// MarkProbePaced says that the caller has already waited for the probe's
// start slot, so URLTest must not book another one.
func MarkProbePaced(ctx context.Context) context.Context {
	return context.WithValue(ctx, probePacedKey{}, struct{}{})
}

// UnmarkProbePaced undoes MarkProbePaced: a second probe started beside one
// the caller has paced waits for a start slot of its own.
func UnmarkProbePaced(ctx context.Context) context.Context {
	return context.WithValue(ctx, probePacedKey{}, nil)
}

type probeBookedKey struct{}

// WithProbeBooked makes ProbePace call booked once the probe has booked its
// start slot, before it waits for it: a round starts the probes that must
// queue behind this one only then.
func WithProbeBooked(ctx context.Context, booked func()) context.Context {
	return context.WithValue(ctx, probeBookedKey{}, booked)
}

// ProbeBooked tells the caller of WithProbeBooked that the probe needs no
// turn of its own.
func ProbeBooked(ctx context.Context) {
	if booked, ok := ctx.Value(probeBookedKey{}).(func()); ok {
		booked()
	}
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
		ProbeBooked(ctx)
		return nil
	}
	limit := time.Duration(-1)
	if deadline, ok := ctx.Deadline(); ok {
		if limit = time.Until(deadline) - ProbeReserve; limit < 0 {
			limit = 0
		}
	}
	wait := reserveProbeStart(host, time.Now(), limit)
	ProbeBooked(ctx)
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
// the caller has to wait for it, at most limit (a negative limit is none). A
// caller that cannot wait for its slot starts earlier and books nothing past
// its real start: otherwise every short-deadline probe would push the probes
// behind it back without spacing anything itself.
func reserveProbeStart(host string, now time.Time, limit time.Duration) time.Duration {
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
	next, booked := probePacer.next[host]
	if booked && next.After(now) {
		start = next
	}
	if limit >= 0 && start.Sub(now) > limit {
		start = now.Add(limit)
	}
	if after := start.Add(ProbeSpacing); !booked || after.After(next) {
		probePacer.next[host] = after
	}
	return start.Sub(now)
}
