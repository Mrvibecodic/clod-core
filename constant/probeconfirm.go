package constant

import "context"

// ProbeFailure collects the outcome of a probe whose failure must not be
// recorded yet: a single failed health check probe is only a suspicion, and
// the node is marked dead after a second probe confirms it.
type ProbeFailure struct {
	Failed bool
}

type probeFailureKey struct{}

// WithHeldProbeFailure makes Proxy.URLTest report a failure through the
// returned ProbeFailure instead of recording it in the node's history.
func WithHeldProbeFailure(ctx context.Context) (context.Context, *ProbeFailure) {
	held := &ProbeFailure{}
	return context.WithValue(ctx, probeFailureKey{}, held), held
}

func HeldProbeFailure(ctx context.Context) *ProbeFailure {
	held, _ := ctx.Value(probeFailureKey{}).(*ProbeFailure)
	return held
}
