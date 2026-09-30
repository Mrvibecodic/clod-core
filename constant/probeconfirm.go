package constant

import (
	"context"
	"fmt"
	"time"
)

// Probe stages: how far a probe got before it failed. A failure while
// connecting to the node (its address, TCP, the transport handshake) points at
// the path to the server; a failure after that points at the node itself or at
// what it has to reach.
const (
	ProbeStageAddress = "адрес пробы"
	ProbeStageDial    = "подключение к узлу"
	ProbeStageRequest = "запрос через узел"
	ProbeStageStatus  = "код ответа"
)

// ProbeResult is the outcome of a Proxy.URLTest run in held mode: it is kept
// here instead of being written to the node's history, and the caller records
// it through ProbeRecorder once it knows what the probe means.
type ProbeResult struct {
	Held      bool // set by URLTest: the probe ran and filled the result
	Time      time.Time
	Delay     uint16
	Err       error
	Satisfied bool
	Status    int
	Stage     string
	Elapsed   time.Duration
}

// OK reports whether the probe counts as a live node.
func (r *ProbeResult) OK() bool {
	return r.Held && r.Err == nil && r.Satisfied
}

// Retryable reports whether a failed probe is worth a second look. Every
// failure on the way to the node or through it is: a closed port, a name that
// does not resolve or an odd status code also come from a network that is
// switching or waking up. Only a probe URL that cannot be used at all says
// nothing that a second probe could change.
func (r *ProbeResult) Retryable() bool {
	return !r.OK() && r.Stage != ProbeStageAddress
}

func (r *ProbeResult) String() string {
	if r.OK() {
		return fmt.Sprintf("%d мс", r.Delay)
	}
	if r.Err == nil {
		return fmt.Sprintf("%s %d за %d мс", r.Stage, r.Status, r.Elapsed.Milliseconds())
	}
	return fmt.Sprintf("%s, %d мс: %v", r.Stage, r.Elapsed.Milliseconds(), r.Err)
}

// ProbeRecorder writes held probe results into a node's history.
type ProbeRecorder interface {
	RecordProbe(url string, result *ProbeResult)
	// RecordSoftFailure adds a failed probe to the history without marking
	// the node dead: the failure is remembered, the verdict is left to the
	// probe recorded after it.
	RecordSoftFailure(url string, at time.Time)
	// LastProbeElapsed is how long the last recorded probe of url took as a
	// whole, 0 if there is none.
	LastProbeElapsed(url string) time.Duration
}

type heldProbeKey struct{}

// WithHeldProbe makes Proxy.URLTest store its outcome in the returned
// ProbeResult instead of recording it in the node's history.
func WithHeldProbe(ctx context.Context) (context.Context, *ProbeResult) {
	held := &ProbeResult{}
	return context.WithValue(ctx, heldProbeKey{}, held), held
}

func HeldProbe(ctx context.Context) *ProbeResult {
	held, _ := ctx.Value(heldProbeKey{}).(*ProbeResult)
	return held
}
