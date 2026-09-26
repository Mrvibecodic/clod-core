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
	// Answered is set by URLTest when the error is a definite answer from the
	// network rather than a stall: the node's port is closed or its name does
	// not resolve.
	Answered bool
}

// OK reports whether the probe counts as a live node.
func (r *ProbeResult) OK() bool {
	return r.Held && r.Err == nil && r.Satisfied
}

// Retryable reports whether a failed probe may have been a momentary stall
// worth a second look. A closed port, a name that does not exist or an
// unexpected status code are answers, not stalls: asking again only hides them.
func (r *ProbeResult) Retryable() bool {
	if r.Stage == ProbeStageStatus || r.Stage == ProbeStageAddress || r.Err == nil {
		return false
	}
	return !r.Answered
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
