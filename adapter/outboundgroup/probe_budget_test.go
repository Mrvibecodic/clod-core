package outboundgroup

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

// lateKeptProxy keeps shared connections; its first try hangs, and its second
// answers after late, the way a node does over a slow link once its stuck
// connection is set aside.
type lateKeptProxy struct {
	*keptProxy
	late time.Duration
}

func (p *lateKeptProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	asks := p.asks.Add(1)
	result := C.HeldProbe(ctx)
	if asks > 1 {
		select {
		case <-time.After(p.late):
			*result = C.ProbeResult{Held: true, Time: time.Now(), Delay: 10, Satisfied: true}
			return 10, nil
		case <-ctx.Done():
		}
	} else {
		<-ctx.Done()
	}
	*result = C.ProbeResult{Held: true, Time: time.Now(), Err: ctx.Err(), Stage: C.ProbeStageRequest}
	return 0, ctx.Err()
}

// bookHost takes the turns to host for the next ahead, as other probes to the
// same server do.
func bookHost(host string, ahead time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for booked := time.Duration(0); booked <= ahead; booked += C.ProbeSpacing {
		_ = C.ProbePaceWithin(ctx, host, -1)
	}
}

// A probe of a node that keeps shared connections, from a check of the group,
// whose first try waited the whole reserve for its turn to the host: its
// second try, at half the timeout, still has a whole timeout of its own. It
// had only what was left of the hedge delay and the timeout, three seconds
// of four, and a node answering in three and a half was found silent.
func TestASecondTryAfterAWaitForTheTurnKeepsItsWholeTimeout(t *testing.T) {
	for _, probe := range []struct {
		name, host string
		run        func(gb *GroupBase, p C.Proxy, d doubt) bool
	}{
		{"a check of a doubt", "192.0.2.61", func(gb *GroupBase, p C.Proxy, d doubt) bool {
			return gb.askHeld([]C.Proxy{p}, d)[0].answered
		}},
		{"a control probe", "192.0.2.62", func(gb *GroupBase, p C.Proxy, d doubt) bool {
			gb.recheckTurn(p, d, time.Time{})
			return !p.(*lateKeptProxy).dead.Load() && p.(*lateKeptProxy).records() == 1
		}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			node := &lateKeptProxy{keptProxy: &keptProxy{askedProxy: askedNode("A", probe.host, true)}, late: 3500 * time.Millisecond}
			gb, _, d := lookGroup(node)
			gb.testTimeout = 4000
			bookHost(C.ProbeHost(node.Addr()), C.ProbeReserve)
			if !probe.run(gb, node, d) {
				t.Fatalf("the node answering its second try in %s was found silent", node.late)
			}
			if got := node.asks.Load(); got != 2 {
				t.Fatalf("the node was tried %d times, want 2", got)
			}
		})
	}
}
