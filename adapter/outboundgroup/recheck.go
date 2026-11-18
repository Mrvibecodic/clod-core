package outboundgroup

import (
	"context"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// recheckPauses are the waits before the probes of a node the group left: a
// check out of schedule found it dead, the scheduled one may be minutes
// away, and until the node is probed again the group stays where it went.
// One probe after each wait, until the node answers.
var recheckPauses = []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute}

// recheckFresh is how old a probe of the node by someone else may be for the
// series to take its answer instead of probing the node itself.
const recheckFresh = 10 * time.Second

// leftDead has p, the node the group was on when a check out of schedule
// found it dead, probed again ahead of the scheduled check. Only the last
// node left is probed.
func (gb *GroupBase) leftDead(p C.Proxy, d doubt) {
	gb.recheck(p, d, recheckPauses)
}

// recheck probes p after each of the pauses. The series ends when p is
// alive, when a newer series of the group has taken its place, or when p is
// no longer one of the group's nodes. A probe of p by someone else does not
// end it: the scheduled check and the checks of the group probe p as one of
// many and do not follow it up. A probe of theirs that is fresh at a turn is
// taken for the turn's own.
func (gb *GroupBase) recheck(p C.Proxy, d doubt, pauses []time.Duration) {
	if p.AliveForTestUrl(d.url) {
		return
	}
	series := gb.rechecks.Add(1)
	seen := lastProbe(p, d.url)
	go func() {
		timeout := time.Duration(gb.testTimeout) * time.Millisecond
		expected, _ := utils.NewUnsignedRanges[uint16](d.expected)
		for _, pause := range pauses {
			time.Sleep(pause)
			if !gb.rechecking(p, series) {
				return
			}
			if last := lastProbe(p, d.url); last.Equal(seen) || time.Since(last) >= recheckFresh {
				ctx, cancel := context.WithTimeout(context.Background(), provider.ProbeHedgeDelay+timeout+C.ProbeReserve)
				_, _ = provider.ProbeNode(ctx, p, d.url, expected, timeout)
				cancel()
			}
			if p.AliveForTestUrl(d.url) {
				log.Infoln("[Проба] %s снова отвечает", p.Name())
				if d.again != nil {
					d.again()
				}
				return
			}
			seen = lastProbe(p, d.url)
		}
	}()
}

// rechecking reports whether the series is still the group's latest and p
// still one of its nodes: a provider update that dropped p ends it.
func (gb *GroupBase) rechecking(p C.Proxy, series int32) bool {
	if gb.rechecks.Load() != series {
		return false
	}
	for _, proxy := range gb.GetProxies(false) {
		if proxy == p {
			return true
		}
	}
	return false
}

// The pause between the checks of the whole group that failed connections
// through a dead node ask for while no node of the group is alive.
const (
	failedDeadPauseFirst = 5 * time.Second
	failedDeadPauseMax   = time.Minute
)

// failedDead answers the connections through p, a node marked dead, that
// failed. With another node alive the group has left p, or leaves it at its
// next connection: these are connections that were being opened through p
// when it was found dead, and they ask nothing. With no node alive there is
// nobody else to go to, and the whole group is checked to find the nodes
// that are back, though no more often than the pause allows: without a
// network every connection fails, and back-to-back checks would find nothing
// new. The pause doubles with every such check, up to failedDeadPauseMax,
// and starts anew once a connection through the group gets an answer or a
// node is alive again.
func (gb *GroupBase) failedDead(p C.Proxy, d doubt) {
	if len(d.next(p)) > 0 {
		gb.failedDeadOver()
		return
	}
	gb.failedDeadMux.Lock()
	now := time.Now()
	if now.Before(gb.failedDeadNext) {
		gb.failedDeadMux.Unlock()
		return
	}
	if gb.failedDeadPause == 0 {
		gb.failedDeadPause = failedDeadPauseFirst
	}
	gb.failedDeadNext = now.Add(gb.failedDeadPause)
	if gb.failedDeadPause *= 2; gb.failedDeadPause > failedDeadPauseMax {
		gb.failedDeadPause = failedDeadPauseMax
	}
	gb.failedDeadMux.Unlock()
	d.all()
	if len(d.next(nil)) > 0 {
		gb.failedDeadOver()
	}
}

// failedDeadOver ends the pause of failedDead: the group has a node that
// answers.
func (gb *GroupBase) failedDeadOver() {
	gb.failedDeadMux.Lock()
	gb.failedDeadNext, gb.failedDeadPause = time.Time{}, 0
	gb.failedDeadMux.Unlock()
}
