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

// leftDead has p, a node the group left because it was found dead, probed
// again ahead of the scheduled check. Only the last node left is probed. A
// series that already follows p goes on as it is: a check that found p dead
// and the group that left it then both tell of the one leave.
func (gb *GroupBase) leftDead(p C.Proxy, d doubt) {
	gb.noteLeave(p)
	gb.recheckMux.Lock()
	following := gb.recheckOf == p
	gb.recheckMux.Unlock()
	if !following {
		gb.recheck(p, d, recheckPauses)
	}
}

// recheckControlPauses are the waits before the control probes a url-test
// group makes of a node the series found alive again, one probe after each:
// at 15, 30, 45 and 60 s. A node that is back for less than a minute is not
// gone back to, and neither is one that goes down for longer than a pause
// within that minute.
var recheckControlPauses = []time.Duration{15 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second}

// recheckReturnGuard is how long after a url-test group went back to a node
// through its control probes a new leave of that node counts as the node
// failing the return, and how long from that leave its revival takes no
// control probes: the group goes back to it only as its scheduled checks
// rank it. A node that flaps slower than the control probes reach would
// have the group come and go with it otherwise.
var recheckReturnGuard = 10 * time.Minute

// recheckRestarts is how many times in a row the series of one node starts
// over after a control probe it failed. A node that keeps coming and going
// is left to the scheduled checks after that: the series would follow it for
// as long as it flaps.
const recheckRestarts = 3

// recheck probes p after each of the pauses. The series ends when p is
// alive, when a newer series of the group has taken its place, or when p is
// no longer one of the group's nodes. A probe of p by someone else does not
// end it: the scheduled check and the checks of the group probe p as one of
// many and do not follow it up. A probe of theirs that is fresh at a turn is
// taken for the turn's own.
func (gb *GroupBase) recheck(p C.Proxy, d doubt, pauses []time.Duration) {
	gb.recheckEvery(p, d, pauses, recheckControlPauses, 0)
}

// recheckEvery starts the series of p, with controls the waits before the
// control probes; see revived. A series started in place of another, after,
// starts only if no newer one has taken that one's place, and only
// recheckRestarts times in a row: a series started by a leave counts anew.
func (gb *GroupBase) recheckEvery(p C.Proxy, d doubt, pauses []time.Duration, controls []time.Duration, after int32) {
	if p.AliveForTestUrl(d.url) {
		return
	}
	gb.recheckMux.Lock()
	if after != 0 && gb.rechecks.Load() != after {
		gb.recheckMux.Unlock()
		return
	}
	if after == 0 {
		gb.recheckRestarts = 0
	} else if gb.recheckRestarts++; gb.recheckRestarts > recheckRestarts {
		gb.recheckMux.Unlock()
		log.Infoln("[Проба] %s: то отвечает, то нет — дальше его проверяет только плановая проверка", p.Name())
		return
	}
	series := gb.rechecks.Add(1)
	gb.recheckOf = p
	// A probation belongs to the series it was found alive by.
	gb.probation = nil
	gb.recheckMux.Unlock()
	seen := lastProbe(p, d.url)
	go func() {
		defer gb.recheckEnded(series)
		for _, pause := range pauses {
			time.Sleep(pause)
			if !gb.rechecking(p, series) {
				return
			}
			seen = gb.recheckTurn(p, d, seen)
			if p.AliveForTestUrl(d.url) {
				log.Infoln("[Проба] %s снова отвечает", p.Name())
				gb.revived(p, d, series, func() { gb.recheckEvery(p, d, pauses, controls, series) }, controls)
				return
			}
		}
	}()
}

// probeBudget is how long a check of the group gives a probe of ProbeNode at
// timeout: its first try may wait up to ProbeReserve for its turn to the
// host, and a second try started after that still has a whole timeout of its
// own before the end of the probe, which takes ProbeReserve more.
func probeBudget(timeout time.Duration) time.Duration {
	return provider.ProbeHedgeDelay + timeout + 2*C.ProbeReserve
}

// recheckTurn probes p unless someone else has, since seen, and recently
// enough for the series to take their answer. It returns when p was last
// probed.
func (gb *GroupBase) recheckTurn(p C.Proxy, d doubt, seen time.Time) time.Time {
	if last := lastProbe(p, d.url); last.Equal(seen) || time.Since(last) >= recheckFresh {
		timeout := time.Duration(gb.testTimeout) * time.Millisecond
		expected, _ := utils.NewUnsignedRanges[uint16](d.expected)
		ctx, cancel := context.WithTimeout(context.Background(), probeBudget(timeout))
		_, _ = provider.ProbeNode(ctx, p, d.url, expected, timeout)
		cancel()
	}
	return lastProbe(p, d.url)
}

// revived gives p, found alive again, back to the group. A url-test group
// ranks a node by its last few probes, and the failures among them keep it
// off a node that is back for minutes: control probes, after each of the
// controls waits, that p all passes leave it none, and the group makes its
// choice anew. A control probe that fails leaves p dead by its own record,
// and the series starts over unless a newer one has taken its place. A node
// that is up for a moment and down again is not gone back to: its failures
// stay in the records the group ranks it by. Neither is one that passed the
// control probes with a failure still among the records it is ranked by, a
// try a control probe needed included: the scheduled checks rank it. Nor is
// one that the group went back to this way and left dead again soon after,
// for recheckReturnGuard from that leave: it gets no control probes then.
// While its control probes run, p is on probation: the group does not go to
// it for its speed, which its clean records would have it do long before
// the last one.
//
// Any other group takes p back as soon as it is alive.
func (gb *GroupBase) revived(p C.Proxy, d doubt, series int32, anew func(), controls []time.Duration) {
	if gb.Type() == C.URLTest {
		gb.recheckMux.Lock()
		guarded := gb.returned == p && time.Now().Before(gb.returnGuardUntil)
		if guarded && gb.rechecks.Load() == series {
			gb.recheckRestarts = 0
		}
		gb.recheckMux.Unlock()
		if guarded {
			log.Infoln("[Проба] %s снова отвечает; недавно возвращался и упал — возврат теперь только по плановым проверкам", p.Name())
			return
		}
		// Until the control probes are over the group does not go to p for
		// its speed: three clean records are enough for that, and p would
		// be gone back to before most of its control probes.
		gb.recheckMux.Lock()
		if gb.rechecks.Load() == series {
			gb.probation, gb.probationOf = p, series
		}
		gb.recheckMux.Unlock()
		defer gb.endProbation(series)
		seen := lastProbe(p, d.url)
		for _, pause := range controls {
			time.Sleep(pause)
			if !gb.rechecking(p, series) {
				return
			}
			if seen = gb.recheckTurn(p, d, seen); !p.AliveForTestUrl(d.url) {
				anew()
				return
			}
		}
		gb.recheckMux.Lock()
		if gb.rechecks.Load() == series {
			gb.recheckRestarts = 0
		}
		gb.recheckMux.Unlock()
		gb.endProbation(series)
		if !cleanWindow(p, d.url) {
			return
		}
		gb.recheckMux.Lock()
		gb.returned, gb.returnedAt, gb.returnGuardUntil = p, time.Now(), time.Time{}
		gb.recheckMux.Unlock()
		log.Infoln("[Проба] %s: контрольные пробы прошли, узел снова в выборе", p.Name())
	}
	if d.again != nil {
		d.again()
	}
}

// noteLeave notes that the group leaves p dead. A leave of the node the group
// went back to through control probes, within recheckReturnGuard of that
// return, starts the guard: see revived. A node a provider update has taken
// out of the group is forgotten.
func (gb *GroupBase) noteLeave(p C.Proxy) {
	gb.recheckMux.Lock()
	returned := gb.returned
	gb.recheckMux.Unlock()
	if returned == nil {
		return
	}
	if !gb.hasNode(returned) {
		gb.recheckMux.Lock()
		if gb.returned == returned {
			gb.returned = nil
		}
		gb.recheckMux.Unlock()
		return
	}
	if returned != p {
		return
	}
	now := time.Now()
	gb.recheckMux.Lock()
	if gb.returned == p && now.Sub(gb.returnedAt) < recheckReturnGuard && !now.Before(gb.returnedAt) && !now.Before(gb.returnGuardUntil) {
		gb.returnGuardUntil = now.Add(recheckReturnGuard)
	}
	gb.recheckMux.Unlock()
}

// cleanWindow reports whether the records p is ranked by, the last
// flakyWindow, are all answers: no failure among them, a try a probe needed
// before it answered included.
func cleanWindow(p C.Proxy, url string) bool {
	history := probeHistory(p, url)
	if len(history) < flakyWindow {
		return false
	}
	for _, record := range history[len(history)-flakyWindow:] {
		if record.Delay == 0 {
			return false
		}
	}
	return true
}

// recheckEnded marks the series over unless a newer one has taken its place.
func (gb *GroupBase) recheckEnded(series int32) {
	gb.recheckMux.Lock()
	if gb.rechecks.Load() == series {
		gb.recheckOf = nil
	}
	gb.recheckMux.Unlock()
	gb.endProbation(series)
}

// endProbation ends the probation the series put its node on, if it did.
func (gb *GroupBase) endProbation(series int32) {
	gb.recheckMux.Lock()
	if gb.probationOf == series {
		gb.probation = nil
	}
	gb.recheckMux.Unlock()
}

// onProbation is the node the group's series found alive and is making its
// control probes of, nil if none: the group does not go to it for its speed.
// A probation is the series' own: a newer series ends it.
func (gb *GroupBase) onProbation() C.Proxy {
	gb.recheckMux.Lock()
	defer gb.recheckMux.Unlock()
	if gb.probation == nil || gb.probationOf != gb.rechecks.Load() {
		return nil
	}
	return gb.probation
}

// rechecking reports whether the series is still the group's latest and p
// still one of its nodes: a provider update that dropped p ends it.
func (gb *GroupBase) rechecking(p C.Proxy, series int32) bool {
	if gb.rechecks.Load() != series {
		return false
	}
	return gb.hasNode(p)
}

// hasNode reports whether p is one of the group's nodes.
func (gb *GroupBase) hasNode(p C.Proxy) bool {
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
