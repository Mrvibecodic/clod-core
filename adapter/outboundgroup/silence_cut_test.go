package outboundgroup

import (
	"testing"
	"time"
)

// cuts counts n connections through node cut before their first byte.
func cuts(gb *GroupBase, node *measuredProxy, asks *askCounter, n int) {
	for i := 0; i < n; i++ {
		gb.noteCut(node, born, time.Now(), asks.ask)
	}
}

func cutGroup() (*GroupBase, *measuredProxy, *askCounter) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	takenLongAgo(gb, node)
	return gb, node, asks
}

func windows(gb *GroupBase) (quiet, cut time.Time) {
	gb.silence.mu.Lock()
	defer gb.silence.mu.Unlock()
	return gb.silence.quietUntil, gb.silence.cutQuietUntil
}

// A check started by cut connections that found the node alive leaves its
// silence counted: a node that goes silent after a burst of cut connections
// is asked about at once, not when the pause is over.
func TestSilenceAfterACheckOfCutsStillAsks(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	takenLongAgo(gb, node)
	cut, conns := refusedConns(t, gb, node, asks, silenceCount)
	cut()
	readAll(conns)
	asks.want(t, 1)
	gb.asked(node, verdictAlive, false)
	silent(gb, node, asks, silenceCount)
	asks.want(t, 2)
}

// The pause such a check leaves holds back cut connections.
func TestThePauseOfCutsHoldsCuts(t *testing.T) {
	gb, node, asks := cutGroup()
	cuts(gb, node, asks, silenceCount)
	asks.want(t, 1)
	gb.asked(node, verdictAlive, false)
	cuts(gb, node, asks, 3*silenceCount)
	asks.want(t, 1)
	// Cut connections held back count nothing toward silence either.
	silent(gb, node, asks, silenceCount-1)
	asks.want(t, 1)
}

// Two cut connections and one left silent are silence: the check they start
// leaves the pause of a silent site, as before.
func TestCutsMixedWithSilenceLeaveThePauseOfASilentSite(t *testing.T) {
	gb, node, asks := cutGroup()
	cuts(gb, node, asks, silenceCount-1)
	silent(gb, node, asks, 1)
	asks.want(t, 1)
	gb.asked(node, verdictAlive, false)
	silent(gb, node, asks, silenceCount)
	cuts(gb, node, asks, silenceCount)
	asks.want(t, 1)
	if quiet, cut := windows(gb); time.Until(quiet).Round(time.Second) != silencePause || !cut.IsZero() {
		t.Fatalf("pauses %s and %s, want %s and none", time.Until(quiet), time.Until(cut), silencePause)
	}
}

// A check started by silence while the pause of cut connections runs takes
// the next pause in advance: two checks in two pauses, as there were.
func TestACheckBySilenceDuringThePauseOfCutsTakesTheNextPause(t *testing.T) {
	gb, node, asks := cutGroup()
	cuts(gb, node, asks, silenceCount)
	gb.asked(node, verdictAlive, false)
	_, cutUntil := windows(gb)
	silent(gb, node, asks, silenceCount)
	asks.want(t, 2)
	gb.asked(node, verdictAlive, false)
	quiet, cut := windows(gb)
	if want := cutUntil.Add(2 * silencePause); quiet.Before(want.Add(-time.Second)) || !cut.Equal(quiet) {
		t.Fatalf("the pauses end %s and %s from now, want both %s", time.Until(quiet), time.Until(cut), time.Until(want))
	}
	silent(gb, node, asks, silenceCount)
	cuts(gb, node, asks, silenceCount)
	asks.want(t, 2)
}

// The pause of cut connections takes nothing off the pause of a silent site,
// and a network found down still holds back both.
func TestThePauseOfCutsLeavesTheOtherPauses(t *testing.T) {
	gb, node, asks := cutGroup()
	silent(gb, node, asks, silenceCount)
	gb.asked(node, verdictAlive, false)
	quiet, _ := windows(gb)
	gb.silence.mu.Lock()
	gb.silence.asking, gb.silence.byCuts = true, true
	gb.silence.mu.Unlock()
	gb.asked(node, verdictAlive, false)
	if after, _ := windows(gb); !after.Equal(quiet) {
		t.Fatalf("the pause of a silent site moved by %s", after.Sub(quiet))
	}

	gb, node, asks = cutGroup()
	cuts(gb, node, asks, silenceCount)
	gb.asked(node, verdictAlive, false)
	silent(gb, node, asks, silenceCount)
	asks.want(t, 2)
	gb.asked(node, verdictNoNetwork, false)
	silent(gb, node, asks, silenceCount)
	gb.silence.mu.Lock()
	gb.silence.cutQuietUntil = time.Now()
	gb.silence.mu.Unlock()
	cuts(gb, node, asks, silenceCount)
	asks.want(t, 2)
	// An answer ends the wait of a lost network, not that of cut connections.
	gb.noteAnswer(node, born)
	gb.silence.mu.Lock()
	gb.silence.answeredAt = time.Now().Add(-silenceAfter - time.Second)
	gb.silence.cutQuietUntil = time.Now().Add(time.Minute)
	gb.silence.mu.Unlock()
	cuts(gb, node, asks, silenceCount)
	asks.want(t, 2)
	silent(gb, node, asks, silenceCount)
	asks.want(t, 3)
}

// Bursts of cut connections every 20 to 30 seconds, against a pause of 30:
// every other burst comes after the pause. The node is asked about once a
// pause, as it was.
func TestBurstsOfCutsAskOncePerPause(t *testing.T) {
	gb, node, asks := cutGroup()
	const bursts = 6
	for i := 0; i < bursts; i++ {
		if i%2 == 0 {
			gb.silence.mu.Lock()
			gb.silence.quietUntil, gb.silence.cutQuietUntil = time.Now(), time.Now()
			gb.silence.mu.Unlock()
		}
		before := asks.n.Load()
		cuts(gb, node, asks, 5)
		asks.want(t, before+int32((i+1)%2))
		if asks.n.Load() > before {
			gb.asked(node, verdictAlive, false)
		}
	}
	asks.want(t, bursts/2)
}
