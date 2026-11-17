package xhttp

import (
	"sync"
	"time"
)

// retireLimit is how many connections set aside a pool keeps open at most.
// A node slow under load would have one set aside on every check, and long
// sessions keep each of them open: past the limit nothing more is set aside.
const retireLimit = 2

// retireStep is how often a connection set aside is looked at.
var retireStep = time.Second

var (
	retiredMu sync.Mutex
	// retired counts the connections each client (or each pool retired on
	// its own) has set aside and not yet closed. One is dropped from it at
	// zero, so it does not keep the client alive.
	retired = map[any]int{}
)

// Retire has the next sessions open over new connections and reports whether
// there were connections to set aside. Those are left alone: their sessions
// go on for as long as they like, and each connection closes when its last
// session ends. A connection that stalled is not asked again, one that only
// lagged loses nothing. With nothing set aside and still open, the current
// connections are always set aside, however many they are: a pool as large as
// the server asks for is set aside whole once. Past that, nothing more is set
// aside and the sessions go on as they did while the connections set aside
// and still open, with the current ones that carry sessions, would go past
// retireLimit. Those without a session do not count: they are set aside with
// the rest and close at once.
//
// closed ends the wait: the connections set aside close with their client.
func (m *ReuseManager) Retire(closed <-chan struct{}) bool {
	if m == nil {
		return false
	}
	return retire(m, closed, m)
}

// retire sets the open connections of all the pools aside under owner, or
// none of them when owner has connections set aside and still open and those,
// with the ones that carry sessions, would go past retireLimit.
func retire(owner any, closed <-chan struct{}, pools ...*ReuseManager) bool {
	retiredMu.Lock()
	defer retiredMu.Unlock()
	for _, m := range pools {
		if m != nil {
			m.mu.Lock()
			defer m.mu.Unlock()
		}
	}
	var old []*reuseEntry
	busy := 0
	for _, m := range pools {
		if m == nil {
			continue
		}
		for _, entry := range m.entries {
			if !entry.isClosed() {
				old = append(old, entry)
				if entry.openUsage.Load() > 0 {
					busy++
				}
			}
		}
	}
	if len(old) == 0 || (retired[owner] > 0 && retired[owner]+busy > retireLimit) {
		return false
	}
	for _, m := range pools {
		if m != nil {
			m.entries = make([]*reuseEntry, 0)
		}
	}
	retired[owner] += len(old)
	go closeWhenIdle(owner, old, retireStep, closed)
	return true
}

// release takes n connections set aside off the count once they are closed.
func release(owner any, n int) {
	if n == 0 {
		return
	}
	retiredMu.Lock()
	if retired[owner] -= n; retired[owner] <= 0 {
		delete(retired, owner)
	}
	retiredMu.Unlock()
}

func closeWhenIdle(owner any, entries []*reuseEntry, step time.Duration, closed <-chan struct{}) {
	tick := time.NewTicker(step)
	defer tick.Stop()
	for {
		kept := entries[:0]
		for _, entry := range entries {
			if entry.isClosed() {
				continue
			}
			if entry.openUsage.Load() == 0 {
				entry.close()
				continue
			}
			kept = append(kept, entry)
		}
		release(owner, len(entries)-len(kept))
		if entries = kept; len(entries) == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-closed:
			for _, entry := range entries {
				entry.close()
			}
			release(owner, len(entries))
			return
		}
	}
}

// Retire has the client's next sessions open over new connections (see
// ReuseManager.Retire). The upload and download connections count together
// and are set aside together or not at all. A client without a pool keeps
// none.
func (c *Client) Retire() bool {
	return retire(c, c.ctx.Done(), c.uploadManager, c.downloadManager)
}
