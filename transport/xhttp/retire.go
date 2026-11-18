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
	// retired holds the connections each client (or each pool retired on its
	// own) has set aside and not yet closed: their number is what counts
	// toward retireLimit. One is dropped from it at zero, so it does not keep
	// the client alive.
	retired = map[any]map[*reuseEntry]struct{}{}
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
	if n := len(retired[owner]); len(old) == 0 || (n > 0 && n+busy > retireLimit) {
		return false
	}
	for _, m := range pools {
		if m != nil {
			m.entries = make([]*reuseEntry, 0)
		}
	}
	set := retired[owner]
	if set == nil {
		set = map[*reuseEntry]struct{}{}
		retired[owner] = set
	}
	for _, entry := range old {
		set[entry] = struct{}{}
	}
	go closeWhenIdle(owner, old, retireStep, closed)
	return true
}

// release takes connections set aside off the count once they are closed.
// One CloseRetired took off already is not there any more.
func release(owner any, entries []*reuseEntry) {
	if len(entries) == 0 {
		return
	}
	retiredMu.Lock()
	if set := retired[owner]; set != nil {
		for _, entry := range entries {
			delete(set, entry)
		}
		if len(set) == 0 {
			delete(retired, owner)
		}
	}
	retiredMu.Unlock()
}

// closeRetired closes the connections set aside under key, whatever sessions
// they still carry, and leaves none counted.
func closeRetired(key any) {
	retiredMu.Lock()
	set := retired[key]
	delete(retired, key)
	retiredMu.Unlock()
	for entry := range set {
		entry.close()
	}
}

func closeWhenIdle(owner any, entries []*reuseEntry, step time.Duration, closed <-chan struct{}) {
	tick := time.NewTicker(step)
	defer tick.Stop()
	for {
		var gone, kept []*reuseEntry
		for _, entry := range entries {
			if entry.isClosed() {
				gone = append(gone, entry)
				continue
			}
			if entry.openUsage.Load() == 0 {
				entry.close()
				gone = append(gone, entry)
				continue
			}
			kept = append(kept, entry)
		}
		release(owner, gone)
		if entries = kept; len(entries) == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-closed:
			for _, entry := range entries {
				entry.close()
			}
			release(owner, entries)
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

// CloseRetired closes the connections the client has set aside, whatever
// sessions they still carry, and leaves none counted. Closing the client does
// it as well; this is for when the client goes on.
func (c *Client) CloseRetired() {
	closeRetired(c)
}

// Pooled reports whether the client keeps a pool of connections for its
// sessions.
func (c *Client) Pooled() bool {
	return c.uploadManager != nil || c.downloadManager != nil
}
