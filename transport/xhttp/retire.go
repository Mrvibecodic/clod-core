package xhttp

import "time"

// Retire has the next sessions open over new connections and reports whether
// there were connections to set aside. Those are left alone: their sessions
// go on for as long as they like, and each connection closes when its last
// session ends. A connection that stalled is not asked again, one that only
// lagged loses nothing.
//
// closed ends the wait: the connections set aside close with their client.
func (m *ReuseManager) Retire(closed <-chan struct{}) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	old := m.entries
	m.entries = make([]*reuseEntry, 0)
	m.mu.Unlock()
	if len(old) == 0 {
		return false
	}
	go closeWhenIdle(old, time.Second, closed)
	return true
}

func closeWhenIdle(entries []*reuseEntry, step time.Duration, closed <-chan struct{}) {
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
		if entries = kept; len(entries) == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-closed:
			for _, entry := range entries {
				entry.close()
			}
			return
		}
	}
}

// Retire has the client's next sessions open over new connections (see
// ReuseManager.Retire). A client without a pool keeps none.
func (c *Client) Retire() bool {
	up, down := c.uploadManager.Retire(c.ctx.Done()), c.downloadManager.Retire(c.ctx.Done())
	return up || down
}
