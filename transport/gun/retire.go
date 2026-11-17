package gun

import (
	"sync"
	"time"
)

// retireLimit is how many connections set aside a client keeps open at most.
// A node slow under load would have one set aside on every check, and long
// streams keep each of them open: past the limit nothing more is set aside.
const retireLimit = 2

// retireStep is how often a connection set aside is looked at.
var retireStep = time.Second

var (
	retiredMu sync.Mutex
	// retired counts the connections each client has set aside and not yet
	// closed. A client is dropped from it at zero, so it does not keep the
	// client alive.
	retired = map[*Client]int{}
)

// Retire has the next streams open over new connections and reports whether
// there were connections to set aside. Those are left alone: their streams go
// on for as long as they like, and each connection closes when its last
// stream ends. A connection that stalled is not asked again, one that only
// lagged loses nothing. With nothing set aside and still open, the current
// connections are always set aside, however many they are: a pool as large as
// the server asks for is set aside whole once. Past that, nothing more is set
// aside and the streams go on as they did while the connections set aside and
// still open, with the current ones that carry streams, would go past
// retireLimit. Those without a stream do not count: they are set aside with
// the rest and close at once.
func (c *Client) Retire() bool {
	retiredMu.Lock()
	c.mutex.Lock()
	old := c.transports
	busy := 0
	for _, t := range old {
		if t.count.Load() > 0 {
			busy++
		}
	}
	if len(old) == 0 || (retired[c] > 0 && retired[c]+busy > retireLimit) {
		c.mutex.Unlock()
		retiredMu.Unlock()
		return false
	}
	c.transports = nil
	c.mutex.Unlock()
	retired[c] += len(old)
	retiredMu.Unlock()
	step := retireStep
	for _, t := range old {
		go func(t *Transport) {
			t.closeWhenIdle(step)
			retiredMu.Lock()
			if retired[c]--; retired[c] <= 0 {
				delete(retired, c)
			}
			retiredMu.Unlock()
		}(t)
	}
	return true
}

// closeWhenIdle looks at the streams once a step and closes the connection
// that had none twice in a row: a stream that had taken the connection and
// was yet to open on it is counted by then.
func (t *Transport) closeWhenIdle(step time.Duration) {
	tick := time.NewTicker(step)
	defer tick.Stop()
	for idle := 0; idle < 2; {
		select {
		case <-tick.C:
		case <-t.ctx.Done():
			return
		}
		if t.count.Load() > 0 {
			idle = 0
		} else {
			idle++
		}
	}
	_ = t.Close()
}
