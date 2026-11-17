package gun

import "time"

// Retire has the next streams open over new connections and reports whether
// there were connections to set aside. Those are left alone: their streams go
// on for as long as they like, and each connection closes when its last
// stream ends. A connection that stalled is not asked again, one that only
// lagged loses nothing.
func (c *Client) Retire() bool {
	c.mutex.Lock()
	old := c.transports
	c.transports = nil
	c.mutex.Unlock()
	for _, t := range old {
		go t.closeWhenIdle(time.Second)
	}
	return len(old) > 0
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
