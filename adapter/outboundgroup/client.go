package outboundgroup

import C "github.com/metacubex/mihomo/constant"

// What the Android client reads from a group without asking for its current
// node: for url-test that makes (and caches for ten seconds) a new choice, and
// fallback drops a pinned node that is down.

// CheckOptions is a group's health check URL and expected status as the
// subscription gave them.
type CheckOptions interface {
	TestOptions() (url string, expectedStatus string)
}

// Pinnable is a group that picks its node itself and can be pinned to one by
// hand: Pinned is the pinned node, "" when the group picks.
type Pinnable interface {
	Pinned() string
	CurrentNode() C.Proxy
}

var (
	_ CheckOptions = (*Fallback)(nil)
	_ CheckOptions = (*LoadBalance)(nil)
	_ CheckOptions = (*URLTest)(nil)
	_ CheckOptions = (*Selector)(nil)
	_ Pinnable     = (*Fallback)(nil)
	_ Pinnable     = (*URLTest)(nil)
)

func (f *Fallback) TestOptions() (string, string)     { return f.testUrl, f.expectedStatus }
func (lb *LoadBalance) TestOptions() (string, string) { return lb.testUrl, lb.expectedStatus }
func (u *URLTest) TestOptions() (string, string)      { return u.testUrl, u.expectedStatus }

// TestOptions leaves the URL empty when it is the default, as MarshalJSON does:
// the client then takes the URL of the group's providers.
func (s *Selector) TestOptions() (string, string) {
	if s.testUrl == C.DefaultTestURL {
		return "", s.expectedStatus
	}
	return s.testUrl, s.expectedStatus
}

func (f *Fallback) Pinned() string { return f.selected.Load() }
func (u *URLTest) Pinned() string  { return u.selected.Load() }

// ResetChoice drops the cached choice, so that probes recorded outside the
// group's own check (the client's) decide the next dial, not a choice made
// before them.
func (u *URLTest) ResetChoice() {
	u.fastSingle.Reset()
}
