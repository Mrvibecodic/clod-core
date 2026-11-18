package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	stdatomic "sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type URLTestOption struct {
	Tolerance uint16 `group:"tolerance,omitempty"`
}

type URLTest struct {
	*GroupBase
	selected       atomic.TypedValue[string]
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	fastNode       C.Proxy
	fastSingle     *singledo.Single[C.Proxy]
	// fastMu serialises the choice: a Reset of fastSingle while a choice is
	// being made lets a second one start alongside the first.
	fastMu sync.Mutex
	// hiddenGeneration — набор скрытых узлов, при котором сделан выбор:
	// сменился — выбор делается заново, а не берётся из кеша.
	hiddenGeneration stdatomic.Uint64
	// held is the node a check asked for by a doubt of it left alive, and
	// when; nil when its delay has been measured since.
	held atomic.TypedValue[*hold]
}

// hold is a node kept whatever delays a check has left, and when it was.
type hold struct {
	node C.Proxy
	at   time.Time
}

func (u *URLTest) Now() string {
	return u.fast(false).Name()
}

// CurrentNode is the node the group is using, without making a new choice:
// a choice is cached for ten seconds, and one made at the start of a check
// would hide that check's own results from the first dial after it.
func (u *URLTest) CurrentNode() C.Proxy {
	u.fastMu.Lock()
	defer u.fastMu.Unlock()
	if u.fastNode != nil && hidden.Hides(u.fastNode) {
		return nil
	}
	return u.fastNode
}

func (u *URLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name && proxy != hiddenReject {
			p = proxy
			break
		}
	}
	if p == nil {
		if u.hiddenMember(name) {
			return ErrHidden
		}
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *URLTest) ForceSet(name string) {
	u.selected.Store(name)
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *URLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	// Before the node is taken: a connection that took its node before the
	// group went to another is the older of the two.
	born := time.Now()
	proxy := u.fast(true)
	// A node marked dead while the connection was being opened says nothing
	// against the mark: only one known dead when the group took it does, and
	// only by an answer through it.
	dead := !proxy.AliveForTestUrl(u.testUrl)
	c, err = proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.checkAfter(proxy, born))
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				u.onDialSuccess()
			} else {
				u.onDialFailed(proxy.Type(), err, u.checkAfter(proxy, born))
			}
		})
	}
	if err == nil {
		var answered func()
		if dead {
			answered = func() { u.onDeadNodeDialed(proxy, u.testUrl, u.healthCheck) }
		}
		c = u.watch(c, proxy, born, u.doubt(proxy), answered)
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	born := time.Now()
	proxy := u.fast(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.checkAfter(proxy, born))
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (u *URLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

// holdSpeed keeps the group on p whatever delays the check just made has
// left: a check asked for by failed or unanswered connections measures the
// node when it is at its worst, it tells alive from dead and no more.
func (u *URLTest) holdSpeed(p C.Proxy) {
	u.held.Store(&hold{node: p, at: time.Now()})
	u.fastSingle.Reset()
}

// speedHeld reports whether the delay of the group's node is the one such a
// check left. The next probe of the node ends it; a hold of another node
// keeps nothing.
func (u *URLTest) speedHeld() bool {
	held := u.held.Load()
	if held == nil || held.node != u.fastNode {
		return false
	}
	if lastProbe(u.fastNode, u.testUrl).After(held.at) {
		// A hold set meanwhile is another one and stays.
		u.held.CompareAndSwap(held, nil)
		return false
	}
	return true
}

// release gives p, a node that answers again, back to the choice. The hold
// of another node stays: it is ended by that node's own probe.
func (u *URLTest) release(p C.Proxy) {
	if held := u.held.Load(); held != nil && held.node == p {
		u.held.CompareAndSwap(held, nil)
	}
	u.fastSingle.Reset()
}

// chooseAgain gives p, a node that answers again, back to the choice and
// makes the choice now: a choice is kept for ten seconds, and the group would
// show and use the node it is on until the next connection after that. The
// hold of another node stays, as release leaves it.
func (u *URLTest) chooseAgain(p C.Proxy) {
	u.release(p)
	u.fast(false)
}

// doubt is how the group asks about p.
func (u *URLTest) doubt(p C.Proxy) doubt {
	return doubt{url: u.testUrl, expected: u.expectedStatus, next: u.nextNodes, all: u.healthCheck,
		hold: func() { u.holdSpeed(p) }, again: func() { u.chooseAgain(p) }}
}

// checkAfter is the check the failed connections through p ask for.
func (u *URLTest) checkAfter(p C.Proxy, born time.Time) func() {
	return func() { u.failedOf(p, born, u.doubt(p)) }
}

// nextNodes is the alive nodes of the group without the given one, the best
// first: where the group would go from it.
func (u *URLTest) nextNodes(but C.Proxy) []C.Proxy {
	nodes := aliveBut(u.GetProxies(false), but, u.testUrl)
	sort.SliceStable(nodes, func(i, j int) bool { return u.score(nodes[i]) < u.score(nodes[j]) })
	return nodes
}

func (u *URLTest) healthCheck() {
	u.fastSingle.Reset()
	u.GroupBase.healthCheck()
	u.fastSingle.Reset()
}

// flakyWindow is how many latest probes count when a node is ranked: every
// failure among them adds flakyPenalty to its delay. A node that drops probes
// but passes the second one stays alive, yet loses to a steady node unless
// that one is slower by more than the penalty.
const flakyWindow = 3

// flakyPenalty is a quarter of the probe timeout per recent failure.
func (u *URLTest) flakyPenalty() uint32 {
	return uint32(u.testTimeout) / 4
}

// score ranks p for url-test: its last delay plus the penalty for recent
// failures; 0xffff for a dead node.
func (u *URLTest) score(p C.Proxy) uint32 {
	delay := uint32(p.LastDelayForTestUrl(u.testUrl))
	if delay >= 0xffff {
		return delay
	}
	history := probeHistory(p, u.testUrl)
	var failures uint32
	for i := len(history) - 1; i >= 0 && i >= len(history)-flakyWindow; i-- {
		if history[i].Delay == 0 {
			failures++
		}
	}
	score := delay + failures*u.flakyPenalty()
	if score > 0xfffe {
		score = 0xfffe
	}
	return score
}

func (u *URLTest) fast(touch bool) C.Proxy {
	if generation := hidden.Generation(); u.hiddenGeneration.Load() != generation {
		u.hiddenGeneration.Store(generation)
		u.fastSingle.Reset()
	}
	elm, shared := u.pick(touch)
	// The choice is kept for ten seconds: a node marked dead meanwhile is
	// left at once, not when the kept choice runs out.
	if elm != nil && !elm.AliveForTestUrl(u.testUrl) && anyAlive(u.GetProxies(false), u.testUrl) {
		u.fastSingle.Reset()
		elm, shared = u.pick(touch)
	}
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		u.Touch()
	}

	return elm
}

// anyAlive reports whether the list has an alive node.
func anyAlive(proxies []C.Proxy, url string) bool {
	for _, proxy := range proxies {
		if proxy.AliveForTestUrl(url) {
			return true
		}
	}
	return false
}

func (u *URLTest) pick(touch bool) (C.Proxy, bool) {
	// left is the dead node the choice moved off, if it did.
	var left C.Proxy
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		u.fastMu.Lock()
		defer u.fastMu.Unlock()
		proxies := u.GetProxies(touch)
		if selected := u.selected.Load(); selected != "" {
			for _, proxy := range proxies {
				if !proxy.AliveForTestUrl(u.testUrl) {
					continue
				}
				if proxy.Name() == selected {
					u.fastNode = proxy
					return proxy, nil
				}
			}
		}

		// Find the current node in the whole list, the first place included:
		// the same object if it is still there, otherwise the object with its
		// name from the same provider — a provider update replaced it and
		// handed it the old object's checks. A namesake from another provider
		// is another server: if the node is gone, the choice is made afresh.
		var current C.Proxy
		if u.fastNode != nil {
			for _, proxy := range proxies {
				if proxy == u.fastNode {
					current = proxy
					break
				}
			}
			if current == nil {
				provider := u.fastNode.ProxyInfo().ProviderName
				for _, proxy := range proxies {
					if proxy.Name() == u.fastNode.Name() && proxy.ProxyInfo().ProviderName == provider {
						current = proxy
						break
					}
				}
			}
		}
		fastNotExist := current == nil
		if current != nil {
			u.fastNode = current
		}

		// A node on probation is no candidate unless the group is on it
		// already or no other node is alive.
		probation := u.onProbation()
		if probation == u.fastNode || len(aliveBut(proxies, probation, u.testUrl)) == 0 {
			probation = nil
		}
		fast := proxies[0]
		if fast == probation {
			fast = proxies[1]
		}
		minScore := u.score(fast)
		for _, proxy := range proxies {
			if proxy == probation || !proxy.AliveForTestUrl(u.testUrl) {
				continue
			}

			score := u.score(proxy)
			if score < minScore {
				fast = proxy
				minScore = score
			}

		}
		if u.fastNode == nil || fastNotExist {
			u.fastNode = fast
		} else if !u.fastNode.AliveForTestUrl(u.testUrl) {
			// With every node dead there is nowhere to go: the group stays
			// where it is rather than move to the first of the list.
			if fast.AliveForTestUrl(u.testUrl) {
				left, u.fastNode = u.fastNode, fast
			}
		} else if u.score(u.fastNode) > minScore+uint32(u.tolerance) && !u.speedHeld() {
			u.fastNode = fast
		}
		return u.fastNode, nil
	})
	// Whoever found the node dead, the group has left it: it is probed again
	// ahead of the scheduled check.
	if left != nil {
		u.leftDead(left, u.doubt(left))
	}
	return elm, shared
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTest) SupportUDP() bool {
	if u.disableUDP {
		return false
	}
	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
// MarshalJSON describes the group as it is: reading it does not make a new
// choice, as a dial would. Before the first dial there is no node yet, and
// the one the group would pick is shown.
func (u *URLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	now := u.Now
	if current := u.CurrentNode(); current != nil {
		now = current.Name
	}
	return json.Marshal(map[string]any{
		"type":           u.Type().String(),
		"now":            now(),
		"all":            all,
		"testUrl":        u.testUrl,
		"expectedStatus": u.expectedStatus,
		"fixed":          u.selected.Load(),
		"hidden":         u.Hidden(),
		"icon":           u.Icon(),
		"emptyFallback":  u.EmptyFallback().Name(),
	})
}

func (u *URLTest) Providers() []P.ProxyProvider {
	return u.providers
}

func (u *URLTest) Proxies() []C.Proxy {
	return u.GetProxies(false)
}

func (u *URLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	dm, err := u.GroupBase.URLTest(ctx, u.testUrl, expectedStatus)
	// A choice cached while the test ran was made from the old delays.
	u.fastSingle.Reset()
	return dm, err
}

func NewURLTest(option GroupCommonOption, urlTestOption URLTestOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*URLTest, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	urlTest := &URLTest{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.URLTest,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		tolerance:      urlTestOption.Tolerance,
	}

	return urlTest, nil
}
