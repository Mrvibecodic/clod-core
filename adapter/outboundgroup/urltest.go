package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type URLTestOption struct {
	Tolerance uint16 `group:"tolerance,omitempty"`
}

type URLTest struct {
	*GroupBase
	selected       string
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	fastNode       C.Proxy
	fastSingle     *singledo.Single[C.Proxy]
	// carried is the object that replaced the current node on a provider
	// update, and carriedScore the old object's score: the new object has no
	// checks yet and competes with the old one's result until its first one.
	carried      C.Proxy
	carriedScore uint32
}

func (u *URLTest) Now() string {
	return u.fast(false).Name()
}

func (u *URLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *URLTest) ForceSet(name string) {
	u.selected = name
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *URLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := u.fast(true)
	c, err = proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.healthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				u.onDialSuccess()
			} else {
				u.onDialFailed(proxy.Type(), err, u.healthCheck)
			}
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := u.fast(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.healthCheck)
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (u *URLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
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
	var history []C.DelayHistory
	if h, ok := p.(interface {
		DelayHistoryForTestUrl(url string) []C.DelayHistory
	}); ok {
		history = h.DelayHistoryForTestUrl(u.testUrl)
	} else {
		history = p.ExtraDelayHistories()[u.testUrl].History
	}
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
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		proxies := u.GetProxies(touch)
		if u.selected != "" {
			for _, proxy := range proxies {
				if !proxy.AliveForTestUrl(u.testUrl) {
					continue
				}
				if proxy.Name() == u.selected {
					u.fastNode = proxy
					return proxy, nil
				}
			}
		}

		fast := proxies[0]
		minScore := u.score(fast)
		fastNotExist := true

		// The whole list is scanned, the first node included: a current node
		// standing first must not count as missing and lose the tolerance.
		for _, proxy := range proxies {
			if u.fastNode != nil && proxy.Name() == u.fastNode.Name() {
				// A provider update replaces the node objects under the same
				// names; follow the current object, the old one gets no checks.
				if proxy != u.fastNode && u.fastNode.LastDelayForTestUrl(u.testUrl) != 0xffff {
					u.carried, u.carriedScore = proxy, u.score(u.fastNode)
				}
				u.fastNode = proxy
				fastNotExist = false
			}

			if !proxy.AliveForTestUrl(u.testUrl) {
				continue
			}

			score := u.score(proxy)
			if score < minScore {
				fast = proxy
				minScore = score
			}

		}
		if u.fastNode == nil || fastNotExist || !u.fastNode.AliveForTestUrl(u.testUrl) {
			u.fastNode = fast
		} else {
			current := u.score(u.fastNode)
			if u.fastNode == u.carried && u.fastNode.LastDelayForTestUrl(u.testUrl) == 0xffff {
				current = u.carriedScore
			} else {
				u.carried = nil
			}
			if current > minScore+uint32(u.tolerance) {
				u.fastNode = fast
			}
		}
		return u.fastNode, nil
	})
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		u.Touch()
	}

	return elm
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
func (u *URLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":           u.Type().String(),
		"now":            u.Now(),
		"all":            all,
		"testUrl":        u.testUrl,
		"expectedStatus": u.expectedStatus,
		"fixed":          u.selected,
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
