package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type FallbackOption struct{}

type Fallback struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	selected       atomic.TypedValue[string]
	expectedStatus string
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := f.findAliveProxy(true)
	c, err := proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailed(proxy.Type(), err, f.healthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				f.onDialSuccess()
				f.onDeadNodeDialed(proxy, f.testUrl, f.healthCheck)
			} else {
				f.onDialFailed(proxy.Type(), err, f.healthCheck)
			}
		})
	} else if err == nil {
		f.onDeadNodeDialed(proxy, f.testUrl, f.healthCheck)
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := f.findAliveProxy(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(f)
	}

	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (f *Fallback) SupportUDP() bool {
	if f.disableUDP {
		return false
	}

	proxy := f.findAliveProxy(false)
	return proxy.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fallback) IsL3Protocol(metadata *C.Metadata) bool {
	return f.findAliveProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
// MarshalJSON describes the group as it is: reading it neither chooses a node
// nor drops a selection whose node is down, as a dial would.
func (f *Fallback) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range f.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":           f.Type().String(),
		"now":            f.CurrentNode().Name(),
		"all":            all,
		"testUrl":        f.testUrl,
		"expectedStatus": f.expectedStatus,
		"fixed":          f.selected.Load(),
		"hidden":         f.Hidden(),
		"icon":           f.Icon(),
		"emptyFallback":  f.EmptyFallback().Name(),
	})
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.findAliveProxy(touch)
	return proxy
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	selected := f.selected.Load()
	proxy, selectedDown := aliveProxy(f.GetProxies(touch), selected, f.testUrl)
	if selectedDown {
		// Only the selection that was passed over: one set meanwhile stays.
		f.selected.CompareAndSwap(selected, "")
	}
	return proxy
}

// CurrentNode is the node the group would dial now. Unlike a dial, it does
// not drop a selection whose node is down.
func (f *Fallback) CurrentNode() C.Proxy {
	proxy, _ := aliveProxy(f.GetProxies(false), f.selected.Load(), f.testUrl)
	return proxy
}

// aliveProxy is the selected node if it is alive, else the first alive node
// after it; the first node when none is alive. selectedDown reports that the
// selection was passed over.
func aliveProxy(proxies []C.Proxy, selected, testUrl string) (proxy C.Proxy, selectedDown bool) {
	for _, proxy := range proxies {
		if selected != "" {
			if proxy.Name() != selected {
				continue
			}
			if proxy.AliveForTestUrl(testUrl) {
				return proxy, false
			}
			selected, selectedDown = "", true
			continue
		}
		if proxy.AliveForTestUrl(testUrl) {
			return proxy, selectedDown
		}
	}
	if selected != "" {
		// The selected node is not in the list: a provider update dropped it,
		// or the selection came back from the cache for a node that is gone.
		// The group works as if nothing were selected and keeps the selection
		// for the node's return.
		proxy, _ := aliveProxy(proxies, "", testUrl)
		return proxy, false
	}

	return proxies[0], selectedDown
}

func (f *Fallback) Set(name string) error {
	var p C.Proxy
	for _, proxy := range f.GetProxies(false) {
		if proxy.Name() == name && proxy != hiddenReject {
			p = proxy
			break
		}
	}

	if p == nil {
		if f.hiddenMember(name) {
			return ErrHidden
		}
		return errors.New("proxy not exist")
	}

	f.selected.Store(name)
	if !p.AliveForTestUrl(f.testUrl) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
		defer cancel()
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
		_, _ = p.URLTest(ctx, f.testUrl, expectedStatus)
	}

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.selected.Store(name)
}

func (f *Fallback) Providers() []P.ProxyProvider {
	return f.providers
}

func (f *Fallback) Proxies() []C.Proxy {
	return f.GetProxies(false)
}

func NewFallback(option GroupCommonOption, fallbackOption FallbackOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Fallback, error) {
	return &Fallback{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Fallback,
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
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}, nil
}
