package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/atomic"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"github.com/dlclark/regexp2"
	"golang.org/x/exp/slices"
)

type GroupBase struct {
	*outbound.Base
	hidden            bool
	icon              string
	filterRegs        []*regexp2.Regexp
	excludeFilterRegs []*regexp2.Regexp
	excludeTypeArray  []string
	providers         []P.ProxyProvider
	failedTestMux     sync.Mutex
	failedTimes       int
	failedTime        time.Time
	failedTesting     atomic.Bool
	testTimeout       int
	maxFailedTimes    int
	emptyFallback     C.Proxy
	// reviveNext is when a dial through a node marked dead may start the next
	// check, reviveWait the pause after it; see onDeadNodeDialed.
	reviveMux  sync.Mutex
	reviveNext time.Time
	reviveWait time.Duration

	// for GetProxies
	getProxiesMutex  sync.Mutex
	providerVersions []uint32
	providerProxies  []C.Proxy
	allProxies       []C.Proxy
	hiddenGeneration uint64
}

// hiddenReject — член группы, у которой скрыты все узлы (component/hidden):
// такая группа отказывает, а не выпускает напрямую, как EmptyFallback. Его
// нельзя выбрать: выбор пользователя остаётся за ним.
var hiddenReject C.Proxy = adapter.NewProxy(outbound.NewReject())

// ErrHidden — узел группы сейчас скрыт: выбрать его нельзя.
var ErrHidden = errors.New("proxy hidden")

// AllHidden — все узлы группы скрыты: её список — один hiddenReject.
func AllHidden(proxies []C.Proxy) bool {
	return len(proxies) == 1 && proxies[0] == hiddenReject
}

// isServer — узел из конфига или провайдера, а не группа и не встроенный.
func isServer(p C.Proxy) bool {
	s, ok := p.(interface{ Server() (string, bool) })
	if !ok {
		return false
	}
	_, ok = s.Server()
	return ok
}

// groupAllHidden — p — группа, у которой скрыты все узлы: через неё отказ.
func groupAllHidden(p C.Proxy) bool {
	g, ok := p.Adapter().(ProxyGroup)
	return ok && AllHidden(g.Proxies())
}

// visibleFallback — куда идёт группа, когда её выбор скрыт: первый видимый
// сервер, иначе первая группа, у которой видно хоть что-то; встроенные
// (DIRECT и др.) не годятся — скрытый выбор не выпускает напрямую. Ничего —
// отказ.
func visibleFallback(proxies []C.Proxy) C.Proxy {
	for _, p := range proxies {
		if isServer(p) {
			return p
		}
	}
	for _, p := range proxies {
		if _, ok := p.Adapter().(ProxyGroup); ok && !groupAllHidden(p) {
			return p
		}
	}
	return hiddenReject
}

type GroupBaseOption struct {
	Name           string
	Type           C.AdapterType
	Hidden         bool
	Icon           string
	Filter         string
	ExcludeFilter  string
	ExcludeType    string
	TestTimeout    int
	MaxFailedTimes int
	EmptyFallback  C.Proxy
	Providers      []P.ProxyProvider
}

func NewGroupBase(opt GroupBaseOption) *GroupBase {
	var excludeTypeArray []string
	if opt.ExcludeType != "" {
		excludeTypeArray = strings.Split(opt.ExcludeType, "|")
	}

	var excludeFilterRegs []*regexp2.Regexp
	if opt.ExcludeFilter != "" {
		for _, excludeFilter := range strings.Split(opt.ExcludeFilter, "`") {
			excludeFilterReg := regexp2.MustCompile(excludeFilter, regexp2.None)
			excludeFilterRegs = append(excludeFilterRegs, excludeFilterReg)
		}
	}

	var filterRegs []*regexp2.Regexp
	if opt.Filter != "" {
		for _, filter := range strings.Split(opt.Filter, "`") {
			filterReg := regexp2.MustCompile(filter, regexp2.None)
			filterRegs = append(filterRegs, filterReg)
		}
	}

	gb := &GroupBase{
		Base:              outbound.NewBase(outbound.BaseOption{Name: opt.Name, Type: opt.Type}),
		hidden:            opt.Hidden,
		icon:              opt.Icon,
		filterRegs:        filterRegs,
		excludeFilterRegs: excludeFilterRegs,
		excludeTypeArray:  excludeTypeArray,
		providers:         opt.Providers,
		failedTesting:     atomic.NewBool(false),
		testTimeout:       opt.TestTimeout,
		maxFailedTimes:    opt.MaxFailedTimes,
		emptyFallback:     opt.EmptyFallback,
	}

	if gb.testTimeout == 0 {
		gb.testTimeout = 5000
	}
	if gb.maxFailedTimes == 0 {
		gb.maxFailedTimes = 5
	}

	return gb
}

func (gb *GroupBase) Hidden() bool {
	return gb.hidden
}

func (gb *GroupBase) Icon() string {
	return gb.icon
}

func (gb *GroupBase) EmptyFallback() C.Proxy {
	return gb.emptyFallback
}

// hiddenMember — узел группы, сейчас скрытый (component/hidden).
func (gb *GroupBase) hiddenMember(name string) bool {
	if !hidden.Active() {
		return false
	}
	_, all := gb.members(false)
	for _, p := range all {
		if p.Name() == name && hidden.Hides(p) {
			return true
		}
	}

	return false
}

func (gb *GroupBase) Touch() {
	for _, pd := range gb.providers {
		pd.Touch()
	}
}

func (gb *GroupBase) GetProxies(touch bool) []C.Proxy {
	visible, _ := gb.members(touch)
	return visible
}

// members — члены группы без скрытых (как GetProxies) и все, как в конфиге.
func (gb *GroupBase) members(touch bool) (visible, all []C.Proxy) {
	providerVersions := make([]uint32, len(gb.providers))
	for i, pd := range gb.providers {
		if touch { // touch first
			pd.Touch()
		}
		providerVersions[i] = pd.Version()
	}
	hiddenGeneration := hidden.Generation()

	// thread safe
	gb.getProxiesMutex.Lock()
	defer gb.getProxiesMutex.Unlock()

	// return the cached proxies if version not changed
	if slices.Equal(providerVersions, gb.providerVersions) && hiddenGeneration == gb.hiddenGeneration {
		return gb.providerProxies, gb.allProxies
	}

	var proxies []C.Proxy
	if len(gb.filterRegs) == 0 {
		for _, pd := range gb.providers {
			proxies = append(proxies, pd.Proxies()...)
		}
	} else {
		for _, pd := range gb.providers {
			if pd.VehicleType() == P.Compatible { // compatible provider unneeded filter
				proxies = append(proxies, pd.Proxies()...)
				continue
			}

			var newProxies []C.Proxy
			proxiesSet := map[string]struct{}{}
			for _, filterReg := range gb.filterRegs {
				for _, p := range pd.Proxies() {
					name := p.Name()
					if mat, _ := filterReg.MatchString(name); mat {
						if _, ok := proxiesSet[name]; !ok {
							proxiesSet[name] = struct{}{}
							newProxies = append(newProxies, p)
						}
					}
				}
			}
			proxies = append(proxies, newProxies...)
		}
	}

	// Multiple filers means that proxies are sorted in the order in which the filers appear.
	// Although the filter has been performed once in the previous process,
	// when there are multiple providers, the array needs to be reordered as a whole.
	if len(gb.providers) > 1 && len(gb.filterRegs) > 1 {
		var newProxies []C.Proxy
		proxiesSet := map[string]struct{}{}
		for _, filterReg := range gb.filterRegs {
			for _, p := range proxies {
				name := p.Name()
				if mat, _ := filterReg.MatchString(name); mat {
					if _, ok := proxiesSet[name]; !ok {
						proxiesSet[name] = struct{}{}
						newProxies = append(newProxies, p)
					}
				}
			}
		}
		for _, p := range proxies { // add not matched proxies at the end
			name := p.Name()
			if _, ok := proxiesSet[name]; !ok {
				proxiesSet[name] = struct{}{}
				newProxies = append(newProxies, p)
			}
		}
		proxies = newProxies
	}

	if len(gb.excludeFilterRegs) > 0 {
		var newProxies []C.Proxy
	LOOP1:
		for _, p := range proxies {
			name := p.Name()
			for _, excludeFilterReg := range gb.excludeFilterRegs {
				if mat, _ := excludeFilterReg.MatchString(name); mat {
					continue LOOP1
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if gb.excludeTypeArray != nil {
		var newProxies []C.Proxy
	LOOP2:
		for _, p := range proxies {
			mType := p.Type().String()
			for _, excludeType := range gb.excludeTypeArray {
				if strings.EqualFold(mType, excludeType) {
					continue LOOP2
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if len(proxies) == 0 {
		empty := []C.Proxy{gb.EmptyFallback()}
		return empty, empty
	}

	visible = proxies
	if hidden.Active() {
		visible = make([]C.Proxy, 0, len(proxies))
		for _, p := range proxies {
			if !hidden.Hides(p) {
				visible = append(visible, p)
			}
		}
		if len(visible) == 0 {
			visible = append(visible, hiddenReject)
		}
	}

	// only cache when proxies not empty
	gb.providerVersions = providerVersions
	gb.providerProxies = visible
	gb.allProxies = proxies
	gb.hiddenGeneration = hiddenGeneration

	return visible, proxies
}

func (gb *GroupBase) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	var wg sync.WaitGroup
	var lock sync.Mutex
	mp := map[string]uint16{}
	proxies := gb.GetProxies(false)
	for _, proxy := range proxies {
		proxy := proxy
		wg.Add(1)
		go func() {
			delay, err := proxy.URLTest(ctx, url, expectedStatus)
			if err == nil {
				lock.Lock()
				mp[proxy.Name()] = delay
				lock.Unlock()
			}

			wg.Done()
		}()
	}
	wg.Wait()

	if len(mp) == 0 {
		return mp, fmt.Errorf("get delay: all proxies timeout")
	} else {
		return mp, nil
	}
}

func (gb *GroupBase) onDialFailed(adapterType C.AdapterType, err error, fn func()) {
	if adapterType == C.Direct || adapterType == C.Compatible || adapterType == C.Reject || adapterType == C.Pass || adapterType == C.RejectDrop {
		return
	}

	if C.ProbeHolding(time.Now()) {
		return
	}

	if errors.Is(err, C.ErrNotSupport) {
		return
	}

	go func() {
		if N.IsConnRefused(err) {
			fn()
			return
		}

		// The group check runs outside the lock: held through it, the lock
		// queued every failure of the check's seconds, and they re-triggered
		// a second full check as soon as the first one ended.
		var trigger bool
		gb.failedTestMux.Lock()
		gb.failedTimes++
		if gb.failedTimes == 1 {
			log.Debugln("ProxyGroup: %s first failed", gb.Name())
			gb.failedTime = time.Now()
		} else {
			if time.Since(gb.failedTime) > time.Duration(gb.testTimeout)*time.Millisecond {
				gb.failedTimes = 0
				gb.failedTestMux.Unlock()
				return
			}

			log.Debugln("ProxyGroup: %s failed count: %d", gb.Name(), gb.failedTimes)
			// While a check runs, the failures it absorbs start nothing: the
			// counter is cleared when it ends.
			if gb.failedTimes >= gb.maxFailedTimes && !gb.failedTesting.Load() {
				log.Warnln("because %s failed multiple times, activate health check", gb.Name())
				trigger = true
			}
		}
		gb.failedTestMux.Unlock()

		if trigger {
			fn()
		}
	}()
}

func (gb *GroupBase) healthCheck() {
	// Only one caller runs the check even when several failures trip it at
	// the same moment.
	if !gb.failedTesting.CompareAndSwap(false, true) {
		return
	}

	wg := sync.WaitGroup{}
	for _, proxyProvider := range gb.providers {
		wg.Add(1)
		proxyProvider := proxyProvider
		go func() {
			defer wg.Done()
			proxyProvider.HealthCheck()
		}()
	}

	wg.Wait()
	// The counter is cleared before the check is marked finished: a failure
	// landing in between must not see the count left from before the check.
	gb.failedTestMux.Lock()
	gb.failedTimes = 0
	gb.failedTestMux.Unlock()
	gb.failedTesting.Store(false)
}

const (
	reviveFirstWait = 30 * time.Second
	reviveMaxWait   = 10 * time.Minute
)

// onDeadNodeDialed is told of a connection through p that worked. A group
// dials a node marked dead only when none of its nodes is alive, and a check
// that ran while the network was gone leaves them so until the next interval:
// a connection that works shows the network is back, and the group is
// checked at once. The next such check waits reviveFirstWait; a check that
// leaves p dead doubles the wait, up to reviveMaxWait, so that a node that
// carries traffic but fails its probes does not have the group probed over
// and over.
func (gb *GroupBase) onDeadNodeDialed(p C.Proxy, testUrl string, fn func()) {
	if p.AliveForTestUrl(testUrl) {
		return
	}
	gb.reviveMux.Lock()
	now := time.Now()
	if now.Before(gb.reviveNext) {
		gb.reviveMux.Unlock()
		return
	}
	if gb.reviveWait == 0 {
		gb.reviveWait = reviveFirstWait
	}
	// No second check starts while this one runs.
	gb.reviveNext = now.Add(reviveMaxWait)
	gb.reviveMux.Unlock()

	log.Infoln("[Проба] %s: соединение через %s прошло, а узел помечен мёртвым — группа перепроверяется", gb.Name(), p.Name())
	go func() {
		fn()
		gb.reviveMux.Lock()
		defer gb.reviveMux.Unlock()
		if p.AliveForTestUrl(testUrl) {
			gb.reviveWait = reviveFirstWait
		} else if gb.reviveWait *= 2; gb.reviveWait > reviveMaxWait {
			gb.reviveWait = reviveMaxWait
		}
		gb.reviveNext = time.Now().Add(gb.reviveWait)
	}()
}

func (gb *GroupBase) onDialSuccess() {
	if !gb.failedTesting.Load() {
		gb.failedTestMux.Lock()
		gb.failedTimes = 0
		gb.failedTestMux.Unlock()
	}
}
