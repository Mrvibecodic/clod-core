package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type SelectorOption struct {
	DefaultSelected string `group:"default-selected,omitempty"`
}

type Selector struct {
	*GroupBase
	disableUDP     bool
	selected       string
	testUrl        string
	expectedStatus string
}

// DialContext implements C.ProxyAdapter
func (s *Selector) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	c, err := s.selectedProxy(true).DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(s)
	}
	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (s *Selector) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	pc, err := s.selectedProxy(true).ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(s)
	}
	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (s *Selector) SupportUDP() bool {
	if s.disableUDP {
		return false
	}

	return s.selectedProxy(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (s *Selector) IsL3Protocol(metadata *C.Metadata) bool {
	return s.selectedProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (s *Selector) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range s.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	// When testurl is the default value
	// do not append a value to ensure that the web dashboard follows the settings of the dashboard
	var url string
	if s.testUrl != C.DefaultTestURL {
		url = s.testUrl
	}

	return json.Marshal(map[string]any{
		"type":           s.Type().String(),
		"now":            s.Now(),
		"all":            all,
		"testUrl":        url,
		"expectedStatus": s.expectedStatus,
		"hidden":         s.Hidden(),
		"icon":           s.Icon(),
		"emptyFallback":  s.EmptyFallback().Name(),
	})
}

func (s *Selector) Now() string {
	return s.selectedProxy(false).Name()
}

func (s *Selector) Set(name string) error {
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name && proxy != hiddenReject {
			s.selected = name
			return nil
		}
	}

	// Скрытый узел не выбирается; выбор, сохранённый раньше, ставит ForceSet:
	// пока узел скрыт, группа идёт через видимый и вернётся к нему, когда его
	// покажут.
	if s.hiddenMember(name) {
		return ErrHidden
	}

	return errors.New("proxy not exist")
}

// SelectedHidden — выбранный пользователем узел сейчас скрыт.
func (s *Selector) SelectedHidden() bool {
	return s.selected != "" && s.hiddenMember(s.selected)
}

func (s *Selector) ForceSet(name string) {
	s.selected = name
}

// Unwrap implements C.ProxyAdapter
func (s *Selector) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return s.selectedProxy(touch)
}

func (s *Selector) selectedProxy(touch bool) C.Proxy {
	proxies, all := s.members(touch)
	for _, proxy := range proxies {
		if proxy.Name() == s.selected {
			if hidden.Active() && groupAllHidden(proxy) {
				return visibleFallback(proxies)
			}
			return proxy
		}
	}

	// Выбора нет (или он пропал) — первый член группы, как в конфиге. Без
	// скрытых это proxies[0], как в ядре; скрыт он — видимый сервер, а не
	// DIRECT, что мог оказаться первым среди видимых.
	if hidden.Active() {
		first := all[0]
		for _, proxy := range all {
			if proxy.Name() == s.selected {
				first = proxy
				break
			}
		}
		if first != proxies[0] || groupAllHidden(first) {
			return visibleFallback(proxies)
		}
	}

	return proxies[0]
}

func (s *Selector) Providers() []P.ProxyProvider {
	return s.providers
}

func (s *Selector) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func NewSelector(option GroupCommonOption, selectorOption SelectorOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Selector, error) {
	return &Selector{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Selector,
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
		selected:       selectorOption.DefaultSelected,
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}, nil
}
