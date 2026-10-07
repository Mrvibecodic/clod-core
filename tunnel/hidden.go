package tunnel

import (
	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

func init() {
	// dialer-proxy ищется там же, где его ищет соединение (proxydialer).
	hidden.SetLookup(func(name string) C.Proxy {
		return Proxies()[name]
	})
}

// SetHidden — имена серверов, которые клиент убирает из групп
// (component/hidden). Скрываются только серверы — узлы из конфига и
// провайдеров, в том числе пришедшие позже, — и серверы, которые соединяются
// через скрытый (dialer-proxy); имя группы или встроенного узла ничего не
// скрывает. Соединения через только что скрытый сервер закрываются,
// остальные не трогаются. changed — набор сменился.
func SetHidden(names []string) (changed bool) {
	before, changed := hidden.Replace(names)
	if !changed {
		return false
	}

	servers := map[string][]C.Proxy{}
	EachProxy(func(p C.Proxy) {
		servers[p.Name()] = append(servers[p.Name()], p)
	})

	statistic.DefaultManager.Range(func(c statistic.Tracker) bool {
		// Первый в цепочке — сам сервер, дальше группы.
		if chain := c.Info().Chain; len(chain) > 0 {
			for _, p := range servers[chain[0]] {
				if hidden.Hides(p) && !before(p) {
					_ = c.Close()
					break
				}
			}
		}

		return true
	})

	return true
}

// EachProxy — серверы и группы ядра вместе с узлами провайдеров.
func EachProxy(visit func(C.Proxy)) {
	for _, p := range Proxies() {
		visit(p)
	}
	for _, pd := range Providers() {
		for _, p := range pd.Proxies() {
			visit(p)
		}
	}
}
