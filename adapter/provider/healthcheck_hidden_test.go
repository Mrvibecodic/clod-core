package provider

import (
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
)

func parsedNode(t *testing.T, mapping map[string]any) C.Proxy {
	t.Helper()
	proxy, err := adapter.ParseProxy(mapping)
	if err != nil {
		t.Fatal(err)
	}
	return proxy
}

// Проверка при первой загрузке провайдера: набор поставлен раньше, чем узлы
// появились, и узлы из него и цепочки через них не проверяются.
func TestHiddenNodesAreNotProbedOnTheFirstLoad(t *testing.T) {
	target := newProbeTarget(t, 0)

	lte := parsedNode(t, map[string]any{"name": "lte", "type": "direct"})
	prev := hidden.SetLookup(func(name string) C.Proxy {
		if name == "lte" {
			return lte
		}
		return nil
	})
	t.Cleanup(func() { hidden.SetLookup(prev) })

	hidden.Replace([]string{"lte"})
	t.Cleanup(func() { hidden.Replace(nil) })

	proxies := []C.Proxy{
		parsedNode(t, map[string]any{"name": "lte", "type": "direct"}),
		parsedNode(t, map[string]any{"name": "via-lte", "type": "direct", "dialer_proxy": "lte"}),
		parsedNode(t, map[string]any{"name": "wifi", "type": "direct"}),
	}

	hc := NewHealthCheck(proxies, target.url(), 1000, 0, false, nil)
	t.Cleanup(hc.close)
	hc.check()
	if target.connections() != 1 {
		t.Fatalf("проверено узлов: %d, а не один видимый", target.connections())
	}

	hidden.Replace(nil)
	shown := NewHealthCheck(proxies[:1], target.url(), 1000, 0, false, nil)
	t.Cleanup(shown.close)
	shown.check()
	if target.connections() != 2 {
		t.Fatalf("показанный узел не проверен: %d", target.connections())
	}
}
