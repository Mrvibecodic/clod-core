package outboundgroup

import (
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

func hide(t *testing.T, names ...string) {
	t.Helper()
	hidden.Replace(names)
	t.Cleanup(func() { hidden.Replace(nil) })
}

func node(name string, delay uint16) *urlTestProxy {
	return &urlTestProxy{name: name, alive: true, delay: delay}
}

// Узлы из тестов — серверы, не встроенные и не группы.
func (p *urlTestProxy) Type() C.AdapterType     { return C.Vless }
func (p *urlTestProxy) Server() (string, bool)  { return "", true }
func (p *urlTestProxy) Adapter() C.ProxyAdapter { return nil }

// chainedNode — сервер, который соединяется через dialer (dialer-proxy).
type chainedNode struct {
	*urlTestProxy
	dialer string
}

func (p *chainedNode) Server() (string, bool) { return p.dialer, true }

func useLookup(t *testing.T, proxies ...C.Proxy) {
	byName := map[string]C.Proxy{}
	for _, p := range proxies {
		byName[p.Name()] = p
	}
	prev := hidden.SetLookup(func(name string) C.Proxy { return byName[name] })
	t.Cleanup(func() { hidden.SetLookup(prev) })
}

func names(proxies []C.Proxy) []string {
	out := []string{}
	for _, p := range proxies {
		out = append(out, p.Name())
	}
	return out
}

func selector(proxies ...C.Proxy) *Selector {
	return &Selector{GroupBase: NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{&urlTestProvider{proxies: proxies, version: 1}}})}
}

func TestHiddenNodesLeaveGroupMembers(t *testing.T) {
	provider := &urlTestProvider{proxies: []C.Proxy{node("lte", 10), node("wifi", 20)}, version: 1}
	gb := NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}})

	if got := names(gb.GetProxies(false)); len(got) != 2 {
		t.Fatalf("без набора все узлы: %v", got)
	}

	hide(t, "lte")
	if got := names(gb.GetProxies(false)); len(got) != 1 || got[0] != "wifi" {
		t.Fatalf("скрытый узел остался в группе (кеш списка): %v", got)
	}

	hidden.Replace(nil)
	if got := names(gb.GetProxies(false)); len(got) != 2 {
		t.Fatalf("показанный снова узел не вернулся: %v", got)
	}
}

func TestProviderNodesThatComeAfterTheSetAreHidden(t *testing.T) {
	lte := node("lte", 10)
	useLookup(t, lte)

	// Набор поставлен, пока у провайдера ещё ничего нет (до загрузки).
	hide(t, "lte")
	provider := &urlTestProvider{version: 0}
	gb := NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}, EmptyFallback: node("COMPATIBLE", 0)})
	gb.GetProxies(false)

	// Провайдер загрузился: узел из списка и сервер через него (dialer-proxy).
	provider.proxies = []C.Proxy{lte, &chainedNode{node("via-lte", 30), "lte"}, node("wifi", 20)}
	provider.version = 1
	if got := names(gb.GetProxies(false)); len(got) != 1 || got[0] != "wifi" {
		t.Fatalf("загруженный провайдер: %v", got)
	}

	// Обновление провайдера добавило ещё один узел из списка.
	provider.proxies = append(provider.proxies, node("lte", 15))
	provider.version = 2
	if got := names(gb.GetProxies(false)); len(got) != 1 || got[0] != "wifi" {
		t.Fatalf("обновлённый провайдер: %v", got)
	}
}

func TestGroupWithEveryNodeHiddenRejects(t *testing.T) {
	compatible := node("COMPATIBLE", 0)

	provider := &urlTestProvider{proxies: []C.Proxy{node("lte", 10)}, version: 1}
	gb := NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}, EmptyFallback: compatible})

	hide(t, "lte")
	got := gb.GetProxies(false)
	if !AllHidden(got) || got[0].Type() != C.Reject {
		t.Fatalf("группа без видимых узлов обязана отказывать, а не идти напрямую: %v", names(got))
	}
	if again := gb.GetProxies(false); &again[0] != &got[0] {
		t.Fatal("список скрытой группы не закеширован")
	}

	// Пустая по другой причине — как прежде, через EmptyFallback.
	empty := NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{&urlTestProvider{version: 1}}, EmptyFallback: compatible})
	if got := empty.GetProxies(false); len(got) != 1 || got[0] != compatible || AllHidden(got) {
		t.Fatalf("пустая группа: %v", names(got))
	}
}

func TestAHiddenNodeIsNotSelectable(t *testing.T) {
	provider := &urlTestProvider{proxies: []C.Proxy{node("lte", 10), node("wifi", 20)}, version: 1}
	s := &Selector{GroupBase: NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}})}
	u := &URLTest{GroupBase: NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}}), fastSingle: singledo.NewSingle[C.Proxy](time.Second)}
	f := &Fallback{GroupBase: NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{provider}})}

	hide(t, "lte")
	for name, set := range map[string]func(string) error{"select": s.Set, "url-test": u.Set, "fallback": f.Set} {
		if err := set("lte"); !errors.Is(err, ErrHidden) {
			t.Fatalf("%s: скрытый узел выбран: %v", name, err)
		}
		if err := set("REJECT"); err == nil {
			t.Fatalf("%s: отказ скрытой группы выбран", name)
		}
		if err := set("unknown"); err == nil || errors.Is(err, ErrHidden) {
			t.Fatalf("%s: чужое имя: %v", name, err)
		}
	}
	if s.SelectedHidden() || s.Now() != "wifi" {
		t.Fatal("отказ ничего не запоминает")
	}
}

func TestSelectorKeepsARestoredHiddenChoice(t *testing.T) {
	s := selector(node("wifi", 20), node("lte", 10))

	if err := s.Set("lte"); err != nil || s.Now() != "lte" {
		t.Fatalf("выбор: %v %s", err, s.Now())
	}

	hide(t, "lte")
	if s.Now() != "wifi" || !s.SelectedHidden() {
		t.Fatalf("скрытый выбор — первый видимый, а не %s", s.Now())
	}

	// Выбор, восстановленный при загрузке, пока узел скрыт, ставится как есть.
	r := selector(node("wifi", 20), node("lte", 10))
	r.ForceSet("lte")
	if r.Now() != "wifi" {
		t.Fatalf("восстановленный скрытый выбор: %s", r.Now())
	}

	hidden.Replace(nil)
	if s.Now() != "lte" || r.Now() != "lte" || s.SelectedHidden() {
		t.Fatalf("показанный узел снова выбран, а не %s/%s", s.Now(), r.Now())
	}
}

func TestURLTestDropsAHiddenCurrentNode(t *testing.T) {
	lte, wifi := node("lte", 10), node("wifi", 300)
	u := &URLTest{
		GroupBase:  NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{&urlTestProvider{proxies: []C.Proxy{lte, wifi}, version: 1}}}),
		tolerance:  50,
		fastSingle: singledo.NewSingle[C.Proxy](time.Hour),
	}

	if got := u.fast(false); got != lte {
		t.Fatalf("быстрый: %s", got.Name())
	}

	hide(t, "lte")
	if got := u.fast(false); got != wifi {
		t.Fatalf("скрытый текущий узел не сменён, несмотря на кеш выбора: %s", got.Name())
	}
	if current := u.CurrentNode(); current != nil && current.Name() == "lte" {
		t.Fatal("текущим показан скрытый узел")
	}

	// Закрепление, восстановленное при загрузке, пока узел скрыт.
	u.ForceSet("lte")
	if got := u.fast(false); got != wifi {
		t.Fatalf("закреплённый скрытый не используется: %s", got.Name())
	}

	hidden.Replace(nil)
	if got := u.fast(false); got != lte {
		t.Fatalf("закреплённый показанный снова используется: %s", got.Name())
	}
}

func TestFallbackSkipsAHiddenNode(t *testing.T) {
	lte, wifi := node("lte", 10), node("wifi", 20)
	f := &Fallback{GroupBase: NewGroupBase(GroupBaseOption{Providers: []P.ProxyProvider{&urlTestProvider{proxies: []C.Proxy{lte, wifi}, version: 1}}})}

	hide(t, "lte")
	if got := f.findAliveProxy(false); got != wifi {
		t.Fatalf("fallback через скрытый: %s", got.Name())
	}
	f.ForceSet("lte")
	if got := f.findAliveProxy(false); got != wifi {
		t.Fatalf("закреплённый скрытый: %s", got.Name())
	}

	hidden.Replace(nil)
	if got := f.findAliveProxy(false); got != lte {
		t.Fatalf("закреплённый показанный: %s", got.Name())
	}
}

func TestAHiddenChoiceNeverFallsBackToDirect(t *testing.T) {
	direct := adapter.NewProxy(outbound.NewDirect())
	reject := adapter.NewProxy(outbound.NewReject())

	// GLOBAL: DIRECT и REJECT впереди, дальше серверы.
	s := selector(direct, reject, node("lte", 10), node("wifi", 20))
	if err := s.Set("lte"); err != nil {
		t.Fatal(err)
	}

	hide(t, "lte")
	if got := s.Now(); got != "wifi" {
		t.Fatalf("GLOBAL со скрытым выбором пошёл через %s", got)
	}

	// Выбора не было, первый член скрыт, следом DIRECT.
	if got := selector(node("lte", 10), direct, node("NL", 20)).Now(); got != "NL" {
		t.Fatalf("без выбора: %s", got)
	}

	// Обычная select-группа с DIRECT первым и без других серверов.
	only := selector(direct, node("lte", 10))
	only.ForceSet("lte")
	if got := only.selectedProxy(false); got.Type() != C.Reject {
		t.Fatalf("без видимых серверов — отказ, а не %s", got.Name())
	}

	// Не скрытый, а пропавший выбор при видимом первом — как в ядре: первый.
	gone := selector(direct, reject, node("lte", 10), node("wifi", 20))
	gone.ForceSet("removed")
	if got := gone.Now(); got != "DIRECT" {
		t.Fatalf("пропавший выбор: %s", got)
	}
}

func TestAGroupWithEveryNodeHiddenIsSkipped(t *testing.T) {
	auto := adapter.NewProxy(&URLTest{
		GroupBase:  NewGroupBase(GroupBaseOption{Name: "LTE-auto", Type: C.URLTest, Providers: []P.ProxyProvider{&urlTestProvider{proxies: []C.Proxy{node("lte", 10)}, version: 1}}}),
		fastSingle: singledo.NewSingle[C.Proxy](time.Second),
	})

	hide(t, "lte")

	if got := selector(auto, node("lte", 10), node("wifi", 20)).Now(); got != "wifi" {
		t.Fatalf("без выбора через группу, где всё скрыто: %s", got)
	}

	picked := selector(auto, node("wifi", 20))
	if err := picked.Set("LTE-auto"); err != nil {
		t.Fatal(err)
	}
	if got := picked.Now(); got != "wifi" {
		t.Fatalf("выбранная группа, где всё скрыто: %s", got)
	}

	hidden.Replace(nil)
	if got := picked.Now(); got != "LTE-auto" {
		t.Fatalf("показанная группа снова выбрана: %s", got)
	}
}
