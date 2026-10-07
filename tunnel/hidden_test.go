package tunnel

import (
	"testing"

	"github.com/metacubex/mihomo/component/hidden"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/gofrs/uuid/v5"
)

type chainTracker struct {
	C.Connection
	info   *statistic.TrackerInfo
	closed bool
}

func (c *chainTracker) ID() string                   { return c.info.UUID.String() }
func (c *chainTracker) Info() *statistic.TrackerInfo { return c.info }
func (c *chainTracker) Close() error                 { c.closed = true; return nil }

func newChainTracker(t *testing.T, chain ...string) *chainTracker {
	c := &chainTracker{info: &statistic.TrackerInfo{UUID: uuid.Must(uuid.NewV4()), Chain: chain}}
	statistic.DefaultManager.Join(c)
	t.Cleanup(func() { statistic.DefaultManager.Leave(c) })
	return c
}

// hiddenServer — узел из конфига: имя и dialer-proxy; group — группа.
type hiddenServer struct {
	C.Proxy
	name, dialer string
	group        bool
}

func (s *hiddenServer) Name() string { return s.name }
func (s *hiddenServer) Server() (string, bool) {
	return s.dialer, !s.group
}

type serverProvider struct {
	P.ProxyProvider
	proxies []C.Proxy
}

func (p *serverProvider) Proxies() []C.Proxy { return p.proxies }

func useProxies(t *testing.T, own []C.Proxy, provided ...C.Proxy) {
	prevProxies, prevProviders := Proxies(), Providers()
	byName := map[string]C.Proxy{}
	for _, p := range own {
		byName[p.Name()] = p
	}
	UpdateProxies(byName, map[string]P.ProxyProvider{"sub": &serverProvider{proxies: provided}})
	t.Cleanup(func() {
		hidden.Replace(nil)
		UpdateProxies(prevProxies, prevProviders)
	})
}

func TestSetHiddenClosesOnlyWhatGoesThroughNewlyHiddenServers(t *testing.T) {
	useProxies(t,
		[]C.Proxy{
			&hiddenServer{name: "lte"},
			&hiddenServer{name: "wifi"},
			&hiddenServer{name: "Proxy", group: true},
		},
		// Узлы провайдера: через сервер из конфига и через группу.
		&hiddenServer{name: "via-lte", dialer: "lte"},
		&hiddenServer{name: "via-group", dialer: "Proxy"},
	)

	through := newChainTracker(t, "lte", "Proxy")
	chained := newChainTracker(t, "via-lte", "Proxy")
	other := newChainTracker(t, "wifi", "Proxy")
	viaGroup := newChainTracker(t, "via-group", "Proxy")
	// Скрытое имя дальше по цепочке — группа, а не сервер соединения.
	group := newChainTracker(t, "wifi", "lte")

	if !SetHidden([]string{"lte", "Proxy", "DIRECT"}) {
		t.Fatal("набор сменился")
	}
	if !through.closed || !chained.closed || other.closed || viaGroup.closed || group.closed {
		t.Fatalf("закрыты: %v %v %v %v %v", through.closed, chained.closed, other.closed, viaGroup.closed, group.closed)
	}

	through.closed = false
	if SetHidden([]string{"DIRECT", "Proxy", "lte"}) || through.closed {
		t.Fatal("тот же набор соединений не трогает")
	}

	// Расширение закрывает только новые: соединение через уже скрытый
	// сервер (правило, назвавшее его прямо) остаётся.
	if !SetHidden([]string{"lte", "wifi"}) || through.closed || !other.closed {
		t.Fatalf("расширение: %v %v", through.closed, other.closed)
	}

	other.closed = false
	if !SetHidden(nil) || hidden.Active() || through.closed || other.closed {
		t.Fatal("показ серверов соединений не трогает")
	}
}
