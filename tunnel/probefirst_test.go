package tunnel

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

type fakeProxy struct {
	C.Proxy
	name    string
	adapter C.ProxyAdapter
}

func (p *fakeProxy) Name() string            { return p.name }
func (p *fakeProxy) Adapter() C.ProxyAdapter { return p.adapter }

type fakeNode struct{ C.ProxyAdapter }

type fakeGroup struct {
	C.ProxyAdapter
	now     string
	members []C.Proxy
}

func (g *fakeGroup) Now() string        { return g.now }
func (g *fakeGroup) Proxies() []C.Proxy { return g.members }

// fakeAuto knows its node like url-test does; Now() would make a new choice.
type fakeAuto struct {
	fakeGroup
	current C.Proxy
}

func (g *fakeAuto) Now() string          { panic("Now() makes a new choice") }
func (g *fakeAuto) CurrentNode() C.Proxy { return g.current }

func node(name string) C.Proxy {
	return &fakeProxy{name: name, adapter: &fakeNode{}}
}

func group(name, now string, members ...C.Proxy) C.Proxy {
	return &fakeProxy{name: name, adapter: &fakeGroup{now: now, members: members}}
}

func TestCurrentNodesWalksNestedGroupsDownToTheNode(t *testing.T) {
	hk, us := node("HK"), node("US")
	usTwin := node("US")
	auto := group("auto", "US", hk, us, usTwin)
	proxy := group("Proxy", "auto", auto, hk)
	idle := group("Idle", "", hk)
	UpdateProxies(map[string]C.Proxy{"Proxy": proxy, "auto": auto, "Idle": idle, "HK": hk, "US": us}, nil)
	defer UpdateProxies(map[string]C.Proxy{}, nil)

	got := map[C.Proxy]int{}
	for _, p := range currentNodes() {
		got[p]++
	}
	// Proxy → auto → US and its namesake; auto itself → the same two; Idle points nowhere.
	if got[us] != 2 || got[usTwin] != 2 || got[hk] != 0 || len(got) != 2 {
		t.Fatalf("current nodes = %v", got)
	}
}

func TestCurrentNodesStopsOnAGroupCycle(t *testing.T) {
	a := &fakeGroup{now: "B"}
	b := &fakeGroup{now: "A"}
	pa, pb := &fakeProxy{name: "A", adapter: a}, &fakeProxy{name: "B", adapter: b}
	a.members, b.members = []C.Proxy{pb}, []C.Proxy{pa}
	UpdateProxies(map[string]C.Proxy{"A": pa, "B": pb}, nil)
	defer UpdateProxies(map[string]C.Proxy{}, nil)

	if got := currentNodes(); len(got) != 0 {
		t.Fatalf("a cycle of groups yielded nodes: %v", got)
	}
}

func TestCurrentNodesAsksAChoosingGroupForItsNodeWithoutANewChoice(t *testing.T) {
	hk, us := node("HK"), node("US")
	auto := &fakeProxy{name: "auto", adapter: &fakeAuto{fakeGroup: fakeGroup{members: []C.Proxy{hk, us}}, current: us}}
	cold := &fakeProxy{name: "cold", adapter: &fakeAuto{fakeGroup: fakeGroup{members: []C.Proxy{hk}}}}
	proxy := group("Proxy", "auto", auto, hk)
	UpdateProxies(map[string]C.Proxy{"Proxy": proxy, "auto": auto, "cold": cold}, nil)
	defer UpdateProxies(map[string]C.Proxy{}, nil)

	got := map[C.Proxy]int{}
	for _, p := range currentNodes() {
		got[p]++
	}
	// Proxy → auto → US; auto itself → US; cold has not chosen yet.
	if got[us] != 2 || len(got) != 1 {
		t.Fatalf("current nodes = %v", got)
	}
}

func TestCurrentNodesSkipGlobalOutsideGlobalMode(t *testing.T) {
	hk := node("HK")
	UpdateProxies(map[string]C.Proxy{"GLOBAL": group("GLOBAL", "HK", hk)}, nil)
	defer UpdateProxies(map[string]C.Proxy{}, nil)
	defer SetMode(Rule)

	SetMode(Rule)
	if got := currentNodes(); len(got) != 0 {
		t.Fatalf("GLOBAL counted in rule mode: %v", got)
	}
	SetMode(Global)
	if got := currentNodes(); len(got) != 1 || got[0] != hk {
		t.Fatalf("GLOBAL not counted in global mode: %v", got)
	}
}
