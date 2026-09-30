package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type namedAdapter struct {
	C.ProxyAdapter
	name string
}

func (n *namedAdapter) Name() string { return n.name }

// A node that was never measured at the group url (for example, left out of
// the checks by a filter) must not hold the group once other nodes have
// measured delays: only a node replaced by a provider update waits for its
// first check.
func TestURLTestNeverMeasuredNodeDoesNotHoldTheGroup(t *testing.T) {
	const url = "https://probe.invalid/204"
	mk := func(name string) *adapter.Proxy {
		return adapter.NewProxy(&namedAdapter{outbound.NewDirect(), name})
	}
	us, hk1, hk2 := mk("US-1"), mk("HK-1"), mk("HK-2")
	provider := &measuredProvider{proxies: []C.Proxy{us, hk1, hk2}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: url, TestTimeout: 5000},
		URLTestOption{Tolerance: 50}, us, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.Now(); got != "US-1" {
		t.Fatalf("before any check = %s", got)
	}
	now := time.Now()
	hk1.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Delay: 80, Satisfied: true})
	hk2.RecordProbe(url, &C.ProbeResult{Held: true, Time: now, Delay: 90, Satisfied: true})
	group.fastSingle.Reset()
	if got := group.Now(); got != "HK-1" {
		t.Fatalf("after the check = %s, want HK-1", got)
	}
}

// A node of another provider with the same name does not take the place of
// the current node while the current object is still listed.
func TestURLTestKeepsTheSameObjectAmongNamesakes(t *testing.T) {
	a := &measuredProxy{name: "HK-1", delay: 50, alive: true}
	b := &measuredProxy{name: "HK-1", delay: 90, alive: true}
	provider := &measuredProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/"},
		URLTestOption{Tolerance: 50}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatal("initial choice is not the faster namesake")
	}
	for i := 0; i < 3; i++ {
		group.fastSingle.Reset()
		if got := group.fast(false); got != C.Proxy(a) {
			t.Fatalf("round %d: the group moved to the slower namesake", i)
		}
	}
}

// A node replaced twice before its first check keeps competing with the
// score of the last measured object.
func TestURLTestCarriesTheScoreThroughTwoReplacements(t *testing.T) {
	old := &measuredProxy{name: "current", delay: 100, alive: true}
	other := &measuredProxy{name: "other", delay: 0xffff, alive: true}
	provider := &measuredProvider{proxies: []C.Proxy{old, other}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/"},
		URLTestOption{Tolerance: 50}, old, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.fast(false); got != C.Proxy(old) {
		t.Fatal("initial choice")
	}
	for _, name := range []string{"first", "second"} {
		replacement := &measuredProxy{name: "current", delay: 0xffff, alive: true}
		provider.proxies = []C.Proxy{replacement, other}
		provider.version++
		group.fastSingle.Reset()
		if got := group.fast(false); got != C.Proxy(replacement) {
			t.Fatalf("%s replacement: the group did not follow the new object", name)
		}
	}
	other.delay = 120
	group.fastSingle.Reset()
	if got := group.Now(); got != "current" {
		t.Fatalf("a node 20 ms slower than the carried score took over: %s", got)
	}
}

// After one provider replaces the current node, the group follows that
// provider's new object, not a namesake from another provider.
func TestURLTestFollowsTheReplacementFromTheSameProvider(t *testing.T) {
	xa := &measuredProxy{name: "X", delay: 120, alive: true, provider: "A"}
	ya := &measuredProxy{name: "Y", delay: 300, alive: true, provider: "A"}
	xb := &measuredProxy{name: "X", delay: 100, alive: true, provider: "B"}
	a := &measuredProvider{proxies: []C.Proxy{xa, ya}, version: 1}
	b := &measuredProvider{proxies: []C.Proxy{xb}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/"},
		URLTestOption{Tolerance: 50}, xa, []P.ProxyProvider{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.fast(false); got != C.Proxy(xb) {
		t.Fatal("initial choice is not X of B")
	}
	nb := &measuredProxy{name: "X", delay: 0xffff, alive: true, provider: "B"}
	b.proxies = []C.Proxy{nb}
	b.version++
	group.fastSingle.Reset()
	if got := group.fast(false); got != C.Proxy(nb) {
		t.Fatal("the group moved to X of another provider instead of the replacement")
	}
}

// A node removed by its provider is not replaced by a namesake from another
// provider: that is another server, and the choice is made afresh.
func TestURLTestRemovedNodeIsNotReplacedByANamesake(t *testing.T) {
	xa := &measuredProxy{name: "X", delay: 100, alive: true, provider: "A"}
	za := &measuredProxy{name: "Z", delay: 250, alive: true, provider: "A"}
	xb := &measuredProxy{name: "X", delay: 300, alive: true, provider: "B"}
	a := &measuredProvider{proxies: []C.Proxy{xa, za}, version: 1}
	b := &measuredProvider{proxies: []C.Proxy{xb}, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/"},
		URLTestOption{Tolerance: 50}, xa, []P.ProxyProvider{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got := group.fast(false); got != C.Proxy(xa) {
		t.Fatal("initial choice is not X of A")
	}
	a.proxies = []C.Proxy{za}
	a.version++
	group.fastSingle.Reset()
	if got := group.fast(false); got != C.Proxy(za) {
		t.Fatalf("after X of A was removed the group chose %v, want the fresh best Z", got.Name())
	}
}
