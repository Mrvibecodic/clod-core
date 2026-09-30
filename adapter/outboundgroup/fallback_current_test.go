package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// Reading the current node of a fallback group does not drop a selection
// whose node is down; only a dial does.
func TestFallbackCurrentNodeKeepsADownSelection(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 100, alive: false}
	c := &measuredProxy{name: "C", delay: 100, alive: true}
	provider := &measuredProvider{proxies: []C.Proxy{a, b, c}, version: 1}
	group, err := NewFallback(GroupCommonOption{Name: "fb", URL: "https://probe.invalid/"}, FallbackOption{}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	group.selected.Store("B")

	if got := group.CurrentNode(); got != C.Proxy(c) {
		t.Fatalf("current node with the selection down = %v, want the next alive node C", got)
	}
	if group.selected.Load() != "B" {
		t.Fatalf("reading the current node dropped the selection: %q", group.selected.Load())
	}

	if got := group.findAliveProxy(false); got != C.Proxy(c) || group.selected.Load() != "" {
		t.Fatalf("a dial keeps passing over the down selection and drops it: %v, %q", got, group.selected.Load())
	}

	b.alive = true
	group.selected.Store("B")
	if got := group.CurrentNode(); got != C.Proxy(b) {
		t.Fatalf("current node with the selection alive = %v, want B", got)
	}
}

// A selection whose node is not in the list (a provider dropped it) works as
// no selection: the first alive node, not the first node. The selection is
// kept for the node's return.
func TestFallbackSelectionOfAMissingNode(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: false}
	b := &measuredProxy{name: "B", delay: 100, alive: true}
	provider := &measuredProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewFallback(GroupCommonOption{Name: "fb", URL: "https://probe.invalid/"}, FallbackOption{}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	group.selected.Store("GONE")
	if got := group.findAliveProxy(false); got != C.Proxy(b) {
		t.Fatalf("dial with the selected node missing = %s, want the first alive B", got.Name())
	}
	if got := group.CurrentNode(); got != C.Proxy(b) {
		t.Fatalf("current node = %s, want B", got.Name())
	}
	if group.selected.Load() != "GONE" {
		t.Fatalf("the selection was dropped: %q", group.selected.Load())
	}
	back := &measuredProxy{name: "GONE", delay: 100, alive: true}
	provider.proxies = []C.Proxy{a, b, back}
	provider.version++
	if got := group.findAliveProxy(false); got != C.Proxy(back) {
		t.Fatalf("the selected node came back, the group is on %s", got.Name())
	}
}
