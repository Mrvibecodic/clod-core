package outboundgroup

import (
	"encoding/json"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

func nowOf(t *testing.T, group json.Marshaler) string {
	t.Helper()
	data, err := group.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var described struct {
		Now string `json:"now"`
	}
	if err := json.Unmarshal(data, &described); err != nil {
		t.Fatal(err)
	}
	return described.Now
}

// Reading a fallback group as JSON does not drop a selection whose node is
// down: a client that polls the list is not a dial.
func TestFallbackJSONKeepsADownSelection(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 100, alive: false}
	provider := &measuredProvider{proxies: []C.Proxy{a, b}, version: 1}
	group, err := NewFallback(GroupCommonOption{Name: "fb", URL: "https://probe.invalid/"}, FallbackOption{}, a, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	group.selected.Store("B")
	if now := nowOf(t, group); now != "A" || group.selected.Load() != "B" {
		t.Fatalf("now %q, selection %q", now, group.selected.Load())
	}
}

// Reading a url-test group as JSON shows the node it uses without choosing
// anew; before the first dial it shows the node it would pick.
func TestURLTestJSONDoesNotChoose(t *testing.T) {
	slow := &measuredProxy{name: "slow", delay: 300, alive: true}
	fast := &measuredProxy{name: "fast", delay: 100, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{slow, fast})
	if now := nowOf(t, group); now != "fast" {
		t.Fatalf("before the first dial: now %q", now)
	}
	slow.delay = 10
	group.fastSingle.Reset()
	if now := nowOf(t, group); now != "fast" {
		t.Fatalf("a read chose anew: now %q", now)
	}
	if got := group.fast(false); got != C.Proxy(slow) {
		t.Fatalf("the dial after it did not: %v", got)
	}
}

// A pinned node is shown as soon as it is pinned: the next dial takes it,
// whatever the group used before.
func TestURLTestJSONShowsThePinAtOnce(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 200, alive: true}
	group, _ := measuredGroup(t, []C.Proxy{a, b})
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("first dial: %v", got)
	}
	group.ForceSet("B")
	if now := nowOf(t, group); now != "B" {
		t.Fatalf("after the pin: now %q", now)
	}
	// A dead pin is not what the next dial takes: the node in use is shown.
	b.alive = false
	group.fastSingle.Reset()
	if now := nowOf(t, group); now != "A" {
		t.Fatalf("dead pin: now %q", now)
	}
}

// A node in use that died or left the group is shown replaced, the way the
// next dial replaces it; a live one is kept without a new choice.
func TestURLTestJSONReplacesADeadOrGoneCurrentNode(t *testing.T) {
	a := &measuredProxy{name: "A", delay: 100, alive: true}
	b := &measuredProxy{name: "B", delay: 200, alive: true}
	group, provider := measuredGroup(t, []C.Proxy{a, b})
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("first dial: %v", got)
	}
	// The check that found A dead also drops the cached choice; a dead node
	// reports the worst delay, as the real adapter does.
	a.alive = false
	a.delay = 0xffff
	group.fastSingle.Reset()
	if now := nowOf(t, group); now != "B" {
		t.Fatalf("dead current node: now %q", now)
	}
	a.alive = true
	a.delay = 100
	group.fastSingle.Reset()
	if got := group.fast(false); got != C.Proxy(a) {
		t.Fatalf("back alive, dial: %v", got)
	}
	// The node left with a provider update; the cached choice outlives it by
	// ten seconds at most, as it does for a dial.
	provider.proxies = []C.Proxy{b}
	provider.version++
	group.fastSingle.Reset()
	if now := nowOf(t, group); now != "B" {
		t.Fatalf("gone current node: now %q", now)
	}
}
