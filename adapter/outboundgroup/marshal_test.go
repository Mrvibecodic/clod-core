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
