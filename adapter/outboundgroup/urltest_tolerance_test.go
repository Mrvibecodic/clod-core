package outboundgroup

import (
	"fmt"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type measuredProxy struct {
	C.Proxy
	name  string
	delay uint16
	alive bool
}

func (p *measuredProxy) Name() string                                   { return p.name }
func (p *measuredProxy) AliveForTestUrl(string) bool                    { return p.alive }
func (p *measuredProxy) LastDelayForTestUrl(string) uint16              { return p.delay }
func (p *measuredProxy) DelayHistoryForTestUrl(string) []C.DelayHistory { return nil }

type measuredProvider struct {
	P.ProxyProvider
	proxies []C.Proxy
	version uint32
}

func (p *measuredProvider) Proxies() []C.Proxy { return p.proxies }
func (p *measuredProvider) Version() uint32    { return p.version }

func measuredGroup(t *testing.T, proxies []C.Proxy) (*URLTest, *measuredProvider) {
	t.Helper()
	provider := &measuredProvider{proxies: proxies, version: 1}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/"},
		URLTestOption{Tolerance: 50}, proxies[0], []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	return group, provider
}

// The tolerance holds the current node wherever it stands in the list,
// the first place included.
func TestURLTestToleranceAtEveryPosition(t *testing.T) {
	for position := 0; position < 3; position++ {
		for _, improvement := range []uint16{1, 49, 50, 51} {
			t.Run(fmt.Sprintf("position=%d/improvement=%d", position, improvement), func(t *testing.T) {
				current := &measuredProxy{name: "current", delay: 100, alive: true}
				alternate := &measuredProxy{name: "alternate", delay: 200, alive: true}
				third := &measuredProxy{name: "third", delay: 300, alive: true}
				proxies := []C.Proxy{alternate, third}
				proxies = append(proxies[:position], append([]C.Proxy{current}, proxies[position:]...)...)
				group, _ := measuredGroup(t, proxies)
				if got := group.Now(); got != "current" {
					t.Fatalf("initial = %s", got)
				}
				alternate.delay = 100 - improvement
				group.fastSingle.Reset()
				want := "current"
				if improvement > 50 {
					want = "alternate"
				}
				if got := group.Now(); got != want {
					t.Fatalf("selected = %s, want %s", got, want)
				}
			})
		}
	}
}

// The tolerance does not keep a dead or removed first node.
func TestURLTestToleranceDropsDeadOrRemovedNode(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed=%t", removed), func(t *testing.T) {
			current := &measuredProxy{name: "current", delay: 100, alive: true}
			alternate := &measuredProxy{name: "alternate", delay: 110, alive: true}
			group, provider := measuredGroup(t, []C.Proxy{current, alternate})
			if got := group.Now(); got != "current" {
				t.Fatal(got)
			}
			if removed {
				provider.proxies = []C.Proxy{alternate}
				provider.version++
			} else {
				current.alive = false
				current.delay = 65535
			}
			group.fastSingle.Reset()
			if got := group.Now(); got != "alternate" {
				t.Fatalf("failed node retained: %s", got)
			}
		})
	}
}
