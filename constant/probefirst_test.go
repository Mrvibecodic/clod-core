package constant

import "testing"

type orderedProxy struct {
	Proxy
	name     string
	provider string
}

func (p *orderedProxy) Name() string         { return p.name }
func (p *orderedProxy) ProxyInfo() ProxyInfo { return ProxyInfo{ProviderName: p.provider} }

func names(proxies []Proxy) string {
	s := ""
	for _, p := range proxies {
		s += p.Name()
	}
	return s
}

func TestProbeFirstOrderMovesTheCurrentNodesToTheFront(t *testing.T) {
	a, b, c, d := &orderedProxy{name: "a"}, &orderedProxy{name: "b"}, &orderedProxy{name: "c"}, &orderedProxy{name: "d"}
	list := []Proxy{a, b, c, d}
	other := &orderedProxy{name: "x"}
	for _, tt := range []struct {
		name    string
		current []Proxy
		want    string
		head    int
	}{
		{"nothing registered", nil, "abcd", 0},
		{"current last", []Proxy{d}, "dabc", 1},
		{"two current keep their order", []Proxy{c, b}, "bcad", 2},
		{"current already first", []Proxy{a}, "abcd", 1},
		{"current not in the list", []Proxy{other}, "abcd", 0},
		{"replaced object of the same node", []Proxy{&orderedProxy{name: "c"}}, "cabd", 1},
		{"namesake of another provider", []Proxy{&orderedProxy{name: "c", provider: "other"}}, "abcd", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			SetProbeFirst(func() []Proxy { return tt.current })
			defer SetProbeFirst(nil)
			ordered, head := ProbeFirstOrder(list)
			if got := names(ordered); got != tt.want || head != tt.head {
				t.Fatalf("order = %s (%d first), want %s (%d first)", got, head, tt.want, tt.head)
			}
			if got := names(list); got != "abcd" {
				t.Fatalf("the input was reordered in place: %s", got)
			}
		})
	}
}
