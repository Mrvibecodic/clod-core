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
	}{
		{"nothing registered", nil, "abcd"},
		{"current last", []Proxy{d}, "dabc"},
		{"two current keep their order", []Proxy{c, b}, "bcad"},
		{"current already first", []Proxy{a}, "abcd"},
		{"current not in the list", []Proxy{other}, "abcd"},
		{"replaced object of the same node", []Proxy{&orderedProxy{name: "c"}}, "cabd"},
		{"namesake of another provider", []Proxy{&orderedProxy{name: "c", provider: "other"}}, "abcd"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			SetProbeFirst(func() []Proxy { return tt.current })
			defer SetProbeFirst(nil)
			if got := names(ProbeFirstOrder(list)); got != tt.want {
				t.Fatalf("order = %s, want %s", got, tt.want)
			}
			if got := names(list); got != "abcd" {
				t.Fatalf("the input was reordered in place: %s", got)
			}
		})
	}
}
