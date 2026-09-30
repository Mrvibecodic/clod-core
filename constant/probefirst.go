package constant

// probeFirst reports the nodes the groups point at right now. It is set by the
// tunnel, which owns the groups; a check probes these nodes before the others.
var probeFirst func() []Proxy

func SetProbeFirst(fn func() []Proxy) {
	probeFirst = fn
}

// ProbeFirstOrder returns proxies with the nodes the groups point at moved to
// the front, in their original order, the rest after them in theirs. A round
// runs ten probes at a time, so a node at the end of a long list waited for
// every batch before it; the node the group is using is now in the first one.
//
// Nodes match by name and provider, not by object: a subscription update
// replaces the objects, and the round it starts is the one where the group
// still holds the old object of its node.
func ProbeFirstOrder(proxies []Proxy) []Proxy {
	if probeFirst == nil || len(proxies) < 2 {
		return proxies
	}
	current := probeFirst()
	if len(current) == 0 {
		return proxies
	}
	first := make(map[string]struct{}, len(current))
	for _, p := range current {
		first[probeFirstKey(p)] = struct{}{}
	}
	ordered := make([]Proxy, 0, len(proxies))
	for _, p := range proxies {
		if _, ok := first[probeFirstKey(p)]; ok {
			ordered = append(ordered, p)
		}
	}
	if len(ordered) == 0 {
		return proxies
	}
	for _, p := range proxies {
		if _, ok := first[probeFirstKey(p)]; !ok {
			ordered = append(ordered, p)
		}
	}
	return ordered
}

func probeFirstKey(p Proxy) string {
	return p.Name() + "\x00" + p.ProxyInfo().ProviderName
}
