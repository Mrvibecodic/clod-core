package tunnel

import (
	C "github.com/metacubex/mihomo/constant"
)

// proxyGroup is what every group adapter answers: the name of the node it
// points at now and its members.
type proxyGroup interface {
	Now() string
	Proxies() []C.Proxy
}

// currentNoder is a group that can name its node without choosing anew.
type currentNoder interface {
	CurrentNode() C.Proxy
}

func init() {
	C.SetProbeFirst(currentNodes)
}

// currentNodes are the nodes the groups point at right now, nested groups
// walked down to the node. The members are read without touching the groups,
// so a lazy check stays lazy. GLOBAL carries traffic only in global mode.
func currentNodes() []C.Proxy {
	configMux.RLock()
	all := proxies
	configMux.RUnlock()

	var nodes []C.Proxy
	for name, p := range all {
		if name == "GLOBAL" && Mode() != Global {
			continue
		}
		if g, ok := p.Adapter().(proxyGroup); ok {
			nodes = appendCurrentNodes(nodes, g)
		}
	}
	return nodes
}

func appendCurrentNodes(nodes []C.Proxy, g proxyGroup) []C.Proxy {
	for depth := 0; depth < 16; depth++ {
		var inner proxyGroup
		if c, ok := g.(currentNoder); ok {
			p := c.CurrentNode()
			if p == nil {
				return nodes
			}
			if next, ok := p.Adapter().(proxyGroup); ok {
				inner = next
			} else {
				nodes = append(nodes, p)
			}
		} else {
			now := g.Now()
			if now == "" {
				return nodes
			}
			// Namesakes from different providers are different servers
			// with one name: every one of them is taken.
			for _, p := range g.Proxies() {
				if p.Name() != now {
					continue
				}
				if next, ok := p.Adapter().(proxyGroup); ok {
					inner = next
				} else {
					nodes = append(nodes, p)
				}
			}
		}
		if inner == nil {
			return nodes
		}
		g = inner
	}
	return nodes
}
