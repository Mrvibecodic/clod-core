package provider

import (
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// keptNode is a node that keeps shared connections, or not, and may refuse
// to set them aside.
type keptNode struct {
	C.ProxyAdapter
	keeps, refuses bool
	retired        atomic.Int32
}

func (n *keptNode) KeepsConns() bool { return n.keeps }

func (n *keptNode) RetireConns() bool {
	n.retired.Add(1)
	return n.keeps && !n.refuses
}

func TestFreshConnsSetsAsideTheConnectionsANodeKeeps(t *testing.T) {
	for _, c := range []struct {
		name           string
		keeps, refuses bool
		want           bool
		retired        int32
	}{
		{"keeps", true, false, true, 1},
		{"refused", true, true, false, 1},
		{"a connection per stream", false, false, false, 0},
	} {
		node := &keptNode{ProxyAdapter: outbound.NewDirect(), keeps: c.keeps, refuses: c.refuses}
		if got := FreshConns(adapter.NewProxy(node)); got != c.want || node.retired.Load() != c.retired {
			t.Fatalf("%s: set aside %v, asked %d times; want %v, %d", c.name, got, node.retired.Load(), c.want, c.retired)
		}
	}
}
