package outboundgroup

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// slowNode opens its connection when told to.
type slowNode struct {
	*measuredProxy
	// taken is closed when the group has taken the node and dials it.
	taken chan struct{}
	open  chan struct{}
	dead  atomic.Bool
	// far is the other end of the connection the node opened.
	far net.Conn
}

func (n *slowNode) AliveForTestUrl(string) bool { return !n.dead.Load() }

func (n *slowNode) Type() C.AdapterType { return C.Vless }

func (n *slowNode) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	close(n.taken)
	<-n.open
	near, far := net.Pipe()
	n.far = far
	return outbound.NewConn(near, outbound.NewDirect()), nil
}

func slowGroup(t *testing.T, node *slowNode) (*URLTest, *countingProvider) {
	t.Helper()
	provider := &countingProvider{proxies: []C.Proxy{node}}
	group, err := NewURLTest(GroupCommonOption{Name: "auto", URL: "https://probe.invalid/", TestTimeout: 5000},
		URLTestOption{}, node, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	return group, provider
}

// A node marked dead while a connection through it was being opened is not
// taken for a dead node that carries traffic: the group is not checked over,
// whatever comes back over that connection.
func TestANodeMarkedDeadDuringTheDialStartsNoCheck(t *testing.T) {
	node := &slowNode{measuredProxy: &measuredProxy{name: "A", delay: 100, alive: true}, taken: make(chan struct{}), open: make(chan struct{})}
	group, provider := slowGroup(t, node)
	done := make(chan C.Conn)
	go func() {
		conn, _ := group.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP})
		done <- conn
	}()
	// The group took the node alive.
	<-node.taken
	node.dead.Store(true)
	close(node.open)
	conn := <-done
	defer conn.Close()
	go func() { _, _ = node.far.Write([]byte("pong")) }()
	if _, err := conn.Read(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	waitChecks(t, provider, 0)
}

// A connection through a node marked dead that only opened shows nothing:
// many nodes open one without a packet sent. The group is checked over when
// an answer comes back through the node.
func TestADeadNodeIsCheckedOverWhenItAnswersNotWhenItOpens(t *testing.T) {
	node := &slowNode{measuredProxy: &measuredProxy{name: "A", delay: 100, alive: true}, taken: make(chan struct{}), open: make(chan struct{})}
	node.dead.Store(true)
	close(node.open)
	group, provider := slowGroup(t, node)
	conn, err := group.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() { _, _ = node.far.Read(make([]byte, 16)) }()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitChecks(t, provider, 0)

	go func() { _, _ = node.far.Write([]byte("pong")) }()
	if _, err := conn.Read(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	waitChecks(t, provider, 1)
}
