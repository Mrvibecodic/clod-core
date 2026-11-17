package tuic

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

type countedClient struct {
	streams atomic.Int64
	visited atomic.Pointer[time.Time]
	closed  atomic.Bool
}

func (c *countedClient) DialContext(context.Context, *C.Metadata) (net.Conn, error) { return nil, nil }
func (c *countedClient) ListenPacket(context.Context, *C.Metadata) (net.PacketConn, error) {
	return nil, nil
}
func (c *countedClient) OpenStreams() int64 { return c.streams.Load() }
func (c *countedClient) LastVisited() time.Time {
	if at := c.visited.Load(); at != nil {
		return *at
	}
	return time.Time{}
}
func (c *countedClient) SetLastVisited(at time.Time) { c.visited.Store(&at) }
func (c *countedClient) Close()                      { c.closed.Store(true) }

func closedSoon(c *countedClient) bool {
	for i := 0; i < 100 && !c.closed.Load(); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	return c.closed.Load()
}

// After Retire the pool hands out new connections: the ones it had are
// taken out of it, the stream connections and the packet ones alike.
func TestRetireTakesTheConnectionsOutOfThePool(t *testing.T) {
	pool := &PoolClient{}
	if pool.Retire() {
		t.Fatal("there was nothing to set aside yet")
	}
	pool.tcpClients.PushFront(&countedClient{})
	pool.udpClients.PushFront(&countedClient{})
	if !pool.Retire() {
		t.Fatal("the connections were not set aside")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 0 {
		t.Fatalf("left in the pool: %d and %d", pool.tcpClients.Len(), pool.udpClients.Len())
	}
}

// A connection set aside stays open while it carries a stream, and one taken
// a moment ago stays open though it shows no stream yet; each closes once it
// carries nothing and nobody is on the way to it.
func TestAConnectionSetAsideClosesWhenItCarriesNothing(t *testing.T) {
	busy, taken, idle := &countedClient{}, &countedClient{}, &countedClient{}
	busy.streams.Store(1)
	taken.SetLastVisited(time.Now())
	go closeWhenIdle([]Client{busy, taken, idle}, 10*time.Millisecond, 200*time.Millisecond, func() {})
	if !closedSoon(idle) {
		t.Fatal("the idle connection stayed open")
	}
	if busy.closed.Load() || taken.closed.Load() {
		t.Fatalf("closed: the busy one %v, the one just taken %v", busy.closed.Load(), taken.closed.Load())
	}
	if !closedSoon(taken) {
		t.Fatal("the connection nobody came to stayed open")
	}
	busy.streams.Store(0)
	if !closedSoon(busy) {
		t.Fatal("the connection stayed open after its last stream")
	}
}

// While two connections set aside still carry streams, Retire sets nothing
// more aside and the pool keeps the connection it has; once one of them
// closes, Retire works again.
func TestRetireKeepsAtMostTwoConnectionsSetAside(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 10 * time.Millisecond

	pool := &PoolClient{}
	first, second, third := &countedClient{}, &countedClient{}, &countedClient{}
	first.streams.Store(1)
	second.streams.Store(1)
	pool.tcpClients.PushFront(first)
	if !pool.Retire() {
		t.Fatal("the first connection was not set aside")
	}
	pool.tcpClients.PushFront(second)
	if !pool.Retire() {
		t.Fatal("the second connection was not set aside")
	}
	pool.tcpClients.PushFront(third)
	if pool.Retire() {
		t.Fatal("a third connection was set aside while two still carry streams")
	}
	if pool.tcpClients.Len() != 1 || third.closed.Load() {
		t.Fatal("the connection in the pool was taken out of it")
	}

	first.streams.Store(0)
	if !closedSoon(first) {
		t.Fatal("the connection set aside stayed open after its last stream")
	}
	deadline := time.Now().Add(time.Second)
	for !pool.Retire() {
		if time.Now().After(deadline) {
			t.Fatal("Retire did not work again after a connection set aside closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pool.tcpClients.Len() != 0 {
		t.Fatal("the connection was left in the pool")
	}
	second.streams.Store(0)
	third.streams.Store(0)
}

// A pool of more stream connections than the limit, with nothing set aside
// yet, is set aside whole; the packet connection beside it does not fit and
// stays in the pool. While those stream connections are still open, the
// stream connection that came after them is not set aside.
func TestRetireOfAPoolLargerThanTheLimitSetsItAsideOnce(t *testing.T) {
	pool := &PoolClient{}
	var streams []*countedClient
	for i := 0; i < retireLimit+1; i++ {
		c := &countedClient{}
		c.streams.Store(1)
		defer c.streams.Store(0)
		streams = append(streams, c)
		pool.tcpClients.PushFront(c)
	}
	packets := &countedClient{}
	pool.udpClients.PushFront(packets)
	if !pool.Retire() {
		t.Fatal("the stream connections were not set aside though nothing was set aside before")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 1 {
		t.Fatalf("left in the pool: %d and %d, want 0 and 1", pool.tcpClients.Len(), pool.udpClients.Len())
	}

	next := &countedClient{}
	next.streams.Store(1)
	defer next.streams.Store(0)
	pool.tcpClients.PushFront(next)
	if pool.Retire() {
		t.Fatal("more was set aside while the stream connections set aside still carry streams")
	}
	if pool.tcpClients.Len() != 1 || pool.udpClients.Len() != 1 {
		t.Fatalf("left in the pool: %d and %d, want 1 and 1", pool.tcpClients.Len(), pool.udpClients.Len())
	}
	time.Sleep(20 * time.Millisecond)
	for _, c := range append(streams, next, packets) {
		if c.closed.Load() {
			t.Fatal("a connection was closed under what it carries")
		}
	}
}

// The stream connections come first: with a stream connection and two packet
// ones, more than may be set aside together, the stream connection is set
// aside and the packet ones stay in the pool, open.
func TestRetireSetsTheStreamConnectionsAsideAndLeavesThePacketOnesThatDoNotFit(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 10 * time.Millisecond

	pool := &PoolClient{}
	stream, packets1, packets2 := &countedClient{}, &countedClient{}, &countedClient{}
	stream.streams.Store(1)
	defer stream.streams.Store(0)
	pool.tcpClients.PushFront(stream)
	pool.udpClients.PushFront(packets1)
	pool.udpClients.PushFront(packets2)
	if !pool.Retire() {
		t.Fatal("the stream connection was not set aside")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 2 {
		t.Fatalf("left in the pool: %d and %d, want 0 and 2", pool.tcpClients.Len(), pool.udpClients.Len())
	}
	time.Sleep(20 * time.Millisecond)
	if stream.closed.Load() || packets1.closed.Load() || packets2.closed.Load() {
		t.Fatal("a connection was closed under what it carries")
	}
}

// With one connection set aside and still busy, a stream connection still
// fits and a packet connection beside it does not: the stream connection is
// set aside, the packet one stays in the pool.
func TestRetireLeavesThePacketConnectionWhenOnlyTheStreamConnectionFits(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 10 * time.Millisecond

	pool := &PoolClient{}
	first, stream, packets := &countedClient{}, &countedClient{}, &countedClient{}
	first.streams.Store(1)
	defer first.streams.Store(0)
	stream.streams.Store(1)
	defer stream.streams.Store(0)
	pool.tcpClients.PushFront(first)
	if !pool.Retire() {
		t.Fatal("the first connection was not set aside")
	}
	pool.tcpClients.PushFront(stream)
	pool.udpClients.PushFront(packets)
	if !pool.Retire() {
		t.Fatal("the stream connection was not set aside")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 1 {
		t.Fatalf("left in the pool: %d and %d, want 0 and 1", pool.tcpClients.Len(), pool.udpClients.Len())
	}
	if stream.closed.Load() || packets.closed.Load() {
		t.Fatal("a connection was closed under what it carries")
	}
}

// A packet connection that outlives the stream connection set aside with it
// does not keep the next stream connection from being set aside: the stream
// connection goes, the new packet connection stays while it does not fit.
func TestRetireOfTheNextStreamConnectionIsNotBlockedByALongPacketConnection(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 10 * time.Millisecond

	pool := &PoolClient{}
	stream, packets := &countedClient{}, &countedClient{}
	stream.streams.Store(1)
	packets.streams.Store(1)
	defer packets.streams.Store(0)
	pool.tcpClients.PushFront(stream)
	pool.udpClients.PushFront(packets)
	if !pool.Retire() {
		t.Fatal("the stream and packet connections were not set aside")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 0 {
		t.Fatalf("left in the pool: %d and %d, want 0 and 0", pool.tcpClients.Len(), pool.udpClients.Len())
	}
	stream.streams.Store(0)
	if !closedSoon(stream) {
		t.Fatal("the stream connection set aside stayed open after its last stream")
	}
	deadline := time.Now().Add(time.Second)
	for {
		retiredMu.Lock()
		n := retired[pool]
		retiredMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections count as set aside, want 1", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	stream2, packets2 := &countedClient{}, &countedClient{}
	stream2.streams.Store(1)
	defer stream2.streams.Store(0)
	packets2.streams.Store(1)
	defer packets2.streams.Store(0)
	pool.tcpClients.PushFront(stream2)
	pool.udpClients.PushFront(packets2)
	if !pool.Retire() {
		t.Fatal("the new stream connection was not set aside while the old packet connection lives")
	}
	if pool.tcpClients.Len() != 0 || pool.udpClients.Len() != 1 || pool.udpClients.Front().Value != packets2 {
		t.Fatalf("left in the pool: %d and %d, want 0 and the new packet connection", pool.tcpClients.Len(), pool.udpClients.Len())
	}
	if stream2.closed.Load() || packets2.closed.Load() {
		t.Fatal("a connection was closed under what it carries")
	}
}

// A stream and a packet connection are set aside together; while both still
// carry something, the two that came after them are not.
func TestRetireOfTwoConnectionsLeavesNoRoomForMore(t *testing.T) {
	pool := &PoolClient{}
	stream, packets := &countedClient{}, &countedClient{}
	stream.streams.Store(1)
	packets.streams.Store(1)
	defer stream.streams.Store(0)
	defer packets.streams.Store(0)
	pool.tcpClients.PushFront(stream)
	pool.udpClients.PushFront(packets)
	if !pool.Retire() {
		t.Fatal("the two connections were not set aside")
	}
	pool.tcpClients.PushFront(&countedClient{})
	pool.udpClients.PushFront(&countedClient{})
	if pool.Retire() {
		t.Fatal("two more connections were set aside while two still carry streams")
	}
	if pool.tcpClients.Len() != 1 || pool.udpClients.Len() != 1 {
		t.Fatalf("left in the pool: %d and %d, want 1 and 1", pool.tcpClients.Len(), pool.udpClients.Len())
	}
}
