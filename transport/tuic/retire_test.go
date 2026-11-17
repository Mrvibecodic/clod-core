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
	go closeWhenIdle([]Client{busy, taken, idle}, 10*time.Millisecond, 200*time.Millisecond)
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
