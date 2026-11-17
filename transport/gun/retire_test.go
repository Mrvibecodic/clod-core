package gun

import (
	"context"
	"io"
	"net"
	stdhttp "net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type watchedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *watchedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// dialedConns keeps the connections a client dials: the dial runs on the
// client's side while the test reads them.
type dialedConns struct {
	mu    sync.Mutex
	conns []*watchedConn
}

func (d *dialedConns) add(c *watchedConn) {
	d.mu.Lock()
	d.conns = append(d.conns, c)
	d.mu.Unlock()
}

func (d *dialedConns) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

func (d *dialedConns) at(i int) *watchedConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conns[i]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// After Retire the next stream goes over a new connection, the stream of the
// old one goes on, and the old connection closes when that stream ends.
func TestRetireOpensANewConnectionAndSeesTheOldOneOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var streams atomic.Int32
	srv := &stdhttp.Server{Handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		streams.Add(1)
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	})}
	srv.Protocols = new(stdhttp.Protocols)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go srv.Serve(ln)
	defer srv.Close()

	var conns dialedConns
	client := NewClient(func() *Transport {
		return NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
			if err != nil {
				return nil, err
			}
			w := &watchedConn{Conn: c}
			conns.add(w)
			return w, nil
		}, nil, &Config{Host: "example.com"})
	}, 0, 0, 0)
	defer client.Close()

	if client.Retire() {
		t.Fatal("there was nothing to set aside yet")
	}
	first, err := client.Dial()
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first stream did not reach the server", func() bool { return streams.Load() == 1 })

	if !client.Retire() {
		t.Fatal("the connection in use was not set aside")
	}
	second, err := client.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitFor(t, "the second stream did not reach the server", func() bool { return streams.Load() == 2 })
	if conns.len() != 2 {
		t.Fatalf("the stream after Retire went over %d connections, want a second one", conns.len())
	}
	if conns.at(0).closed.Load() {
		t.Fatal("the connection set aside was closed under its stream")
	}

	_ = first.Close()
	waitFor(t, "the connection set aside stayed open after its last stream", func() bool { return conns.at(0).closed.Load() })
	if conns.at(1).closed.Load() {
		t.Fatal("the new connection was closed too")
	}
}

// A stream that took the connection just before it was set aside still
// opens on it: a connection seen without streams once is not closed yet, and
// the stream that came meanwhile keeps it open.
func TestAStreamOnItsWayToARetiredConnectionOpens(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var streams atomic.Int32
	srv := &stdhttp.Server{Handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		streams.Add(1)
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	})}
	srv.Protocols = new(stdhttp.Protocols)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go srv.Serve(ln)
	defer srv.Close()

	client := NewClient(func() *Transport {
		return NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		}, nil, &Config{Host: "example.com"})
	}, 0, 0, 0)
	defer client.Close()

	taken := client.getTransport()
	const step = 100 * time.Millisecond
	go taken.closeWhenIdle(step)
	// Seen without streams once by now.
	time.Sleep(step + step/2)
	conn, err := taken.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() { _, _ = conn.Write([]byte{0}) }()
	waitFor(t, "the stream on its way did not reach the server", func() bool { return streams.Load() == 1 })
	time.Sleep(3 * step)
	if taken.ctx.Err() != nil {
		t.Fatal("the connection was closed under the stream that came to it")
	}
	_ = conn.Close()
	waitFor(t, "the connection stayed open after its last stream", func() bool { return taken.ctx.Err() != nil })
}

// While two connections set aside still carry streams, Retire sets nothing
// more aside and the next stream goes over the current connection; once one
// of them closes, Retire works again.
func TestRetireKeepsAtMostTwoConnectionsSetAside(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 20 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var streams atomic.Int32
	srv := &stdhttp.Server{Handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		streams.Add(1)
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	})}
	srv.Protocols = new(stdhttp.Protocols)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go srv.Serve(ln)
	defer srv.Close()

	var dials atomic.Int32
	var conns dialedConns
	client := NewClient(func() *Transport {
		return NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
			if err != nil {
				return nil, err
			}
			w := &watchedConn{Conn: c}
			conns.add(w)
			dials.Add(1)
			return w, nil
		}, nil, &Config{Host: "example.com"})
	}, 0, 0, 0)
	defer client.Close()

	open := func(n int32) net.Conn {
		t.Helper()
		conn, err := client.Dial()
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the stream did not reach the server", func() bool { return streams.Load() == n })
		return conn
	}
	first := open(1)
	if !client.Retire() {
		t.Fatal("the first connection was not set aside")
	}
	second := open(2)
	defer second.Close()
	if !client.Retire() {
		t.Fatal("the second connection was not set aside")
	}
	third := open(3)
	defer third.Close()
	if client.Retire() {
		t.Fatal("a third connection was set aside while two still carry streams")
	}
	fourth := open(4)
	defer fourth.Close()
	if dials.Load() != 3 {
		t.Fatalf("the stream after a refused Retire went over connection %d, want the third", dials.Load())
	}

	_ = first.Close()
	waitFor(t, "the connection set aside stayed open after its last stream", func() bool { return conns.at(0).closed.Load() })
	waitFor(t, "Retire did not work again after a connection set aside closed", client.Retire)
	fifth := open(5)
	defer fifth.Close()
	if dials.Load() != 4 {
		t.Fatalf("the stream after Retire went over %d connections, want a fourth one", dials.Load())
	}
}

// serveStreams runs a server that keeps every stream open until the client
// ends it, and counts the streams that reached it.
func serveStreams(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	streams := new(atomic.Int32)
	srv := &stdhttp.Server{Handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		streams.Add(1)
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	})}
	srv.Protocols = new(stdhttp.Protocols)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go srv.Serve(ln)
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String(), streams
}

// newWatchedClient makes a client of up to maxConnections connections to
// addr and keeps each connection it dials in conns.
func newWatchedClient(addr string, maxConnections int, conns *dialedConns) *Client {
	return NewClient(func() *Transport {
		return NewTransport(func(ctx context.Context, network, _ string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			w := &watchedConn{Conn: c}
			conns.add(w)
			return w, nil
		}, nil, &Config{Host: "example.com"})
	}, maxConnections, 0, 0)
}

// A pool of more connections carrying streams than the limit, with nothing set
// aside yet, is set aside whole: the next streams go over new connections and
// the old ones stay open under their streams. While those are still open, the
// connection that came after them is not set aside.
func TestRetireOfAPoolLargerThanTheLimitSetsItAsideOnce(t *testing.T) {
	addr, streams := serveStreams(t)
	var conns dialedConns
	client := newWatchedClient(addr, retireLimit+1, &conns)
	defer client.Close()

	open := func(n int32) {
		t.Helper()
		conn, err := client.Dial()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		waitFor(t, "the stream did not reach the server", func() bool { return streams.Load() == n })
	}
	for i := int32(1); i <= retireLimit+1; i++ {
		open(i)
	}
	if conns.len() != retireLimit+1 {
		t.Fatalf("the pool holds %d connections, want %d", conns.len(), retireLimit+1)
	}
	if !client.Retire() {
		t.Fatal("the pool was not set aside though nothing was set aside before")
	}
	client.mutex.Lock()
	current := len(client.transports)
	client.mutex.Unlock()
	if current != 0 {
		t.Fatalf("%d connections are still current after Retire", current)
	}

	open(retireLimit + 2)
	if conns.len() != retireLimit+2 {
		t.Fatalf("the stream after Retire went over %d connections, want a new one", conns.len())
	}
	if client.Retire() {
		t.Fatal("more was set aside while the pool set aside still carries streams")
	}
	client.mutex.Lock()
	current = len(client.transports)
	client.mutex.Unlock()
	if current != 1 {
		t.Fatalf("%d connections are current after a refused Retire, want 1", current)
	}
	for i := 0; i < conns.len(); i++ {
		if conns.at(i).closed.Load() {
			t.Fatalf("connection %d was closed under its stream", i)
		}
	}
}

// A pool of two connections is set aside whole; while both still carry
// streams, the two connections that came after them are not.
func TestRetireOfTwoConnectionsLeavesNoRoomForMore(t *testing.T) {
	addr, streams := serveStreams(t)
	var conns dialedConns
	client := newWatchedClient(addr, retireLimit, &conns)
	defer client.Close()

	open := func(n int32) {
		t.Helper()
		conn, err := client.Dial()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		waitFor(t, "the stream did not reach the server", func() bool { return streams.Load() == n })
	}
	open(1)
	open(2)
	if !client.Retire() {
		t.Fatal("the two connections were not set aside")
	}
	open(3)
	open(4)
	if conns.len() != 4 {
		t.Fatalf("the streams after Retire went over %d connections, want four", conns.len())
	}
	if client.Retire() {
		t.Fatal("two more connections were set aside while two still carry streams")
	}
	open(5)
	if conns.len() != 4 {
		t.Fatal("the stream after a refused Retire went over a new connection")
	}
}

// A pool of more connections than the limit is set aside whole when only some
// of them carry streams: the idle ones close at once, the busy one stays open
// until its stream ends.
func TestRetireOfAPoolWhereOnlyOneConnectionCarriesAStreamSetsItAllAside(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 20 * time.Millisecond

	addr, streams := serveStreams(t)
	var conns dialedConns
	client := newWatchedClient(addr, retireLimit+1, &conns)
	defer client.Close()

	var held []net.Conn
	for i := int32(1); i <= retireLimit+1; i++ {
		conn, err := client.Dial()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
		n := i
		waitFor(t, "the stream did not reach the server", func() bool { return streams.Load() == n })
	}
	defer held[0].Close()
	if conns.len() != retireLimit+1 {
		t.Fatalf("the pool holds %d connections, want %d", conns.len(), retireLimit+1)
	}
	client.mutex.Lock()
	pool := append([]*Transport(nil), client.transports...)
	client.mutex.Unlock()
	for _, conn := range held[1:] {
		_ = conn.Close()
	}
	waitFor(t, "the closed streams still count", func() bool {
		return pool[0].count.Load() == 1 && pool[1].count.Load() == 0 && pool[2].count.Load() == 0
	})

	if !client.Retire() {
		t.Fatal("the pool was not set aside though only one connection carries a stream")
	}
	client.mutex.Lock()
	current := len(client.transports)
	client.mutex.Unlock()
	if current != 0 {
		t.Fatalf("%d connections are still current after Retire", current)
	}
	waitFor(t, "the idle connections set aside stayed open", func() bool {
		return conns.at(1).closed.Load() && conns.at(2).closed.Load()
	})
	time.Sleep(5 * retireStep)
	if conns.at(0).closed.Load() {
		t.Fatal("the connection set aside was closed under its stream")
	}
	_ = held[0].Close()
	waitFor(t, "the connection set aside stayed open after its last stream", func() bool { return conns.at(0).closed.Load() })
}
