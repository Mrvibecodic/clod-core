package gun

import (
	"context"
	"io"
	"net"
	stdhttp "net/http"
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

	var conns []*watchedConn
	client := NewClient(func() *Transport {
		return NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
			if err != nil {
				return nil, err
			}
			w := &watchedConn{Conn: c}
			conns = append(conns, w)
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
	if len(conns) != 2 {
		t.Fatalf("the stream after Retire went over %d connections, want a second one", len(conns))
	}
	if conns[0].closed.Load() {
		t.Fatal("the connection set aside was closed under its stream")
	}

	_ = first.Close()
	waitFor(t, "the connection set aside stayed open after its last stream", func() bool { return conns[0].closed.Load() })
	if conns[1].closed.Load() {
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
