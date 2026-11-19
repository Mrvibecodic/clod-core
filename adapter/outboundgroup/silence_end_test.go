package outboundgroup

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// endedConn is a stream whose read waits until it is ended: by the far side
// or a layer below (end), or by its own Close, which ends the read with the
// error the transport gives then.
type endedConn struct {
	net.Conn
	once  sync.Once
	ended chan struct{}
	err   error
	onOwn error
}

func (c *endedConn) end(err error) {
	c.once.Do(func() { c.err = err; close(c.ended) })
}

func (c *endedConn) Read([]byte) (int, error) {
	<-c.ended
	return 0, c.err
}

func (c *endedConn) Close() error {
	c.end(c.onOwn)
	return c.Conn.Close()
}

// endedConns opens n watched connections through node, each of which sent
// its request and waits on its read.
func endedConns(t *testing.T, gb *GroupBase, node C.Proxy, asks *askCounter, n int, onOwn error) ([]*endedConn, []*watchedConn) {
	t.Helper()
	var inner []*endedConn
	var watched []*watchedConn
	for i := 0; i < n; i++ {
		near, far := net.Pipe()
		t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
		go func() { _, _ = far.Read(make([]byte, 64)) }()
		c := &endedConn{Conn: near, ended: make(chan struct{}), onOwn: onOwn}
		w := &watchedConn{Conn: outbound.NewConn(c, outbound.NewDirect()), gb: gb, node: node, born: time.Now(), ask: asks.ask}
		if _, err := w.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = w.Read(make([]byte, 16)) }()
		inner, watched = append(inner, c), append(watched, w)
	}
	time.Sleep(20 * time.Millisecond)
	return inner, watched
}

// A stream the far side ended with net.ErrClosed, as some transports report
// the end of a stream by the server, is cut like one that got io.EOF.
func TestAStreamEndedByTheServerWithErrClosedIsACut(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	takenLongAgo(gb, node)
	inner, _ := endedConns(t, gb, node, asks, silenceCount, io.ErrClosedPipe)
	for _, c := range inner {
		c.end(net.ErrClosed)
	}
	asks.want(t, 1)
}

// A connection closed by its own Close counts nothing, whatever error its
// pending read gets then: the watch is settled before the close.
func TestOwnCloseCountsNothingWhateverTheReadGets(t *testing.T) {
	for _, onOwn := range []error{net.ErrClosed, context.Canceled, io.ErrClosedPipe, errors.New("http2: response body closed"), io.EOF} {
		gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
		takenLongAgo(gb, node)
		_, watched := endedConns(t, gb, node, asks, silenceCount, onOwn)
		for _, w := range watched {
			_ = w.Close()
		}
		asks.want(t, 0)
	}
}
