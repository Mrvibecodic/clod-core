package outboundgroup

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/sing/common/bufio"
)

func silentGroup() *GroupBase {
	return NewGroupBase(GroupBaseOption{Name: "auto", TestTimeout: 5000})
}

type askCounter struct{ n atomic.Int32 }

func (a *askCounter) ask() { a.n.Add(1) }

func (a *askCounter) want(t *testing.T, n int32) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if got := a.n.Load(); got != n {
		t.Fatalf("the group asked %d times, want %d", got, n)
	}
}

// born is when the connections of the tests were opened.
var born = time.Now()

func silent(gb *GroupBase, p C.Proxy, asks *askCounter, times int) {
	for i := 0; i < times; i++ {
		gb.noteSilence(p, born, time.Now(), asks.ask)
	}
}

// Connections left unanswered one after another have the group ask about the
// node once: at the third, and not again while it is asking.
func TestUnansweredConnectionsInARowHaveTheGroupAskOnce(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	silent(gb, node, asks, silenceCount-1)
	asks.want(t, 0)
	silent(gb, node, asks, 1)
	asks.want(t, 1)
	silent(gb, node, asks, 2*silenceCount)
	asks.want(t, 1)
}

// A connection through the node that got its answer after the silent one
// sent its request says the node passes data: it is the site that is silent.
func TestAnAnswerThroughTheNodeClearsTheSilence(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	sent := time.Now()
	// The clock of some systems is coarse.
	time.Sleep(time.Millisecond)
	gb.noteAnswer(node, born)
	for i := 0; i < 2*silenceCount; i++ {
		gb.noteSilence(node, born, sent, asks.ask)
	}
	asks.want(t, 0)

	silent(gb, node, asks, silenceCount-1)
	gb.noteAnswer(node, born)
	silent(gb, node, asks, silenceCount-1)
	asks.want(t, 0)
}

// The silence is counted for the node the group is on: once a connection
// through another node is seen, the count starts anew for that node.
func TestSilenceIsCountedPerNode(t *testing.T) {
	gb, asks := silentGroup(), &askCounter{}
	a, b := &measuredProxy{name: "A", alive: true}, &measuredProxy{name: "B", alive: true}
	later := born.Add(time.Second)
	silent(gb, a, asks, silenceCount-1)
	for i := 0; i < silenceCount-1; i++ {
		gb.noteSilence(b, later, time.Now(), asks.ask)
	}
	asks.want(t, 0)
	gb.noteSilence(b, later, time.Now(), asks.ask)
	asks.want(t, 1)
}

// A group that comes back to a node counts its new connections: they are
// newer than those of the node in between.
func TestAGroupBackOnANodeCountsItsNewConnections(t *testing.T) {
	gb, asks := silentGroup(), &askCounter{}
	a, b := &measuredProxy{name: "A", alive: true}, &measuredProxy{name: "B", alive: true}
	silent(gb, a, asks, 1)
	gb.noteSilence(b, born.Add(time.Second), time.Now(), asks.ask)
	for i := 0; i < silenceCount; i++ {
		gb.noteSilence(a, born.Add(2*time.Second), time.Now(), asks.ask)
	}
	asks.want(t, 1)
}

// With nobody answering the network was down, and the group waits before it
// asks again; the first answer through the node says the network is back and
// ends the wait.
func TestAnAnswerEndsTheWaitOfALostNetwork(t *testing.T) {
	gb, node := silentGroup(), &measuredProxy{name: "A", alive: true}
	windows := func() (silence, failures bool) {
		gb.silence.mu.Lock()
		defer gb.silence.mu.Unlock()
		now := time.Now()
		return now.Before(gb.silence.quietUntil), now.Before(gb.silence.failedQuietUntil)
	}
	gb.noteSilence(node, born, time.Now(), func() {})
	gb.asked(node, verdictNoNetwork, false)
	if silence, failures := windows(); !silence || !failures {
		t.Fatalf("no wait after nobody answered: %v, %v", silence, failures)
	}
	gb.noteAnswer(node, born)
	if silence, failures := windows(); silence || failures {
		t.Fatalf("the wait outlived the answer: %v, %v", silence, failures)
	}

	// The wait of a silent site is not the network's: it stays.
	gb.asked(node, verdictAlive, false)
	gb.noteAnswer(node, born)
	if silence, _ := windows(); !silence {
		t.Fatal("an answer ended the wait of a silent site")
	}
}

// What a check found out of a node is not written to the node the group has
// gone to while the check ran.
func TestTheEndOfACheckLeavesTheNextNodeAlone(t *testing.T) {
	gb, asks := silentGroup(), &askCounter{}
	a, b := &measuredProxy{name: "A", alive: true}, &measuredProxy{name: "B", alive: true}
	silent(gb, a, asks, silenceCount)
	asks.want(t, 1)
	gb.noteSilence(b, born.Add(time.Second), time.Now(), asks.ask)
	gb.asked(a, verdictNoNetwork, false)
	gb.silence.mu.Lock()
	asking, quiet, lost := gb.silence.asking, time.Now().Before(gb.silence.quietUntil), gb.silence.noNetwork
	gb.silence.mu.Unlock()
	if asking {
		t.Fatal("the check did not end")
	}
	if quiet || lost {
		t.Fatalf("the wait of the node left was put on the next one: quiet %v, lost network %v", quiet, lost)
	}
}

// The pause grown over one node is not handed to the next one.
func TestThePauseIsOfTheNodeItWasGrownOver(t *testing.T) {
	gb, asks := silentGroup(), &askCounter{}
	a, b := &measuredProxy{name: "A", alive: true}, &measuredProxy{name: "B", alive: true}
	silent(gb, a, asks, silenceCount)
	asks.want(t, 1)
	gb.asked(a, verdictAlive, false)
	later := born.Add(time.Second)
	for i := 0; i < silenceCount; i++ {
		gb.noteSilence(b, later, time.Now(), asks.ask)
	}
	asks.want(t, 2)
}

// The connections of a node the group has left were opened before those of
// the node it is on: their silence is not counted and their answers clear
// nothing.
func TestConnectionsOfANodeTheGroupHasLeftAreNotCounted(t *testing.T) {
	gb, asks := silentGroup(), &askCounter{}
	a, b := &measuredProxy{name: "A", alive: true}, &measuredProxy{name: "B", alive: true}
	later := born.Add(time.Second)
	silent(gb, a, asks, 1)
	gb.noteSilence(b, later, time.Now(), asks.ask)
	silent(gb, a, asks, 2*silenceCount)
	asks.want(t, 0)

	gb.noteSilence(b, later, time.Now(), asks.ask)
	gb.noteAnswer(a, born)
	gb.noteSilence(b, later, time.Now(), asks.ask)
	asks.want(t, 1)
}

// A node found alive is not asked about again for the pause, the pause doubles
// with every such check in a row up to its limit, and an answer through the
// node starts it over. A node found dead leaves no pause.
func TestThePauseAfterANodeFoundAliveGrows(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	pause := func() time.Duration {
		gb.silence.mu.Lock()
		defer gb.silence.mu.Unlock()
		return time.Until(gb.silence.quietUntil).Round(time.Second)
	}
	for i, want := range []time.Duration{silencePause, 2 * silencePause, silencePauseMax, silencePauseMax} {
		silent(gb, node, asks, silenceCount)
		asks.want(t, int32(i+1))
		gb.asked(node, verdictAlive, false)
		if got := pause(); got != want {
			t.Fatalf("pause %d is %s, want %s", i+1, got, want)
		}
		silent(gb, node, asks, silenceCount)
		asks.want(t, int32(i+1))
		gb.silence.mu.Lock()
		gb.silence.quietUntil = time.Now()
		gb.silence.mu.Unlock()
	}

	gb.noteAnswer(node, born)
	silent(gb, node, asks, silenceCount)
	asks.want(t, 5)
	gb.asked(node, verdictAlive, false)
	if got := pause(); got != silencePause {
		t.Fatalf("pause after an answer is %s, want %s", got, silencePause)
	}

	gb.silence.mu.Lock()
	gb.silence.quietUntil = time.Now()
	gb.silence.mu.Unlock()
	before := asks.n.Load()
	silent(gb, node, asks, silenceCount)
	gb.asked(node, verdictDead, false)
	silent(gb, node, asks, silenceCount)
	asks.want(t, before+2)
}

func watchedPair(t *testing.T, gb *GroupBase, node C.Proxy, asks *askCounter) (C.Conn, net.Conn) {
	t.Helper()
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	conn := &watchedConn{Conn: outbound.NewConn(near, outbound.NewDirect()), gb: gb, node: node, born: time.Now(), ask: asks.ask}
	go func() {
		buf := make([]byte, 16)
		_, _ = far.Read(buf)
	}()
	return conn, far
}

// A connection that sent its request and got no byte back in silenceAfter is
// one the node left unanswered; one that got its answer, or was closed while
// it waited, is not.
func TestAConnectionWithoutAByteBackCountsAsSilence(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	for i := 0; i < silenceCount; i++ {
		conn, _ := watchedPair(t, gb, node, asks)
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
	}
	asks.want(t, 0)
	time.Sleep(silenceAfter + 200*time.Millisecond)
	asks.want(t, 1)
}

func TestAnAnsweredConnectionIsNoSilence(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	for i := 0; i < silenceCount; i++ {
		conn, far := watchedPair(t, gb, node, asks)
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = far.Write([]byte("pong")) }()
		if _, err := conn.Read(make([]byte, 16)); err != nil {
			t.Fatal(err)
		}
	}
	// The answers are forgotten: only the stopped watches keep the count at
	// nought.
	gb.silence.mu.Lock()
	gb.silence.answeredAt = time.Time{}
	gb.silence.mu.Unlock()
	time.Sleep(silenceAfter + 200*time.Millisecond)
	asks.want(t, 0)
}

func TestAClosedConnectionIsNoSilence(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	for i := 0; i < silenceCount; i++ {
		conn, _ := watchedPair(t, gb, node, asks)
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
	time.Sleep(silenceAfter + 200*time.Millisecond)
	asks.want(t, 0)
}

// An empty write only has the node open its connection for a client that is
// yet to say anything: no request went, and no answer is waited for.
func TestAnEmptyWriteStartsNoWatch(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	for i := 0; i < silenceCount; i++ {
		conn, _ := watchedPair(t, gb, node, asks)
		if _, err := conn.Write(nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(silenceAfter + 200*time.Millisecond)
	asks.want(t, 0)
}

// countedConn counts the reads that go through the watch.
type countedConn struct {
	*watchedConn
	reads atomic.Int32
}

func (c *countedConn) Read(b []byte) (int, error) {
	c.reads.Add(1)
	return c.watchedConn.Read(b)
}

func (c *countedConn) ReadBuffer(buffer *buf.Buffer) error {
	c.reads.Add(1)
	return c.watchedConn.ReadBuffer(buffer)
}

// The watch needs the first write and the first answer only: after them a
// copy goes past it, straight to the connection of the node, in both
// directions.
func TestACopyGoesPastTheWatchAfterTheFirstAnswer(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	conn := &watchedConn{Conn: outbound.NewConn(near, outbound.NewDirect()), gb: gb, node: node, born: time.Now(), ask: asks.ask}

	if N.UnwrapReader(conn) != io.Reader(conn) || N.UnwrapWriter(conn) != io.Writer(conn) {
		t.Fatal("the watch was passed before it saw anything")
	}
	if !conn.ReaderPossiblyReplaceable() || !conn.WriterPossiblyReplaceable() {
		t.Fatal("a copy is not told to ask again")
	}

	go func() {
		buf := make([]byte, 16)
		_, _ = far.Read(buf)
		_, _ = far.Write([]byte("pong"))
	}()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if N.UnwrapReader(conn) == io.Reader(conn) || N.UnwrapWriter(conn) == io.Writer(conn) {
		t.Fatal("the watch stays in the way of the data after the first answer")
	}
	if conn.ReaderPossiblyReplaceable() || conn.WriterPossiblyReplaceable() {
		t.Fatal("a copy is told to ask again though the watch is over")
	}
}

// A copy that starts before any data went still gets past the watch: it is
// told to ask again after the first piece.
func TestACopyStartedEarlyGetsPastTheWatch(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	conn := &countedConn{watchedConn: &watchedConn{Conn: outbound.NewConn(near, outbound.NewDirect()), gb: gb, node: node, born: time.Now(), ask: asks.ask}}

	go func() {
		for i := 0; i < 5; i++ {
			_, _ = far.Write([]byte("pong"))
		}
		_ = far.Close()
	}()
	if _, err := bufio.Copy(io.Discard, conn); err != nil {
		t.Fatal(err)
	}
	if got := conn.reads.Load(); got != 1 {
		t.Fatalf("%d reads went through the watch, want the first one only", got)
	}
}
