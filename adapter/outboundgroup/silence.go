package outboundgroup

import (
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

const (
	// silenceAfter is how long a connection that sent its request may wait
	// for the first byte back before it counts as one the node left
	// unanswered.
	silenceAfter = 3 * time.Second
	// silenceCount is how many connections in a row the node leaves
	// unanswered before the group asks about it.
	silenceCount = 3
	// silencePause is how long the group does not ask again after it asked
	// and the node was alive: a site that does not answer costs a check, not
	// a stream of them. Every such check in a row doubles the pause.
	silencePause    = 30 * time.Second
	silencePauseMax = 2 * time.Minute
)

// silence is what a group knows of the connections its node left unanswered.
// A node that takes connections and passes no data fails no dial, so the
// failures of connections never have it checked.
type silence struct {
	mu sync.Mutex
	// node is the node whose connections are counted, since is when the
	// first of them was opened: the connections of a node the group has left
	// are older, and what becomes of them says nothing of the node of now.
	node       C.Proxy
	since      time.Time
	count      int
	answeredAt time.Time
	asking     bool
	pause      time.Duration
	// failedPause is the wait after failed connections asked about a node
	// that then answered: short, a failed connection is the node's own word,
	// and longer each time in a row, a node that answers every time is not
	// asked about every few seconds.
	failedPause time.Duration
	// noNetwork: the windows below were set because nobody answered. An
	// answer through the node ends them: the network is back.
	noNetwork bool
	// quietUntil is when unanswered connections may ask about the node
	// again, failedQuietUntil when failed ones may: a site that keeps silent
	// must not keep a node that refuses connections from being asked about.
	quietUntil       time.Time
	failedQuietUntil time.Time
}

// follows reports whether the connection opened at born through p is one of
// the node the group is on, taking p for that node if it is newer.
func (s *silence) follows(p C.Proxy, born time.Time) bool {
	if s.node == p {
		return true
	}
	if !born.After(s.since) {
		return false
	}
	// What was found out of the node before says nothing of this one.
	s.node, s.since, s.count, s.answeredAt = p, born, 0, time.Time{}
	s.pause, s.failedPause, s.quietUntil, s.failedQuietUntil, s.noNetwork = 0, 0, time.Time{}, time.Time{}, false
	return true
}

// doubt is how a group asks about a node its connections give it doubts of.
type doubt struct {
	url      string
	expected string
	// next is the nodes the group would go to from the given one, best first.
	next func(but C.Proxy) []C.Proxy
	// all checks every node of the group.
	all func()
	// hold keeps the group on its node whatever delays a check has left.
	hold func()
	// again has the group make its choice anew.
	again func()
}

// watchedConn watches a connection of the group until its first byte back.
type watchedConn struct {
	C.Conn
	gb   *GroupBase
	node C.Proxy
	born time.Time
	ask  func()
	// answered is told of the first byte back, nil if nobody waits for it.
	answered func()

	wrote   atomic.Bool
	settled atomic.Bool
	mu      sync.Mutex
	timer   *time.Timer
	sent    time.Time
}

func (c *watchedConn) sentOnce() {
	if !c.wrote.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.sent = time.Now()
	if !c.settled.Load() {
		c.timer = time.AfterFunc(silenceAfter, func() {
			if !c.settled.Load() {
				c.gb.noteSilence(c.node, c.born, c.sent, c.ask)
			}
		})
	}
	c.mu.Unlock()
}

// settle ends the watch: the node answered, or the connection ended some
// other way, which says nothing of the node.
func (c *watchedConn) settle(answered bool) {
	if !c.settled.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
	if answered {
		c.gb.noteAnswer(c.node, c.born)
		if c.answered != nil {
			c.answered()
		}
	}
}

// An empty write sends no request: it only has the node open its
// connection, for a client that is yet to say anything.
func (c *watchedConn) Write(b []byte) (int, error) {
	if len(b) > 0 {
		c.sentOnce()
	}
	return c.Conn.Write(b)
}

func (c *watchedConn) WriteBuffer(buffer *buf.Buffer) error {
	if !buffer.IsEmpty() {
		c.sentOnce()
	}
	return c.Conn.WriteBuffer(buffer)
}

func (c *watchedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 || err != nil {
		c.settle(n > 0)
	}
	return
}

func (c *watchedConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = c.Conn.ReadBuffer(buffer)
	if err != nil || !buffer.IsEmpty() {
		c.settle(!buffer.IsEmpty())
	}
	return
}

func (c *watchedConn) Close() error {
	c.settle(false)
	return c.Conn.Close()
}

func (c *watchedConn) Upstream() any { return c.Conn }

// The watch needs the first write and the first read only. A copy asks once,
// before any data went, whether it may go past the wrapper: answered no, it
// would keep every byte of the connection going through here, and a node that
// hands its stream over to the socket would never get to. So the wrapper says
// it may be passed soon, and the copy asks again after each piece.
func (c *watchedConn) ReaderReplaceable() bool         { return c.settled.Load() }
func (c *watchedConn) ReaderPossiblyReplaceable() bool { return !c.settled.Load() }
func (c *watchedConn) WriterReplaceable() bool         { return c.wrote.Load() }
func (c *watchedConn) WriterPossiblyReplaceable() bool { return !c.wrote.Load() }

var _ N.ExtendedConn = (*watchedConn)(nil)

// watch has the group notice a node that takes connections and passes no
// data: silenceCount connections through p in a row left without a byte back
// have the group ask about p. answered, if any, is told of the first byte
// back: a connection that opened says little, many nodes open one without a
// packet sent, and it is the answer that shows the node carries traffic.
//
// born is when the group took p for the connection: one that was long in
// opening is no newer for that than those opened after it.
func (gb *GroupBase) watch(c C.Conn, p C.Proxy, born time.Time, d doubt, answered func()) C.Conn {
	if !carries(p) {
		if answered != nil {
			answered()
		}
		return c
	}
	return &watchedConn{Conn: c, gb: gb, node: p, born: born, answered: answered, ask: func() { gb.askOf(p, d, false) }}
}

// carries reports whether p is a server of its own: the built-in ways out
// are not asked about.
func carries(p C.Proxy) bool {
	switch p.Type() {
	case C.Direct, C.Compatible, C.Reject, C.Pass, C.RejectDrop:
		return false
	}
	return true
}

func (gb *GroupBase) noteAnswer(p C.Proxy, born time.Time) {
	s := &gb.silence
	s.mu.Lock()
	if s.follows(p, born) {
		s.answeredAt, s.count, s.pause, s.failedPause = time.Now(), 0, 0, 0
		if s.noNetwork {
			s.noNetwork, s.quietUntil, s.failedQuietUntil = false, time.Time{}, time.Time{}
		}
	}
	s.mu.Unlock()
}

func (gb *GroupBase) noteSilence(p C.Proxy, born, sent time.Time, ask func()) {
	s := &gb.silence
	s.mu.Lock()
	// An answer through the node after this request went out: the node
	// passes data, it is the site that is silent.
	if !s.follows(p, born) || s.answeredAt.After(sent) {
		s.mu.Unlock()
		return
	}
	s.count++
	start := s.count >= silenceCount && !s.asking && !time.Now().Before(s.quietUntil)
	if start {
		s.count, s.asking = 0, true
	}
	s.mu.Unlock()
	if start {
		go ask()
	}
}

// verdict is what asking about a node came to.
type verdict int

const (
	// verdictNone: nothing was found out, the node is asked about again soon.
	verdictNone verdict = iota
	// verdictAlive: the node answered.
	verdictAlive
	// verdictDead: the node is marked dead.
	verdictDead
	// verdictNoNetwork: neither the node nor the nodes beside it answered.
	verdictNoNetwork
)

// asked ends a check. After one that left the group where it was the node is
// not asked about again for the pause, and the pause doubles until a
// connection through the node gets an answer:
//   - the node answered its probe while its connections stayed unanswered:
//     the site is silent, and more silence does not ask again; failed
//     connections are the node's own word and do, after a wait of their own
//     that starts short;
//   - nobody answered: the network is down, and neither silence nor failures
//     ask again.
func (gb *GroupBase) asked(v verdict, failed bool) {
	s := &gb.silence
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asking = false
	now := time.Now()
	grown := func() time.Time {
		if s.pause == 0 {
			s.pause = silencePause
		}
		until := now.Add(s.pause)
		if s.pause *= 2; s.pause > silencePauseMax {
			s.pause = silencePauseMax
		}
		return until
	}
	s.noNetwork = v == verdictNoNetwork
	switch {
	case v == verdictDead:
		s.pause, s.failedPause, s.quietUntil, s.failedQuietUntil = 0, 0, time.Time{}, time.Time{}
	case v == verdictNoNetwork:
		until := grown()
		s.quietUntil, s.failedQuietUntil = until, until
	case v == verdictAlive && !failed:
		s.quietUntil = grown()
	case failed:
		// The node answered, or nothing was found out: failed connections
		// ask again after a wait of their own.
		if s.failedPause == 0 {
			s.failedPause = silenceAfter
		}
		s.failedQuietUntil = now.Add(s.failedPause)
		if s.failedPause *= 2; s.failedPause > silencePause {
			s.failedPause = silencePause
		}
	default:
		s.quietUntil = now.Add(silenceAfter)
	}
}

// askOf answers the connections p left unanswered: it has every node of the
// group checked.
func (gb *GroupBase) askOf(p C.Proxy, d doubt, failed bool) {
	// The connections opened before p was marked dead go silent after it.
	if !p.AliveForTestUrl(d.url) || !gb.failedTesting.CompareAndSwap(false, true) {
		gb.asked(verdictNone, failed)
		return
	}
	seen := lastProbe(p, d.url)
	gb.failedTesting.Store(false)
	d.all()
	switch {
	case lastProbe(p, d.url).Equal(seen):
		// Another check of the group was running: this one did not start.
		gb.asked(verdictNone, failed)
	case p.AliveForTestUrl(d.url):
		gb.asked(verdictAlive, failed)
	default:
		gb.asked(verdictDead, failed)
	}
}
