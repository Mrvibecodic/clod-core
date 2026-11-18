package outboundgroup

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
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
	// timed is how many of the connections counted ran out their wait, as
	// opposed to being cut. A check started by cut connections alone (byCuts)
	// that finds the node alive holds back cut connections only, until
	// cutQuietUntil: they were the server refusing sites, and they say
	// nothing of whether the node goes silent after. A check started by
	// silence while that pause runs (early) takes the next pause in advance,
	// so the node is asked about no more often than before.
	timed         int
	byCuts, early bool
	cutQuietUntil time.Time
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
	s.timed, s.cutQuietUntil = 0, time.Time{}
	return true
}

// silenceWitnesses is how many other nodes are asked beside the one in doubt;
// silenceMoreWitnesses is how many more are, when none of those answered.
const (
	silenceWitnesses     = 3
	silenceMoreWitnesses = 6
)

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
	// delivered: a request went out whole. counted: the connection was
	// counted as one the node left unanswered, by its timer or by an error
	// in place of the first byte; it is counted once.
	delivered atomic.Bool
	counted   atomic.Bool
	mu        sync.Mutex
	timer     *time.Timer
	sent      time.Time
}

func (c *watchedConn) sentOnce() {
	if !c.wrote.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.sent = time.Now()
	if !c.settled.Load() {
		c.timer = time.AfterFunc(silenceAfter, func() {
			if !c.settled.Load() && c.counted.CompareAndSwap(false, true) {
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

// refused ends the watch of a connection that got an error in place of its
// first byte back. After a request went out whole that may be a connection
// the node left unanswered, as one whose wait ran out is: a server that takes
// connections and drops every request says so at once. noteCut decides. An
// error after a failed write, or after the local side let its deadline pass,
// says nothing of the node; a connection the local side closed first is
// settled already.
func (c *watchedConn) refused(err error) {
	if !c.delivered.Load() || localEnd(err) {
		c.settle(false)
		return
	}
	if !c.settled.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
	}
	sent := c.sent
	c.mu.Unlock()
	if c.counted.CompareAndSwap(false, true) {
		c.gb.noteCut(c.node, c.born, sent, c.ask)
	}
}

// localEnd reports whether err is the local side ending the read.
func localEnd(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.As(err, &timeout) && timeout.Timeout()
}

// written follows a write: a request that went out whole is waited on, a
// failed write ends the watch. The connection failed, and the group counts
// that where it counts failed connections, not as silence.
func (c *watchedConn) written(request bool, err error) {
	if err != nil {
		c.settle(false)
	} else if request {
		c.delivered.Store(true)
	}
}

// An empty write sends no request: it only has the node open its
// connection, for a client that is yet to say anything.
func (c *watchedConn) Write(b []byte) (n int, err error) {
	if len(b) > 0 {
		c.sentOnce()
	}
	n, err = c.Conn.Write(b)
	c.written(len(b) > 0, err)
	return
}

func (c *watchedConn) WriteBuffer(buffer *buf.Buffer) error {
	request := !buffer.IsEmpty()
	if request {
		c.sentOnce()
	}
	err := c.Conn.WriteBuffer(buffer)
	c.written(request, err)
	return err
}

func (c *watchedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		c.settle(true)
	} else if err != nil {
		c.refused(err)
	}
	return
}

func (c *watchedConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = c.Conn.ReadBuffer(buffer)
	if !buffer.IsEmpty() {
		c.settle(true)
	} else if err != nil {
		c.refused(err)
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
// data: silenceCount connections through p in a row left without a byte back,
// or cut before the first one, have the group ask about p. answered, if any, is told of the first byte
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

// carries reports whether p is a server of its own. The built-in ways out
// are not asked about, and neither is a group inside the group: it answers
// for whatever node it is on, and it has checks of its own.
func carries(p C.Proxy) bool {
	switch p.Type() {
	case C.Direct, C.Compatible, C.Reject, C.Pass, C.RejectDrop:
		return false
	}
	return !isGroup(p)
}

func isGroup(p C.Proxy) bool {
	switch p.Type() {
	case C.Selector, C.Fallback, C.URLTest, C.LoadBalance, C.Relay:
		return true
	}
	return false
}

func (gb *GroupBase) noteAnswer(p C.Proxy, born time.Time) {
	gb.failedDeadOver()
	s := &gb.silence
	s.mu.Lock()
	if s.follows(p, born) {
		s.answeredAt, s.count, s.timed, s.pause, s.failedPause = time.Now(), 0, 0, 0, 0
		if s.noNetwork {
			s.noNetwork, s.quietUntil, s.failedQuietUntil = false, time.Time{}, time.Time{}
		}
	}
	s.mu.Unlock()
}

// noteSilence counts a connection whose wait for its first byte ran out.
func (gb *GroupBase) noteSilence(p C.Proxy, born, sent time.Time, ask func()) {
	gb.note(p, born, sent, ask, false)
}

// noteCut counts a connection cut before its first byte, but only one whose
// request went out at least silenceAfter after the node last answered and
// after the group took the node. A server that blocks some sites closes their
// connections at once, a page opens a few of them in a burst, and nothing
// answers in between: cut so soon after an answer, a connection says the
// site is refused, not that the node is. A server that cuts everything has
// answered nothing for that long by its third cut.
func (gb *GroupBase) noteCut(p C.Proxy, born, sent time.Time, ask func()) {
	gb.note(p, born, sent, ask, true)
}

func (gb *GroupBase) note(p C.Proxy, born, sent time.Time, ask func(), cut bool) {
	// Under the probe hold (network switching, process was paused) it is the
	// network that is silent, as with a failed dial.
	if C.ProbeHolding(sent) || C.ProbeHolding(time.Now()) {
		return
	}
	s := &gb.silence
	s.mu.Lock()
	// An answer through the node after this request went out: the node
	// passes data, it is the site that is silent.
	if !s.follows(p, born) || s.answeredAt.After(sent) {
		s.mu.Unlock()
		return
	}
	if cut {
		since := s.since
		if s.answeredAt.After(since) {
			since = s.answeredAt
		}
		if sent.Before(since.Add(silenceAfter)) {
			s.mu.Unlock()
			return
		}
	}
	now := time.Now()
	if cut && now.Before(s.cutQuietUntil) {
		// What a check that found the node alive has just answered.
		s.mu.Unlock()
		return
	}
	s.count++
	if !cut {
		s.timed++
	}
	start := s.count >= silenceCount && !s.asking && !now.Before(s.quietUntil)
	if start {
		s.byCuts, s.early = s.timed == 0, now.Before(s.cutQuietUntil)
		s.count, s.timed, s.asking = 0, 0, true
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

// asked ends a check of p. After one that left the group where it was the
// node is not asked about again for the pause, and the pause doubles until a
// connection through the node gets an answer:
//   - the node answered its probe while its connections stayed unanswered:
//     the site is silent, and more silence does not ask again; failed
//     connections are the node's own word and do, after a wait of their own
//     that starts short;
//   - nobody answered: the network is down, and neither silence nor failures
//     ask again.
//
// A check that cut connections alone started, and that found the node alive,
// holds back cut connections only: silence is still counted, and a check it
// starts meanwhile takes the next pause in advance.
//
// A check that found nothing out leaves a short wait and makes none longer.
// What was found out of p says nothing of the node the group has gone to
// meanwhile.
func (gb *GroupBase) asked(p C.Proxy, v verdict, failed bool) {
	s := &gb.silence
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asking = false
	now := time.Now()
	switch s.node {
	case p:
	case nil:
		s.node, s.since = p, now
	default:
		return
	}
	grown := func(from time.Time) time.Time {
		if s.pause == 0 {
			s.pause = silencePause
		}
		until := from.Add(s.pause)
		if s.pause *= 2; s.pause > silencePauseMax {
			s.pause = silencePauseMax
		}
		return until
	}
	s.noNetwork = v == verdictNoNetwork
	switch {
	case v == verdictDead:
		s.pause, s.failedPause, s.quietUntil, s.failedQuietUntil = 0, 0, time.Time{}, time.Time{}
		s.cutQuietUntil = time.Time{}
	case v == verdictNoNetwork:
		until := grown(now)
		s.quietUntil, s.failedQuietUntil = until, until
	case v == verdictAlive && !failed && s.byCuts:
		s.cutQuietUntil = grown(now)
	case v == verdictAlive && !failed && s.early:
		// The pause after this check follows the one the cut connections
		// left: two checks in two pauses, as there were.
		from := s.cutQuietUntil
		if from.Before(now) {
			from = now
		}
		s.quietUntil = grown(from)
		s.cutQuietUntil = s.quietUntil
	case v == verdictAlive && !failed:
		s.quietUntil = grown(now)
	case v == verdictAlive:
		// The node answers while connections through it fail.
		if s.failedPause == 0 {
			s.failedPause = silenceAfter
		}
		s.failedQuietUntil = now.Add(s.failedPause)
		if s.failedPause *= 2; s.failedPause > silencePause {
			s.failedPause = silencePause
		}
	case failed:
		s.failedQuietUntil = now.Add(silenceAfter)
	default:
		s.quietUntil = now.Add(silenceAfter)
	}
}

// heldProxy has a node probed without the result being written to it: the
// caller decides what the probe means.
type heldProxy struct {
	C.Proxy

	mu      sync.Mutex
	results []*C.ProbeResult
	soft    []time.Time
	// tries is how many tries the node was given, firstAnswered whether the
	// first of them answered.
	tries         int
	firstAnswered bool
}

// URLTest notes whether the first try answered. The failures written say it
// less well: a node that keeps shared connections and answered at its second
// try, over a new connection, has no failure of its hung first try written.
func (h *heldProxy) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (uint16, error) {
	h.mu.Lock()
	h.tries++
	first := h.tries == 1
	h.mu.Unlock()
	delay, err := h.Proxy.URLTest(ctx, url, expectedStatus)
	if result := C.HeldProbe(ctx); first && result != nil && result.OK() {
		h.mu.Lock()
		h.firstAnswered = true
		h.mu.Unlock()
	}
	return delay, err
}

func (h *heldProxy) RecordProbe(_ string, result *C.ProbeResult) {
	h.mu.Lock()
	h.results = append(h.results, result)
	h.mu.Unlock()
}

func (h *heldProxy) RecordSoftFailure(_ string, at time.Time) {
	h.mu.Lock()
	h.soft = append(h.soft, at)
	h.mu.Unlock()
}

func (h *heldProxy) LastProbeElapsed(url string) time.Duration {
	if r, ok := h.Proxy.(C.ProbeRecorder); ok {
		return r.LastProbeElapsed(url)
	}
	return 0
}

// steady reports whether the node answered at its first try.
func (h *heldProxy) steady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.firstAnswered
}

type answer struct {
	held *heldProxy
	// seen is the last probe written to the node before this one.
	seen time.Time
	// known is false for a probe that found nothing out: it was cut short or
	// the network was switching. steady: the node answered at its first try,
	// as it stood when the probe ended.
	answered, known, steady bool
}

// failed reports whether the node was probed and did not answer.
func (a answer) failed() bool {
	return a.known && !a.answered
}

// stale reports whether the node has been probed by someone else since: what
// they wrote is newer than this answer, and theirs stands.
func (a answer) stale(url string) bool {
	return !lastProbe(a.held.Proxy, url).Equal(a.seen)
}

// replay writes what the probe found to the node and reports whether it did:
// not over a newer probe.
func (a answer) replay(url string) bool {
	r, ok := a.held.Proxy.(C.ProbeRecorder)
	if !ok || a.stale(url) {
		return false
	}
	a.held.mu.Lock()
	defer a.held.mu.Unlock()
	for _, at := range a.held.soft {
		r.RecordSoftFailure(url, at)
	}
	for _, result := range a.held.results {
		r.RecordProbe(url, result)
	}
	return true
}

func (gb *GroupBase) askHeld(nodes []C.Proxy, d doubt) []answer {
	timeout := time.Duration(gb.testTimeout) * time.Millisecond
	expected, _ := utils.NewUnsignedRanges[uint16](d.expected)
	answers := make([]answer, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node C.Proxy) {
			defer wg.Done()
			held := &heldProxy{Proxy: node}
			seen := lastProbe(node, d.url)
			ctx, cancel := context.WithTimeout(context.Background(), probeBudget(timeout))
			defer cancel()
			_, err := provider.ProbeNode(ctx, held, d.url, expected, timeout)
			known := err == nil || (ctx.Err() == nil && !errors.Is(err, provider.ErrProbeDiscarded))
			answers[i] = answer{held: held, seen: seen, answered: err == nil, known: known, steady: err == nil && held.steady()}
		}(i, node)
	}
	wg.Wait()
	return answers
}

// witnesses picks the nodes asked beside p, out of those the group would go
// to, best first. One of them answering says the network is up, and it is
// where the group goes.
//
// The first are few: the best node, and, so that they do not all go down
// with p, the best node of another kind (see kindOf) and the best node of
// another server, if there are such. Nodes of one kind or of one server stop
// together. The node of another kind is taken from another server than p's
// if there is one: a server that went down whole takes all its kinds with
// it.
//
// more is asked only when none of the first answered: a few nodes, each of a
// kind or of a server not asked yet. Before the group takes it for the
// network of the device, it hears from every kind and server it has, up to
// silenceMoreWitnesses of them.
func witnesses(p C.Proxy, next []C.Proxy) (first, more []C.Proxy) {
	servers := make([]C.Proxy, 0, len(next))
	for _, node := range next {
		// A node that refuses everything has no answer to give, and a group
		// answers for whatever node it is on.
		if t := node.Type(); t != C.Reject && t != C.RejectDrop && t != C.Pass && t != C.Compatible && !isGroup(node) {
			servers = append(servers, node)
		}
	}
	home := C.ProbeHostOf(p)
	kinds, hosts := map[string]bool{kindOf(p): true}, map[string]bool{home: true}
	taken := make(map[C.Proxy]bool, silenceWitnesses+silenceMoreWitnesses)
	take := func(list *[]C.Proxy, node C.Proxy) {
		*list = append(*list, node)
		taken[node], kinds[kindOf(node)], hosts[C.ProbeHostOf(node)] = true, true, true
	}
	if len(servers) > 0 {
		take(&first, servers[0])
	}
	otherKind := func(away bool) bool {
		for _, node := range servers {
			if len(first) < silenceWitnesses && !taken[node] && !kinds[kindOf(node)] && (!away || C.ProbeHostOf(node) != home) {
				take(&first, node)
				return true
			}
		}
		return false
	}
	if !otherKind(true) {
		otherKind(false)
	}
	for _, node := range servers {
		if len(first) < silenceWitnesses && !taken[node] && !hosts[C.ProbeHostOf(node)] {
			take(&first, node)
			break
		}
	}
	for _, node := range servers {
		if len(first) < silenceWitnesses && !taken[node] {
			take(&first, node)
		}
	}
	for _, node := range servers {
		if len(more) == silenceMoreWitnesses {
			break
		}
		if !taken[node] && (!kinds[kindOf(node)] || !hosts[C.ProbeHostOf(node)]) {
			take(&more, node)
		}
	}
	return first, more
}

// failedOf answers the connections through p that failed. Those of a node
// marked dead are answered by failedDead. A group whose node is not a server
// of its own has every node checked, as it always had.
//
// born is when the group took p for the connection that failed. A failed
// connection is one of the node the group is on as much as one that opened:
// a node that only refuses is counted from its failures, and the wait its
// check leaves is its own. The failures of a node the group has left ask
// nothing.
func (gb *GroupBase) failedOf(p C.Proxy, born time.Time, d doubt) {
	if !p.AliveForTestUrl(d.url) {
		gb.failedDead(p, d)
		return
	}
	if !carries(p) {
		d.all()
		return
	}
	s := &gb.silence
	s.mu.Lock()
	start := s.follows(p, born) && !s.asking && !time.Now().Before(s.failedQuietUntil)
	if start {
		s.asking, s.byCuts, s.early = true, false, false
	}
	s.mu.Unlock()
	if start {
		gb.askOf(p, d, true)
	}
}

// askOf answers the connections p failed or left unanswered. It probes p and
// a few nodes the group would go to, at once, and writes nothing down until
// it has their answers:
//   - p answers, at its first try or its second: it was the site, nothing is
//     written, so the delay measured at a bad moment moves the group nowhere,
//     and a try it needed then costs it no rank;
//   - p is silent and every other node answers at its first try: p is dead,
//     its answer is written and the group leaves;
//   - anything else (a node beside it silent too, or answering only at its
//     second try, or nobody answering) is no proof: a network that stalls and
//     comes back within the probe looks just so. p and the first few are
//     asked again, anew, with the more beside them, and that second look
//     decides alone: p answers, it is alive; p is silent and any node of the
//     look answers, p is dead; nobody answers, it is the network of the
//     device, and nothing is written. Written down, the failures would mark
//     every node dead for an outage the nodes had no part in, and the marks
//     outlive it.
//
// A node beside p is marked dead only when it was silent at both looks: one
// look is no proof of it either. What the nodes beside p answered is not
// written: the group goes by their scheduled probes, not by ones made while
// the network may have been at its worst.
//
// A group with no other node alive has every node checked instead: there is
// nobody to tell p from the network by.
func (gb *GroupBase) askOf(p C.Proxy, d doubt, failed bool) {
	// The connections opened before p was marked dead go silent after it.
	if !p.AliveForTestUrl(d.url) || !gb.failedTesting.CompareAndSwap(false, true) {
		gb.asked(p, verdictNone, failed)
		return
	}
	first, more := witnesses(p, d.next(p))
	if len(first) == 0 {
		seen := lastProbe(p, d.url)
		gb.failedTesting.Store(false)
		d.all()
		switch {
		case lastProbe(p, d.url).Equal(seen):
			// Another check of the group was running: this one did not start.
			gb.asked(p, verdictNone, failed)
		case p.AliveForTestUrl(d.url):
			if d.hold != nil {
				d.hold()
			}
			gb.asked(p, verdictAlive, failed)
		default:
			gb.asked(p, verdictDead, failed)
			gb.leftDead(p, d)
		}
		return
	}
	// As after a check of the whole group: the failures of these seconds
	// are answered.
	end := func(v verdict) {
		gb.failedTestMux.Lock()
		gb.failedTimes = 0
		gb.failedTestMux.Unlock()
		gb.failedTesting.Store(false)
		gb.asked(p, v, failed)
	}
	// The witnesses open a connection per probe: p is probed over a new
	// one too, not over those it keeps, which are what is in doubt.
	fresh := provider.FreshConns(p)
	nodes := append([]C.Proxy{p}, first...)
	answers := gb.askHeld(nodes, d)
	self, others := answers[0], answers[1:]
	switch {
	case !self.known || self.stale(d.url):
		end(verdictNone)
		return
	case self.answered:
		end(verdictAlive)
		return
	}
	var silent []answer
	if !allSteady(others) {
		look := others
		if !fresh {
			provider.FreshConns(p)
		}
		answers = gb.askHeld(append(nodes, more...), d)
		self, others = answers[0], answers[1:]
		switch {
		case !self.known || self.stale(d.url):
			end(verdictNone)
			return
		case self.answered:
			end(verdictAlive)
			return
		case !anyAnswered(others):
			log.Infoln("[Проба] %s: не ответил ни узел, ни узлы рядом — похоже, нет сети, пометки не меняются", p.Name())
			end(verdictNoNetwork)
			return
		}
		for i, each := range others[:len(first)] {
			if each.failed() && look[i].failed() {
				silent = append(silent, each)
			}
		}
	}
	// A probe written to p by someone else meanwhile stands over this
	// answer: nothing was found out.
	if !self.replay(d.url) {
		end(verdictNone)
		return
	}
	for _, each := range silent {
		if each.replay(d.url) {
			log.Warnln("[Проба] %s: не ответил дважды рядом с ответившими — узел помечен мёртвым", each.held.Name())
		}
	}
	reason := "запросы через узел остаются без ответа"
	if failed {
		reason = "соединения через узел не открываются"
	}
	log.Warnln("[Проба] %s: %s, проба тоже — узел помечен мёртвым", p.Name(), reason)
	end(verdictDead)
	gb.leftDead(p, d)
}

// allSteady reports whether there are answers and every one of them came at
// the first try.
func allSteady(answers []answer) bool {
	for _, each := range answers {
		if !each.steady {
			return false
		}
	}
	return len(answers) > 0
}

func anyAnswered(answers []answer) bool {
	for _, each := range answers {
		if each.answered {
			return true
		}
	}
	return false
}

// aliveBut is the alive nodes of the list without the given one.
func aliveBut(proxies []C.Proxy, but C.Proxy, url string) []C.Proxy {
	var alive []C.Proxy
	for _, proxy := range proxies {
		if proxy != but && proxy.AliveForTestUrl(url) {
			alive = append(alive, proxy)
		}
	}
	return alive
}
