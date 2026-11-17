package outboundgroup

import (
	"context"
	"errors"
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
	return len(h.soft) == 0
}

type answer struct {
	held *heldProxy
	// seen is the last probe written to the node before this one.
	seen time.Time
	// known is false for a probe that found nothing out: it was cut short or
	// the network was switching.
	answered, known bool
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
			ctx, cancel := context.WithTimeout(context.Background(), provider.ProbeHedgeDelay+timeout+C.ProbeReserve)
			defer cancel()
			_, err := provider.ProbeNode(ctx, held, d.url, expected, timeout)
			known := err == nil || (ctx.Err() == nil && !errors.Is(err, provider.ErrProbeDiscarded))
			answers[i] = answer{held: held, seen: seen, answered: err == nil, known: known}
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
// with p, the best node of another kind and the best node of another server,
// if there are such. Nodes of one kind or of one server stop together.
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
	kinds, hosts := map[C.AdapterType]bool{p.Type(): true}, map[string]bool{C.ProbeHostOf(p): true}
	taken := make(map[C.Proxy]bool, silenceWitnesses+silenceMoreWitnesses)
	take := func(list *[]C.Proxy, node C.Proxy) {
		*list = append(*list, node)
		taken[node], kinds[node.Type()], hosts[C.ProbeHostOf(node)] = true, true, true
	}
	if len(servers) > 0 {
		take(&first, servers[0])
	}
	for _, node := range servers {
		if len(first) < silenceWitnesses && !taken[node] && !kinds[node.Type()] {
			take(&first, node)
			break
		}
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
		if !taken[node] && (!kinds[node.Type()] || !hosts[C.ProbeHostOf(node)]) {
			take(&more, node)
		}
	}
	return first, more
}

// failedOf answers the connections through p that failed. A group that is on
// a dead node already has every node checked, as it always had: there is
// nothing to lose, and the nodes that are back are found. So has a group
// whose node is not a server of its own.
//
// born is when the group took p for the connection that failed. A failed
// connection is one of the node the group is on as much as one that opened:
// a node that only refuses is counted from its failures, and the wait its
// check leaves is its own. The failures of a node the group has left ask
// nothing.
func (gb *GroupBase) failedOf(p C.Proxy, born time.Time, d doubt) {
	if !carries(p) || !p.AliveForTestUrl(d.url) {
		d.all()
		return
	}
	s := &gb.silence
	s.mu.Lock()
	start := s.follows(p, born) && !s.asking && !time.Now().Before(s.failedQuietUntil)
	if start {
		s.asking = true
	}
	s.mu.Unlock()
	if start {
		gb.askOf(p, d, true)
	}
}

// askOf answers the connections p failed or left unanswered. It probes p and
// a few nodes the group would go to, at once, and writes nothing down until
// it has their answers:
//   - p answers: it was the site, nothing is written, so the delay measured at
//     a bad moment moves the group nowhere. Only a second try p needed where
//     another node did not is written to p;
//   - p is silent and another node answers: p is dead, the answers are
//     written and the group leaves;
//   - nobody answers, of the few asked first and of the more asked then: it
//     is the network of the device, and nothing is written. Written down, the
//     failures would mark every node dead for an outage the nodes had no part
//     in, and the marks outlive it.
//
// The more are asked together with p and the first few, anew: what those said
// is seconds old by then, and a network that came back meanwhile would have
// their silence held against them beside the answers of the more.
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
	nodes := append([]C.Proxy{p}, first...)
	answers := gb.askHeld(nodes, d)
	self, others := answers[0], answers[1:]
	again := len(more) > 0 && self.known && !self.answered && !self.stale(d.url) && !anyAnswered(others)
	if again {
		answers = gb.askHeld(append(nodes, more...), d)
		self, others = answers[0], answers[1:]
	}
	switch {
	case !self.known || self.stale(d.url):
		end(verdictNone)
		return
	case self.answered:
		// A node that answered at its second try beside one that answered
		// at the first is unsteady, and that is written down: url-test ranks
		// it below the steady ones. With no node steady it is the network,
		// and so it is after nobody answered: a network coming back gives
		// one node its answer a try sooner than another.
		if !again && !self.held.steady() {
			for _, other := range others {
				if other.answered && other.held.steady() {
					self.replay(d.url)
					log.Infoln("[Проба] %s: ответил со второй попытки, узлы рядом — с первой", p.Name())
					break
				}
			}
		}
		end(verdictAlive)
		return
	}
	if !anyAnswered(others) {
		log.Infoln("[Проба] %s: не ответил ни узел, ни узлы рядом — похоже, нет сети, пометки не меняются", p.Name())
		end(verdictNoNetwork)
		return
	}
	// A probe written to p by someone else meanwhile stands over this
	// answer: nothing was found out.
	if !self.replay(d.url) {
		end(verdictNone)
		return
	}
	for _, each := range others {
		if each.known {
			each.replay(d.url)
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
