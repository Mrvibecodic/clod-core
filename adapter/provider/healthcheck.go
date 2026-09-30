package provider

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/dlclark/regexp2"
	"golang.org/x/sync/errgroup"
)

type HealthCheckOption struct {
	URL      string
	Interval uint
}

type extraOption struct {
	expectedStatus utils.IntRanges[uint16]
	filters        map[string]struct{}
}

type HealthCheck struct {
	ctx            context.Context
	ctxCancel      context.CancelFunc
	url            string
	extra          map[string]*extraOption
	mu             sync.Mutex
	proxies        []C.Proxy
	interval       time.Duration
	lazy           bool
	expectedStatus utils.IntRanges[uint16]
	lastTouch      atomic.TypedValue[time.Time]
	singleDo       *singledo.Single[struct{}]
	timeout        time.Duration
}

func (hc *HealthCheck) process() {
	ticker := time.NewTicker(hc.interval)
	go hc.check()
	for {
		select {
		case <-ticker.C:
			lastTouch := hc.lastTouch.Load()
			since := time.Since(lastTouch)
			if !hc.lazy || since < hc.interval {
				hc.check()
			} else {
				log.Debugln("Skip once health check because we are lazy")
			}
		case <-hc.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (hc *HealthCheck) setProxies(proxies []C.Proxy) {
	hc.mu.Lock()
	hc.proxies = proxies
	hc.mu.Unlock()
}

// snapshotProxies reads the list under the lock: a provider update replaces it
// while a round may be starting, and an unsynchronised read of a slice header
// can pair the new array with the old length.
func (hc *HealthCheck) snapshotProxies() []C.Proxy {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.proxies
}

func (hc *HealthCheck) registerHealthCheckTask(url string, expectedStatus utils.IntRanges[uint16], filter string, interval uint) {
	url = strings.TrimSpace(url)
	if len(url) == 0 || url == hc.url {
		log.Debugln("ignore invalid health check url: %s", url)
		return
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// if the provider has not set up health checks, then modify it to be the same as the group's interval
	if hc.interval == 0 {
		hc.interval = time.Duration(interval) * time.Second
	}

	if hc.extra == nil {
		hc.extra = make(map[string]*extraOption)
	}

	// prioritize the use of previously registered configurations, especially those from provider
	if _, ok := hc.extra[url]; ok {
		// provider default health check does not set filter
		if url != hc.url && len(filter) != 0 {
			splitAndAddFiltersToExtra(filter, hc.extra[url])
		}

		log.Debugln("health check url: %s exists", url)
		return
	}

	option := &extraOption{filters: map[string]struct{}{}, expectedStatus: expectedStatus}
	splitAndAddFiltersToExtra(filter, option)
	hc.extra[url] = option
}

func splitAndAddFiltersToExtra(filter string, option *extraOption) {
	filter = strings.TrimSpace(filter)
	if len(filter) != 0 {
		for _, regex := range strings.Split(filter, "`") {
			regex = strings.TrimSpace(regex)
			if len(regex) != 0 {
				option.filters[regex] = struct{}{}
			}
		}
	}
}

func (hc *HealthCheck) auto() bool {
	return hc.interval != 0
}

func (hc *HealthCheck) touch() {
	hc.lastTouch.Store(time.Now())
}

func (hc *HealthCheck) check() {
	if len(hc.snapshotProxies()) == 0 {
		return
	}

	_, _, _ = hc.singleDo.Do(func() (struct{}, error) {
		proxies, head := C.ProbeFirstOrder(hc.snapshotProxies())
		id := utils.NewUUIDV4().String()
		log.Debugln("Start New Health Checking {%s}", id)
		b := new(errgroup.Group)
		b.SetLimit(10)

		option := &extraOption{filters: nil, expectedStatus: hc.expectedStatus}
		tally := &probeTally{down: map[string]struct{}{}}
		// The nodes the groups use go first on every URL: a group with a URL of
		// its own (an extra one) ranks its nodes by that URL, not the provider's.
		// They book their turns to their hosts before the rest is started, or
		// a node of the same host started later could take the first turn.
		booked := &sync.WaitGroup{}
		for _, part := range [][]C.Proxy{proxies[:head], proxies[head:]} {
			part = alternateHosts(part)

			// execute default health check
			hc.execute(b, part, hc.url, id, option, tally, booked)

			// execute extra health check
			for url, option := range hc.extra {
				hc.execute(b, part, url, id, option, tally, booked)
			}
			if booked != nil {
				booked.Wait()
				booked = nil
			}
		}
		_ = b.Wait()
		if stalls := tally.recovered.Load() + tally.stalled.Load(); stalls > 1 {
			log.Warnln("[Проба] за проверку первая проба зависла или оборвалась у %d из %d узлов: повторная прошла у %d, не прошла у %d",
				stalls, tally.total.Load(), tally.recovered.Load(), tally.stalled.Load())
		}
		tally.reportDown()
		log.Debugln("Finish A Health Checking {%s}", id)
		return struct{}{}, nil
	})
}

func (hc *HealthCheck) execute(b *errgroup.Group, proxies []C.Proxy, url, uid string, option *extraOption, tally *probeTally, booked *sync.WaitGroup) {
	url = strings.TrimSpace(url)
	if len(url) == 0 {
		log.Debugln("Health Check has been skipped due to testUrl is empty, {%s}", uid)
		return
	}

	var filterReg *regexp2.Regexp
	var expectedStatus utils.IntRanges[uint16]
	if option != nil {
		expectedStatus = option.expectedStatus
		if len(option.filters) != 0 {
			filters := make([]string, 0, len(option.filters))
			for filter := range option.filters {
				filters = append(filters, filter)
			}

			filterReg = regexp2.MustCompile(strings.Join(filters, "|"), regexp2.None)
		}
	}

	for _, proxy := range proxies {
		// skip proxies that do not require health check
		if filterReg != nil {
			if match, _ := filterReg.MatchString(proxy.Name()); !match {
				continue
			}
		}

		p := proxy
		probeHC, booked := hc, booked
		release := func() {}
		if booked != nil {
			// The probe tells the round once it has booked its turn to the
			// host, or once it needs none (shared, cancelled).
			var once sync.Once
			booked.Add(1)
			release = func() { once.Do(booked.Done) }
			probeHC = &HealthCheck{ctx: C.WithProbeBooked(hc.ctx, release), timeout: hc.timeout}
		}
		b.Go(func() error {
			defer release()
			probeHC.checkOne(p, url, uid, expectedStatus, tally)
			return nil
		})
	}
}

// checkOne probes one node for a round and counts the outcome.
func (hc *HealthCheck) checkOne(p C.Proxy, url, uid string, expectedStatus utils.IntRanges[uint16], tally *probeTally) {
	log.Debugln("Health Checking, proxy: %s, url: %s, id: {%s}", p.Name(), url, uid)
	// A node that was already dead stalls every round: it says nothing
	// about the local side, which is what the round's count is about.
	wasAlive := p.AliveForTestUrl(url)
	outcome, _ := hc.probe(p, url, expectedStatus)
	if outcome == probeCancelled || outcome == probeShared {
		return
	}
	tally.total.Add(1)
	switch outcome {
	case probeRecovered:
		tally.recovered.Add(1)
	case probeStalled:
		if wasAlive {
			tally.stalled.Add(1)
		}
	}
	if wasAlive && !p.AliveForTestUrl(url) {
		tally.noteDown(p.Name())
	}
	log.Debugln("Health Checked, proxy: %s, url: %s, alive: %t, delay: %d ms uid: {%s}", p.Name(), url, p.AliveForTestUrl(url), p.LastDelayForTestUrl(url), uid)
}

// alternateHosts orders proxies so that nodes of one host do not stand in a
// row: a round runs ten probes at a time, and probes to one host are spaced
// out, so a row of them held all ten places while waiting for their turns
// and the nodes of other hosts waited behind them. The order within a host
// is kept.
func alternateHosts(proxies []C.Proxy) []C.Proxy {
	if len(proxies) < 3 {
		return proxies
	}
	var hosts []string
	byHost := map[string][]C.Proxy{}
	for _, p := range proxies {
		host := C.ProbeHost(p.Addr())
		if _, ok := byHost[host]; !ok {
			hosts = append(hosts, host)
		}
		byHost[host] = append(byHost[host], p)
	}
	if len(hosts) == len(proxies) || len(hosts) == 1 {
		return proxies
	}
	ordered := make([]C.Proxy, 0, len(proxies))
	for len(ordered) < len(proxies) {
		for _, host := range hosts {
			if rest := byHost[host]; len(rest) > 0 {
				ordered = append(ordered, rest[0])
				byHost[host] = rest[1:]
			}
		}
	}
	return ordered
}

// probeHedgeDelay is how long the first probe gets before a second one is
// started beside it: long enough for a healthy node to answer and never see a
// second probe, short enough that a node whose first probe stalled is judged
// within one timeout after it. The worst case for a node is thus
// probeHedgeDelay + timeout, not two timeouts.
const probeHedgeDelay = time.Second

// ProbeHedgeDelay is probeHedgeDelay for callers of ProbeNode that budget a
// probe: it may take ProbeHedgeDelay plus its timeout.
const ProbeHedgeDelay = probeHedgeDelay

// probeHedgeGrace is how long the first probe still gets once the second one
// has answered. A lost connect packet is resent about when the second probe
// starts, and then both answer at nearly the same moment: the first one is
// only slower, not failed, and must not cost the node a failure in its history.
const probeHedgeGrace = 250 * time.Millisecond

type probeOutcome int

const (
	probeCancelled probeOutcome = iota
	probeShared                 // another group's check of the same node was already running
	probePassed
	probeRecovered // the first probe stalled, the second passed
	probeStalled   // both probes failed
	probeRefused   // failed with an answer: closed port, missing name, bad status
	probeFailed    // failed with no second probe: the node was dead or is slow
)

// probeTally counts the outcomes of one health check round. Many nodes stalling
// on their first probe in the same round point at the local side (network,
// DNS, the way to the servers) rather than at the nodes.
type probeTally struct {
	total, recovered, stalled atomic.Int32

	// down are the nodes the round took down, reported in one line: when the
	// network is gone every node goes down, and a line per node buries the log.
	mu   sync.Mutex
	down map[string]struct{}
}

func (t *probeTally) noteDown(name string) {
	t.mu.Lock()
	t.down[name] = struct{}{}
	t.mu.Unlock()
}

func (t *probeTally) reportDown() {
	if len(t.down) == 0 {
		return
	}
	names := make([]string, 0, len(t.down))
	for name := range t.down {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > 5 {
		names = append(names[:5], fmt.Sprintf("и ещё %d", len(names)-5))
	}
	log.Warnln("[Проба] перестали отвечать %d из %d узлов: %s", len(t.down), t.total.Load(), strings.Join(names, ", "))
}

// probeFlights are the node probes in progress, by node, URL and expected
// status. A node that stands in several groups was probed once per group in
// the same round; now the first group probes it and the others take the
// outcome of that probe, which is already in the shared history.
var probeFlights = struct {
	sync.Mutex
	m map[string]*probeFlight
}{m: map[string]*probeFlight{}}

type probeFlight struct {
	done    chan struct{}
	outcome probeOutcome
	result  *C.ProbeResult
}

func probeFlightKey(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) string {
	return fmt.Sprintf("%p|%s|%s", p, url, expectedStatus.String())
}

// probeRun is one URLTest running in the background in held mode.
type probeRun struct {
	started time.Time
	result  *C.ProbeResult
	done    chan struct{}
	cancel  context.CancelFunc
}

func (r *probeRun) finished() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// startProbe waits for the probe's start slot (probes to one host are spaced
// out) and then runs the probe in the background: the hedge timer of the
// caller counts from the probe's own start, not from the wait for the slot.
func (hc *HealthCheck) startProbe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16], hedge bool) *probeRun {
	base, cancel := context.WithTimeout(hc.ctx, hc.timeout)
	if hedge {
		// A caller that paced its probe paced the first one only.
		base = C.UnmarkProbePaced(base)
	}
	run := &probeRun{result: &C.ProbeResult{}, done: make(chan struct{}), cancel: cancel}
	pace := func() error { return C.ProbePace(base, C.ProbeHost(p.Addr())) }
	if !hedge {
		if err := pace(); err != nil {
			// The check was cancelled while the probe waited for its turn:
			// nothing ran, the result stays unheld.
			run.started = time.Now()
			close(run.done)
			return run
		}
	}
	ctx, held := C.WithHeldProbe(C.MarkProbePaced(base))
	run.started = time.Now()
	run.result = held
	go func() {
		defer close(run.done)
		// The second probe waits for its turn in the background: an answer to
		// the first one that comes meanwhile is taken at once. A wait that is
		// cut short leaves the result unheld.
		if hedge && pace() != nil {
			return
		}
		_, _ = p.URLTest(ctx, url, expectedStatus)
	}()
	return run
}

// probe tests p and records the result. A probe that has not answered within
// probeHedgeDelay, or failed in a way that may be a momentary stall, gets a
// second probe started beside it: the node is alive if either answers, and the
// stalled probe is kept in its history so url-test ranks the node below steady
// ones. A failure that is an answer (closed port, missing name, unexpected
// status) marks the node dead at once, and so do two failed probes. The result
// returned is the recorded one that decided the outcome, nil if none was.
func (hc *HealthCheck) probe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) (probeOutcome, *C.ProbeResult) {
	recorder, ok := p.(C.ProbeRecorder)
	if !ok {
		ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
		defer cancel()
		_, _ = p.URLTest(ctx, url, expectedStatus)
		return probePassed, nil
	}

	key := probeFlightKey(p, url, expectedStatus)
	for {
		probeFlights.Lock()
		flight, running := probeFlights.m[key]
		if !running {
			break
		}
		probeFlights.Unlock()
		// Waiting for another probe of the node books no turn to its host.
		C.ProbeBooked(hc.ctx)
		select {
		case <-flight.done:
		case <-hc.ctx.Done():
			return probeCancelled, nil
		}
		// A probe cancelled with its own caller (another check, a client's
		// round) found nothing out: this one probes the node itself.
		if flight.outcome != probeCancelled {
			log.Debugln("[Проба] %s: проба уже шла у другой группы, взят её результат", p.Name())
			return probeShared, flight.result
		}
	}
	flight := &probeFlight{done: make(chan struct{})}
	probeFlights.m[key] = flight
	probeFlights.Unlock()
	outcome, result := hc.probeOnce(p, recorder, url, expectedStatus)
	probeFlights.Lock()
	delete(probeFlights.m, key)
	probeFlights.Unlock()
	flight.outcome, flight.result = outcome, result
	close(flight.done)
	return outcome, result
}

// probeOnce is the probe itself, with the hedge; probe wraps it so that one
// node is probed once at a time whichever groups ask.
func (hc *HealthCheck) probeOnce(p C.Proxy, recorder C.ProbeRecorder, url string, expectedStatus utils.IntRanges[uint16]) (probeOutcome, *C.ProbeResult) {
	// The second probe guards a live node against a false verdict; a dead
	// node has none to lose. A live node known to answer slower than the
	// hedge delay gets it only after a quick failure, or it would get two
	// probes every round.
	last := p.LastDelayForTestUrl(url)
	alive := p.AliveForTestUrl(url)
	slow := alive && last != 0xffff && time.Duration(last)*time.Millisecond >= probeHedgeDelay
	first := hc.startProbe(p, url, expectedStatus, false)
	defer first.cancel()
	// The hedge delay counts from the probe's own start, not from its wait
	// for a turn to the host.
	hedge := time.NewTimer(probeHedgeDelay)
	defer hedge.Stop()
	stalled := hedge.C
	if !alive || slow {
		stalled = nil
	}

	select {
	case <-first.done:
		if hc.ctx.Err() != nil || !first.result.Held {
			return probeCancelled, nil
		}
		if first.result.OK() {
			recorder.RecordProbe(url, first.result)
			return probePassed, first.result
		}
		if !first.result.Retryable() || !alive || !hedge.Stop() {
			// An answer, a dead node, or a failure that took the whole hedge
			// delay (a slow node's): no second probe.
			log.Debugln("[Проба] %s: не отвечает (%s)", p.Name(), first.result)
			recorder.RecordProbe(url, first.result)
			if !first.result.Retryable() {
				return probeRefused, first.result
			}
			return probeFailed, first.result
		}
		// A quick failure that may be a lost packet: the second probe still
		// waits out the hedge delay rather than repeating the failure at once.
		hedge.Reset(probeHedgeDelay - time.Since(first.started))
		select {
		case <-hedge.C:
		case <-hc.ctx.Done():
			return probeCancelled, nil
		}
	case <-stalled:
	case <-hc.ctx.Done():
		return probeCancelled, nil
	}

	second := hc.startProbe(p, url, expectedStatus, true)
	defer second.cancel()
	// A closed channel is always ready: once a probe has reported, its channel
	// is set aside so the select waits for the other one.
	firstDone, secondDone := first.done, second.done
	var grace <-chan time.Time
	graceOver := false
	for {
		select {
		case <-firstDone:
			firstDone = nil
		case <-secondDone:
			secondDone = nil
		case <-grace:
			grace, graceOver = nil, true
		case <-hc.ctx.Done():
			return probeCancelled, nil
		}
		if hc.ctx.Err() != nil {
			return probeCancelled, nil
		}
		if first.finished() && first.result.Held && first.result.OK() {
			// The node answered its own probe, only slower than the hedge;
			// the second probe is dropped, its outcome decides nothing.
			recorder.RecordProbe(url, first.result)
			return probePassed, first.result
		}
		if second.finished() && second.result.Held && second.result.OK() {
			if !first.finished() && !graceOver {
				if grace == nil {
					timer := time.NewTimer(probeHedgeGrace)
					defer timer.Stop()
					grace = timer.C
				}
				continue
			}
			// A first probe whose failure was set aside (the network was
			// switching) is no failure of the node.
			if !first.finished() || first.result.Held {
				recorder.RecordSoftFailure(url, first.started)
			}
			recorder.RecordProbe(url, second.result)
			log.Debugln("[Проба] %s: первая проба не ответила за %d мс, повторная прошла: %s",
				p.Name(), time.Since(first.started).Milliseconds(), second.result)
			return probeRecovered, second.result
		}
		if first.finished() && second.finished() {
			if !first.result.Held || !second.result.Held {
				return probeCancelled, nil
			}
			// One round, one failure in the history: url-test counts recent
			// failures, and two records of one outage would double its penalty.
			log.Debugln("[Проба] %s: не отвечает (%s), повторная проба тоже (%s)", p.Name(), first.result, second.result)
			recorder.RecordProbe(url, second.result)
			if !second.result.Retryable() {
				return probeRefused, second.result
			}
			return probeStalled, second.result
		}
	}
}

// ProbeNode probes p the way the scheduled health check does: a second probe
// beside a stalled first one, a failure confirmed before the node is marked
// dead, one probe per node at a time shared with any check already probing
// it. ctx bounds the whole probe, timeout each of its (at most two) attempts,
// so it may take ProbeHedgeDelay plus timeout. A probe that found nothing out
// (ctx ended, the network was switching) returns ctx's error or
// ErrProbeDiscarded and records nothing.
func ProbeNode(ctx context.Context, p C.Proxy, url string, expectedStatus utils.IntRanges[uint16], timeout time.Duration) (uint16, error) {
	// The probe needs only a context and a timeout from its health check.
	outcome, result := (&HealthCheck{ctx: ctx, timeout: timeout}).probe(p, url, expectedStatus)
	switch {
	case outcome == probeCancelled:
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 0, ErrProbeDiscarded
	case result == nil:
		// A node that cannot hold its probe results was recorded directly.
		if p.AliveForTestUrl(url) {
			return p.LastDelayForTestUrl(url), nil
		}
		return 0, errProbeFailed
	case result.OK():
		return result.Delay, nil
	case result.Err != nil:
		return 0, result.Err
	default:
		return 0, fmt.Errorf("%w: %d", errUnexpectedStatus, result.Status)
	}
}

var (
	// ErrProbeDiscarded is a probe whose result says nothing about the node:
	// it ran while the network was switching or the process was paused.
	ErrProbeDiscarded   = errors.New("probe discarded")
	errProbeFailed      = errors.New("probe failed")
	errUnexpectedStatus = errors.New("unexpected status code")
)

func (hc *HealthCheck) close() {
	hc.ctxCancel()
}

func NewHealthCheck(proxies []C.Proxy, url string, timeout uint, interval uint, lazy bool, expectedStatus utils.IntRanges[uint16]) *HealthCheck {
	if url == "" {
		expectedStatus = nil
		interval = 0
	}
	if timeout == 0 {
		timeout = 5000
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthCheck{
		ctx:            ctx,
		ctxCancel:      cancel,
		proxies:        proxies,
		url:            url,
		timeout:        time.Duration(timeout) * time.Millisecond,
		extra:          map[string]*extraOption{},
		interval:       time.Duration(interval) * time.Second,
		lazy:           lazy,
		expectedStatus: expectedStatus,
		singleDo:       singledo.NewSingle[struct{}](time.Second),
	}
}
