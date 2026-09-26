package provider

import (
	"context"
	"fmt"
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
	hc.proxies = proxies
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
	if len(hc.proxies) == 0 {
		return
	}

	_, _, _ = hc.singleDo.Do(func() (struct{}, error) {
		id := utils.NewUUIDV4().String()
		log.Debugln("Start New Health Checking {%s}", id)
		b := new(errgroup.Group)
		b.SetLimit(10)

		// execute default health check
		option := &extraOption{filters: nil, expectedStatus: hc.expectedStatus}
		tally := &probeTally{}
		hc.execute(b, hc.url, id, option, tally)

		// execute extra health check
		if len(hc.extra) != 0 {
			for url, option := range hc.extra {
				hc.execute(b, url, id, option, tally)
			}
		}
		_ = b.Wait()
		if stalls := tally.recovered.Load() + tally.stalled.Load(); stalls > 1 {
			log.Warnln("[Проба] за проверку первая проба зависла или оборвалась у %d из %d узлов: повторная прошла у %d, не прошла у %d",
				stalls, tally.total.Load(), tally.recovered.Load(), tally.stalled.Load())
		}
		log.Debugln("Finish A Health Checking {%s}", id)
		return struct{}{}, nil
	})
}

func (hc *HealthCheck) execute(b *errgroup.Group, url, uid string, option *extraOption, tally *probeTally) {
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

	for _, proxy := range hc.proxies {
		// skip proxies that do not require health check
		if filterReg != nil {
			if match, _ := filterReg.MatchString(proxy.Name()); !match {
				continue
			}
		}

		p := proxy
		b.Go(func() error {
			log.Debugln("Health Checking, proxy: %s, url: %s, id: {%s}", p.Name(), url, uid)
			outcome := hc.probe(p, url, expectedStatus)
			if outcome == probeCancelled || outcome == probeShared {
				return nil
			}
			tally.total.Add(1)
			switch outcome {
			case probeRecovered:
				tally.recovered.Add(1)
			case probeStalled:
				tally.stalled.Add(1)
			}
			log.Debugln("Health Checked, proxy: %s, url: %s, alive: %t, delay: %d ms uid: {%s}", p.Name(), url, p.AliveForTestUrl(url), p.LastDelayForTestUrl(url), uid)
			return nil
		})
	}
}

// probeHedgeDelay is how long the first probe gets before a second one is
// started beside it: long enough for a healthy node to answer and never see a
// second probe, short enough that a node whose first probe stalled is judged
// within one timeout after it. The worst case for a node is thus
// probeHedgeDelay + timeout, not two timeouts.
const probeHedgeDelay = time.Second

type probeOutcome int

const (
	probeCancelled probeOutcome = iota
	probeShared                 // another group's check of the same node was already running
	probePassed
	probeRecovered // the first probe stalled, the second passed
	probeStalled   // both probes failed
	probeRefused   // failed with an answer: closed port, missing name, bad status
)

// probeTally counts the outcomes of one health check round. Many nodes stalling
// on their first probe in the same round point at the local side (network,
// DNS, the way to the servers) rather than at the nodes.
type probeTally struct {
	total, recovered, stalled atomic.Int32
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

func (hc *HealthCheck) startProbe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) *probeRun {
	ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
	ctx, held := C.WithHeldProbe(ctx)
	run := &probeRun{started: time.Now(), result: held, done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(run.done)
		_, _ = p.URLTest(ctx, url, expectedStatus)
	}()
	return run
}

// probe tests p and records the result. A probe that has not answered within
// probeHedgeDelay, or failed in a way that may be a momentary stall, gets a
// second probe started beside it: the node is alive if either answers, and the
// stalled probe is kept in its history so url-test ranks the node below steady
// ones. A failure that is an answer (closed port, missing name, unexpected
// status) marks the node dead at once, and so do two failed probes.
func (hc *HealthCheck) probe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) probeOutcome {
	recorder, ok := p.(C.ProbeRecorder)
	if !ok {
		ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
		defer cancel()
		_, _ = p.URLTest(ctx, url, expectedStatus)
		return probePassed
	}

	key := probeFlightKey(p, url, expectedStatus)
	probeFlights.Lock()
	if flight, running := probeFlights.m[key]; running {
		probeFlights.Unlock()
		select {
		case <-flight.done:
			log.Debugln("[Проба] %s: проба уже шла у другой группы, взят её результат", p.Name())
			return probeShared
		case <-hc.ctx.Done():
			return probeCancelled
		}
	}
	flight := &probeFlight{done: make(chan struct{})}
	probeFlights.m[key] = flight
	probeFlights.Unlock()
	outcome := hc.probeOnce(p, recorder, url, expectedStatus)
	probeFlights.Lock()
	delete(probeFlights.m, key)
	probeFlights.Unlock()
	flight.outcome = outcome
	close(flight.done)
	return outcome
}

// probeOnce is the probe itself, with the hedge; probe wraps it so that one
// node is probed once at a time whichever groups ask.
func (hc *HealthCheck) probeOnce(p C.Proxy, recorder C.ProbeRecorder, url string, expectedStatus utils.IntRanges[uint16]) probeOutcome {
	first := hc.startProbe(p, url, expectedStatus)
	defer first.cancel()
	hedge := time.NewTimer(probeHedgeDelay)
	defer hedge.Stop()

	select {
	case <-first.done:
		if hc.ctx.Err() != nil || !first.result.Held {
			return probeCancelled
		}
		if first.result.OK() {
			recorder.RecordProbe(url, first.result)
			return probePassed
		}
		if !first.result.Retryable() {
			recorder.RecordProbe(url, first.result)
			log.Warnln("[Проба] %s: не отвечает (%s)", p.Name(), first.result)
			return probeRefused
		}
		// A quick failure that may be a lost packet: the second probe still
		// waits out the hedge delay rather than repeating the failure at once.
		select {
		case <-hedge.C:
		case <-hc.ctx.Done():
			return probeCancelled
		}
	case <-hedge.C:
	case <-hc.ctx.Done():
		return probeCancelled
	}

	second := hc.startProbe(p, url, expectedStatus)
	defer second.cancel()
	// A closed channel is always ready: once a probe has reported, its channel
	// is set aside so the select waits for the other one.
	firstDone, secondDone := first.done, second.done
	for {
		select {
		case <-firstDone:
			firstDone = nil
		case <-secondDone:
			secondDone = nil
		case <-hc.ctx.Done():
			return probeCancelled
		}
		if hc.ctx.Err() != nil {
			return probeCancelled
		}
		if first.finished() && first.result.Held && first.result.OK() {
			// The node answered its own probe, only slower than the hedge;
			// the second probe is dropped, its outcome decides nothing.
			recorder.RecordProbe(url, first.result)
			return probePassed
		}
		if second.finished() && second.result.Held && second.result.OK() {
			recorder.RecordSoftFailure(url, first.started)
			recorder.RecordProbe(url, second.result)
			log.Warnln("[Проба] %s: первая проба не ответила за %d мс, повторная прошла: %s",
				p.Name(), time.Since(first.started).Milliseconds(), second.result)
			return probeRecovered
		}
		if first.finished() && second.finished() {
			if !first.result.Held || !second.result.Held {
				return probeCancelled
			}
			recorder.RecordProbe(url, first.result)
			recorder.RecordProbe(url, second.result)
			log.Warnln("[Проба] %s: не отвечает (%s), повторная проба тоже (%s)", p.Name(), first.result, second.result)
			if !second.result.Retryable() {
				return probeRefused
			}
			return probeStalled
		}
	}
}

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
