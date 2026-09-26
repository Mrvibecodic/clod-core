package provider

import (
	"context"
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
			if outcome == probeCancelled {
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

// probeConfirmDelay is the pause before the probe that confirms a failure:
// long enough to outlast a lost packet or a momentary stall, short enough for
// a node that really went down to leave the group within one check.
const probeConfirmDelay = time.Second

type probeOutcome int

const (
	probeCancelled probeOutcome = iota
	probePassed
	probeRecovered // stalled, passed on the second probe
	probeStalled   // stalled on both probes
	probeRefused   // failed with an answer: closed port, missing name, bad status
)

// probeTally counts the outcomes of one health check round. Many nodes stalling
// on their first probe in the same round point at the local side (network,
// DNS, the way to the servers) rather than at the nodes.
type probeTally struct {
	total, recovered, stalled atomic.Int32
}

// probe tests p and records the result. A failure that may be a momentary
// stall is checked by a second probe: if that one passes, the node stays alive,
// but the failed probe is kept in its history so url-test ranks the node below
// steady ones. A failure that is an answer (closed port, missing name,
// unexpected status) or that the second probe confirms marks the node dead.
func (hc *HealthCheck) probe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) probeOutcome {
	recorder, ok := p.(C.ProbeRecorder)
	if !ok {
		ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
		defer cancel()
		_, _ = p.URLTest(ctx, url, expectedStatus)
		return probePassed
	}

	first := hc.heldProbe(p, url, expectedStatus)
	if !first.Held || hc.ctx.Err() != nil {
		return probeCancelled
	}
	if first.OK() {
		recorder.RecordProbe(url, first)
		return probePassed
	}
	if !first.Retryable() {
		recorder.RecordProbe(url, first)
		log.Warnln("[Проба] %s: не отвечает (%s)", p.Name(), first)
		return probeRefused
	}

	timer := time.NewTimer(probeConfirmDelay)
	select {
	case <-timer.C:
	case <-hc.ctx.Done():
		timer.Stop()
		return probeCancelled
	}

	second := hc.heldProbe(p, url, expectedStatus)
	if !second.Held || hc.ctx.Err() != nil {
		return probeCancelled
	}
	if second.OK() {
		recorder.RecordSoftFailure(url, first.Time)
		recorder.RecordProbe(url, second)
		log.Warnln("[Проба] %s: первая проба не прошла (%s), повторная прошла: %s", p.Name(), first, second)
		return probeRecovered
	}
	recorder.RecordProbe(url, first)
	recorder.RecordProbe(url, second)
	log.Warnln("[Проба] %s: не отвечает (%s), повторная проба тоже (%s)", p.Name(), first, second)
	return probeStalled
}

func (hc *HealthCheck) heldProbe(p C.Proxy, url string, expectedStatus utils.IntRanges[uint16]) *C.ProbeResult {
	ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
	defer cancel()
	ctx, held := C.WithHeldProbe(ctx)
	_, _ = p.URLTest(ctx, url, expectedStatus)
	return held
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
