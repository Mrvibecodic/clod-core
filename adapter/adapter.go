package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/queue"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
)

var UnifiedDelay = atomic.NewBool(false)

const (
	defaultHistoriesNum = 10
)

type internalProxyState struct {
	alive   atomic.Bool
	history *queue.Queue[C.DelayHistory]
	// elapsed is how long the last probe took as a whole, dial and handshake
	// included: the unified delay counts only its last request.
	elapsed atomic.Int64
}

type Proxy struct {
	C.ProxyAdapter
	alive   atomic.Bool
	history *queue.Queue[C.DelayHistory]
	extra   xsync.Map[string, *internalProxyState]
	// fingerprint is set for the nodes parsed from a config or a provider:
	// groups, DIRECT and REJECT have none.
	fingerprint string
	// node — see Node; only the nodes parsed from a config or a provider.
	node *Node
}

// Fingerprint identifies a node by how it connects; see fingerprint. It is
// empty for groups and the built-in proxies.
func (p *Proxy) Fingerprint() string {
	return p.fingerprint
}

// Adapter implements C.Proxy
func (p *Proxy) Adapter() C.ProxyAdapter {
	return p.ProxyAdapter
}

// AliveForTestUrl implements C.Proxy
func (p *Proxy) AliveForTestUrl(url string) bool {
	if state, ok := p.extra.Load(url); ok {
		return state.alive.Load()
	}

	return p.alive.Load()
}

// DialContext implements C.ProxyAdapter
func (p *Proxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	conn, err := p.ProxyAdapter.DialContext(ctx, metadata)
	return conn, err
}

// ListenPacketContext implements C.ProxyAdapter
func (p *Proxy) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	pc, err := p.ProxyAdapter.ListenPacketContext(ctx, metadata)
	return pc, err
}

// DelayHistory implements C.Proxy
func (p *Proxy) DelayHistory() []C.DelayHistory {
	queueM := p.history.Copy()
	histories := []C.DelayHistory{}
	for _, item := range queueM {
		histories = append(histories, item)
	}
	return histories
}

// DelayHistoryForTestUrl implements C.Proxy
func (p *Proxy) DelayHistoryForTestUrl(url string) []C.DelayHistory {
	var queueM []C.DelayHistory

	if state, ok := p.extra.Load(url); ok {
		queueM = state.history.Copy()
	}
	histories := []C.DelayHistory{}
	for _, item := range queueM {
		histories = append(histories, item)
	}
	return histories
}

// ExtraDelayHistories return all delay histories for each test URL
// implements C.Proxy
func (p *Proxy) ExtraDelayHistories() map[string]C.ProxyState {
	histories := map[string]C.ProxyState{}

	p.extra.Range(func(k string, v *internalProxyState) bool {
		testUrl := k
		state := v

		queueM := state.history.Copy()
		var history []C.DelayHistory

		for _, item := range queueM {
			history = append(history, item)
		}

		histories[testUrl] = C.ProxyState{
			Alive:   state.alive.Load(),
			History: history,
		}
		return true
	})
	return histories
}

// LastDelayForTestUrl return last history record of the specified URL. if proxy is not alive, return the max value of uint16.
// implements C.Proxy
func (p *Proxy) LastDelayForTestUrl(url string) (delay uint16) {
	var maxDelay uint16 = 0xffff

	alive := false
	var history C.DelayHistory

	if state, ok := p.extra.Load(url); ok {
		alive = state.alive.Load()
		history = state.history.Last()
	}

	if !alive || history.Delay == 0 {
		return maxDelay
	}
	return history.Delay
}

// MarshalJSON implements C.ProxyAdapter
func (p *Proxy) MarshalJSON() ([]byte, error) {
	inner, err := p.ProxyAdapter.MarshalJSON()
	if err != nil {
		return inner, err
	}

	mapping := map[string]any{}
	_ = json.Unmarshal(inner, &mapping)
	mapping["history"] = p.DelayHistory()
	mapping["extra"] = p.ExtraDelayHistories()
	mapping["alive"] = p.alive.Load()
	mapping["name"] = p.Name()
	mapping["udp"] = p.SupportUDP()
	mapping["uot"] = p.SupportUOT()

	proxyInfo := p.ProxyInfo()
	mapping["xudp"] = proxyInfo.XUDP
	mapping["tfo"] = proxyInfo.TFO
	mapping["mptcp"] = proxyInfo.MPTCP
	mapping["smux"] = proxyInfo.SMUX
	mapping["interface"] = proxyInfo.Interface
	mapping["routing-mark"] = proxyInfo.RoutingMark
	mapping["provider-name"] = proxyInfo.ProviderName
	mapping["dialer-proxy"] = proxyInfo.DialerProxy
	if p.fingerprint != "" {
		mapping["fingerprint"] = p.fingerprint
	}

	return json.Marshal(mapping)
}

// URLTest get the delay for the specified URL
// implements C.Proxy
func (p *Proxy) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (t uint16, err error) {
	var satisfied bool
	var status int
	stage := C.ProbeStageAddress

	// Probes to one host are spaced out, and the wait is not part of the
	// measurement: the delay and the elapsed time start with the probe itself.
	if err = C.ProbePace(ctx, C.ProbeHostOf(p)); err != nil {
		// The caller gave up while the probe was waiting for its turn: nothing
		// was measured, so nothing is recorded.
		if held := C.HeldProbe(ctx); held != nil {
			*held = C.ProbeResult{}
		}
		return 0, err
	}
	began := time.Now()

	defer func() {
		// A failure under the probe hold (network switching, process was
		// paused) or of a cancelled probe says nothing about the node: it is
		// neither recorded nor handed to the health check as a result.
		failed := err != nil || !satisfied
		if failed && (ctx.Err() == context.Canceled || C.ProbeHolding(began) || C.ProbeHolding(time.Now())) {
			return
		}
		if held := C.HeldProbe(ctx); held != nil {
			*held = C.ProbeResult{
				Held:      true,
				Time:      time.Now(),
				Delay:     t,
				Err:       err,
				Satisfied: satisfied,
				Status:    status,
				Stage:     stage,
				Elapsed:   time.Since(began),
			}
			return
		}
		p.recordURLTest(url, t, err, satisfied, time.Now(), time.Since(began))
	}()

	unifiedDelay := UnifiedDelay.Load()

	addr, err := urlToMetadata(url)
	if err != nil {
		return
	}

	stage = C.ProbeStageDial
	start := time.Now()
	instance, err := p.DialContext(ctx, &addr)
	if err != nil {
		return
	}
	stage = C.ProbeStageRequest
	defer func() {
		_ = instance.Close()
	}()

	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return
	}
	req = req.WithContext(ctx)

	tlsConfig, err := ca.GetTLSConfig(ca.Option{})
	if err != nil {
		return
	}

	transport := &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return instance, nil
		},
		// from http.DefaultTransport
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
	}

	client := http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	defer client.CloseIdleConnections()

	resp, err := client.Do(req)

	if err != nil {
		return
	}

	_ = resp.Body.Close()

	if unifiedDelay {
		second := time.Now()
		var ignoredErr error
		var secondResp *http.Response
		secondResp, ignoredErr = client.Do(req)
		if ignoredErr == nil {
			resp = secondResp
			_ = resp.Body.Close()
			start = second
		} else {
			if strings.HasPrefix(url, "http://") {
				log.Errorln("%s failed to get the second response from %s: %v", p.Name(), url, ignoredErr)
				log.Warnln("It is recommended to use HTTPS for provider.health-check.url and group.url to ensure better reliability. Due to some proxy providers hijacking test addresses and not being compatible with repeated HEAD requests, using HTTP may result in failed tests.")
			}
		}
	}

	satisfied = resp != nil && (expectedStatus == nil || expectedStatus.Check(uint16(resp.StatusCode)))
	if resp != nil {
		status = resp.StatusCode
	}
	if !satisfied {
		stage = C.ProbeStageStatus
	}
	t = uint16(time.Since(start) / time.Millisecond)
	return
}

// recordURLTest writes the outcome of a probe of url into the history.
func (p *Proxy) recordURLTest(url string, t uint16, err error, satisfied bool, at time.Time, elapsed time.Duration) {
	alive := err == nil
	record := C.DelayHistory{Time: at}
	if alive {
		record.Delay = t
	}

	p.alive.Store(alive)
	p.putHistory(p.history, record)

	state := p.stateForTestUrl(url)

	if !satisfied {
		record.Delay = 0
		alive = false
	}

	state.alive.Store(alive)
	state.elapsed.Store(int64(elapsed))
	p.putHistory(state.history, record)
}

// RecordProbe implements C.ProbeRecorder
func (p *Proxy) RecordProbe(url string, result *C.ProbeResult) {
	p.recordURLTest(url, result.Delay, result.Err, result.Satisfied, result.Time, result.Elapsed)
}

// LastProbeElapsed implements C.ProbeRecorder
func (p *Proxy) LastProbeElapsed(url string) time.Duration {
	if state, ok := p.extra.Load(url); ok {
		return time.Duration(state.elapsed.Load())
	}
	return 0
}

// RecordSoftFailure implements C.ProbeRecorder
func (p *Proxy) RecordSoftFailure(url string, at time.Time) {
	// A probe that hung while the network was switching says nothing about
	// the node, as with a failed one.
	if C.ProbeHolding(at) || C.ProbeHolding(time.Now()) {
		return
	}
	record := C.DelayHistory{Time: at}
	p.putHistory(p.history, record)
	p.putHistory(p.stateForTestUrl(url).history, record)
}

// Inherit takes over what the checks know of old: a provider update replaces
// the objects of the nodes it keeps, and the new ones would start unchecked
// and alive. The state is copied: a probe of old still in flight stays with
// old.
func (p *Proxy) Inherit(old C.Proxy) {
	o, ok := old.(*Proxy)
	if !ok {
		return
	}
	p.alive.Store(o.alive.Load())
	p.copyHistory(p.history, o.history)
	o.extra.Range(func(url string, state *internalProxyState) bool {
		copied := p.stateForTestUrl(url)
		copied.alive.Store(state.alive.Load())
		copied.elapsed.Store(state.elapsed.Load())
		p.copyHistory(copied.history, state.history)
		return true
	})
}

func (p *Proxy) copyHistory(dst, src *queue.Queue[C.DelayHistory]) {
	for _, record := range src.Copy() {
		p.putHistory(dst, record)
	}
}

func (p *Proxy) stateForTestUrl(url string) *internalProxyState {
	state, _ := p.extra.LoadOrStoreFn(url, func() *internalProxyState {
		return &internalProxyState{
			history: queue.New[C.DelayHistory](defaultHistoriesNum),
			alive:   atomic.NewBool(true),
		}
	})
	return state
}

func (p *Proxy) putHistory(history *queue.Queue[C.DelayHistory], record C.DelayHistory) {
	history.Put(record)
	if history.Len() > defaultHistoriesNum {
		history.Pop()
	}
}

func NewProxy(adapter C.ProxyAdapter) *Proxy {
	return &Proxy{
		ProxyAdapter: adapter,
		history:      queue.New[C.DelayHistory](defaultHistoriesNum),
		alive:        atomic.NewBool(true),
	}
}

func urlToMetadata(rawURL string) (addr C.Metadata, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			err = fmt.Errorf("%s scheme not Support", rawURL)
			return
		}
	}

	err = addr.SetRemoteAddress(net.JoinHostPort(u.Hostname(), port))
	return
}
