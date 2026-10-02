package adapter

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

// Verdicts of a download check.
const (
	// DownloadOK: the whole body came through the node.
	DownloadOK = "ok"
	// DownloadFrozen: something came through the node, then nothing for the
	// stall time: the traffic is cut after the first kilobytes.
	DownloadFrozen = "frozen"
	// DownloadDead: nothing came through the node, and a plain probe through
	// it right after failed as well.
	DownloadDead = "dead"
	// DownloadUnknown: the check says nothing about the node — the site
	// answered with an error, the node passes the probe but not the download,
	// the data was still coming when the time ran out.
	DownloadUnknown = "unknown"
)

// DownloadPingTimeout is how long the plain probe after an empty download
// may take.
const DownloadPingTimeout = 5 * time.Second

// DownloadResult is the outcome of Proxy.DownloadCheck.
type DownloadResult struct {
	Verdict string `json:"verdict"`
	// Received counts every byte that came through the node, the TLS
	// handshake and the headers included.
	Received int64 `json:"received"`
	// Status is the status code of the answer, 0 if there was none.
	Status int `json:"status"`
	// Elapsed is how long the check took, in milliseconds.
	Elapsed int64 `json:"elapsed"`
}

var errDownloadStalled = errors.New("no data for the stall time")

// countingConn counts what is read through the node and when it last came.
type countingConn struct {
	net.Conn
	received atomic.Int64
	last     atomic.Int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.received.Add(int64(n))
		c.last.Store(time.Now().UnixNano())
	}
	return n, err
}

// DownloadCheck downloads size bytes of url through the node and tells
// whether the traffic passes, is cut after the first kilobytes or does not
// pass at all. The download may take timeout; it is cut when nothing came for
// stall after the first bytes. When not a byte came, a plain probe of pingURL
// through the node decides: it passes — the node is alive and the verdict is
// unknown, it fails — the node is dead. The node's history is left alone. The
// check waits for its turn to the node's host as probes do.
func (p *Proxy) DownloadCheck(ctx context.Context, url string, size int64, timeout, stall time.Duration, pingURL string) (result DownloadResult) {
	result.Verdict = DownloadUnknown
	if err := C.ProbePace(ctx, C.ProbeHostOf(p)); err != nil {
		return
	}
	began := time.Now()
	defer func() {
		result.Elapsed = time.Since(began).Milliseconds()
	}()

	var conn *countingConn
	verdict, status := p.download(ctx, url, size, timeout, stall, &conn)
	result.Status = status
	if conn != nil {
		result.Received = conn.received.Load()
	}
	// A failure under the probe hold (network switching, process was paused)
	// says nothing about the node.
	failed := verdict == DownloadFrozen || verdict == DownloadDead
	if failed && (C.ProbeHolding(began) || C.ProbeHolding(time.Now())) {
		return
	}
	if verdict != DownloadDead {
		result.Verdict = verdict
		return
	}

	if pingURL == "" {
		pingURL = C.DefaultTestURL
	}
	pingCtx, cancel := context.WithTimeout(ctx, DownloadPingTimeout+C.ProbeReserve)
	defer cancel()
	pingCtx, held := C.WithHeldProbe(pingCtx)
	_, _ = p.URLTest(pingCtx, pingURL, nil)
	switch {
	case !held.Held:
		// The probe did not run or its failure says nothing.
	case held.Err == nil:
		// Any answer: traffic passes the node, the download site does not.
	case ctx.Err() == nil:
		// The probe failed on its own, not because the caller gave up.
		result.Verdict = DownloadDead
	}
	return
}

// download runs the download itself. Its verdict DownloadDead means only that
// not a byte came through the node.
func (p *Proxy) download(ctx context.Context, url string, size int64, timeout, stall time.Duration, counted **countingConn) (verdict string, status int) {
	addr, err := urlToMetadata(url)
	if err != nil {
		return DownloadUnknown, 0
	}
	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	instance, err := p.DialContext(ctx, &addr)
	if err != nil {
		return DownloadDead, 0
	}
	conn := &countingConn{Conn: instance}
	*counted = conn
	defer func() {
		_ = conn.Close()
	}()

	go watchStall(ctx, conn, stall, cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return DownloadUnknown, 0
	}
	// Compressed zeros would not make the size, and a cached copy would not
	// come through the node.
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")

	tlsConfig, err := ca.GetTLSConfig(ca.Option{})
	if err != nil {
		return DownloadUnknown, 0
	}
	transport := &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return conn, nil
		},
		DisableCompression: true,
		DisableKeepAlives:  true,
		TLSClientConfig:    tlsConfig,
	}
	defer transport.CloseIdleConnections()
	client := http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	failed := func() string {
		switch {
		case context.Cause(ctx) == errDownloadStalled:
			return DownloadFrozen
		case conn.received.Load() == 0:
			return DownloadDead
		}
		return DownloadUnknown
	}

	resp, err := client.Do(req)
	if err != nil {
		// Before the answer only a stall counts as a cut: a TLS error or
		// a closed connection with some bytes in is something else.
		return failed(), 0
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return DownloadUnknown, resp.StatusCode
	}

	_, err = io.CopyN(io.Discard, resp.Body, size)
	switch {
	case err == nil:
		return DownloadOK, resp.StatusCode
	case errors.Is(err, io.EOF):
		// The body ended before the size, whole: the site gave less than
		// asked, the check did not check what it meant to.
		return DownloadUnknown, resp.StatusCode
	case ctx.Err() == nil || context.Cause(ctx) == errDownloadStalled:
		// The body broke off on its way: the connection was cut.
		return DownloadFrozen, resp.StatusCode
	}
	// The time ran out while the data was still coming.
	return failed(), resp.StatusCode
}

// watchStall cancels the download when nothing came for stall after the first
// bytes.
func watchStall(ctx context.Context, conn *countingConn, stall time.Duration, cancel context.CancelCauseFunc) {
	tick := stall / 4
	if tick > 250*time.Millisecond {
		tick = 250 * time.Millisecond
	}
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			last := conn.last.Load()
			if last != 0 && now.Sub(time.Unix(0, last)) >= stall {
				cancel(errDownloadStalled)
				_ = conn.Close()
				return
			}
		}
	}
}
