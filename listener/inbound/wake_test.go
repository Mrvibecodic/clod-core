package inbound_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"

	"github.com/metacubex/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sleepyRelay stands between a proxy and its server the way the path does
// across a sleep: flows that were open before it get no packets through in
// either direction afterwards, new ones work.
type sleepyRelay struct {
	front  *net.UDPConn
	server *net.UDPAddr

	mu    sync.Mutex
	flows map[netip.AddrPort]*relayFlow
}

type relayFlow struct {
	back *net.UDPConn
	dead bool
}

func newSleepyRelay(t *testing.T, server string) *sleepyRelay {
	serverAddr, err := net.ResolveUDPAddr("udp", server)
	require.NoError(t, err)
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	r := &sleepyRelay{front: front, server: serverAddr, flows: map[netip.AddrPort]*relayFlow{}}
	t.Cleanup(r.close)
	go r.serve()
	return r
}

func (r *sleepyRelay) serve() {
	buf := make([]byte, 64*1024)
	for {
		n, client, err := r.front.ReadFromUDPAddrPort(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			continue
		}
		if flow := r.flow(client); flow != nil {
			_, _ = flow.back.Write(buf[:n])
		}
	}
}

func (r *sleepyRelay) flow(client netip.AddrPort) *relayFlow {
	r.mu.Lock()
	defer r.mu.Unlock()

	flow := r.flows[client]
	if flow == nil {
		back, err := net.DialUDP("udp", nil, r.server)
		if err != nil {
			return nil
		}
		flow = &relayFlow{back: back}
		r.flows[client] = flow
		go r.replies(client, flow)
	}
	if flow.dead {
		return nil
	}
	return flow
}

func (r *sleepyRelay) replies(client netip.AddrPort, flow *relayFlow) {
	buf := make([]byte, 64*1024)
	for {
		n, err := flow.back.Read(buf)
		if err != nil {
			return
		}
		r.mu.Lock()
		dead := flow.dead
		r.mu.Unlock()
		if !dead {
			_, _ = r.front.WriteToUDPAddrPort(buf[:n], client)
		}
	}
}

func (r *sleepyRelay) sleep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, flow := range r.flows {
		flow.dead = true
	}
}

func (r *sleepyRelay) opened() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.flows)
}

func (r *sleepyRelay) close() {
	_ = r.front.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, flow := range r.flows {
		_ = flow.back.Close()
	}
}

func requestThrough(proxy C.ProxyAdapter, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := proxy.DialContext(ctx, &C.Metadata{NetWork: C.TCP, DstIP: remoteAddr, DstPort: 80})
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s%s?size=1", remoteAddr, httpPath), bytes.NewReader(httpData[:1]))
	if err != nil {
		return err
	}
	if err = req.Write(conn); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// Not parallel: the sleep is process wide.
func TestASessionKeptAcrossASleepIsNotUsedAgain(t *testing.T) {
	servers := map[string]func(t *testing.T, tunnel *TestTunnel) (C.InboundListener, func(netip.AddrPort) (C.ProxyAdapter, error)){
		"hysteria2": func(t *testing.T, tunnel *TestTunnel) (C.InboundListener, func(netip.AddrPort) (C.ProxyAdapter, error)) {
			in, err := inbound.NewHysteria2(&inbound.Hysteria2Option{
				BaseOption:  inbound.BaseOption{NameStr: "hysteria2_inbound", Listen: "127.0.0.1", Port: "0"},
				Users:       map[string]string{"test": userUUID},
				Certificate: tlsCertificate,
				PrivateKey:  tlsPrivateKey,
			})
			require.NoError(t, err)
			return in, func(server netip.AddrPort) (C.ProxyAdapter, error) {
				option := outbound.Hysteria2Option{
					Name:        "hysteria2_outbound",
					Server:      server.Addr().String(),
					Port:        int(server.Port()),
					Password:    userUUID,
					Fingerprint: tlsFingerprint,
				}
				option.DialerForAPI = tunnel.NewDialer()
				option.TunnelForAPI = tunnel
				return outbound.NewHysteria2(option)
			}
		},
		"tuic": func(t *testing.T, tunnel *TestTunnel) (C.InboundListener, func(netip.AddrPort) (C.ProxyAdapter, error)) {
			in, err := inbound.NewTuic(&inbound.TuicOption{
				BaseOption:            inbound.BaseOption{NameStr: "tuic_inbound", Listen: "127.0.0.1", Port: "0"},
				Users:                 map[string]string{userUUID: userUUID},
				Certificate:           tlsCertificate,
				PrivateKey:            tlsPrivateKey,
				AuthenticationTimeout: 5000,
			})
			require.NoError(t, err)
			return in, func(server netip.AddrPort) (C.ProxyAdapter, error) {
				option := outbound.TuicOption{
					Name:        "tuic_outbound",
					Server:      server.Addr().String(),
					Port:        int(server.Port()),
					UUID:        userUUID,
					Password:    userUUID,
					Fingerprint: tlsFingerprint,
				}
				option.DialerForAPI = tunnel.NewDialer()
				option.TunnelForAPI = tunnel
				return outbound.NewTuic(option)
			}
		},
	}

	for name, start := range servers {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() {
				C.ProbeBeat(0)
				C.SetProbeHoldUntil(time.Time{})
			})
			C.ProbeBeat(time.Now().UnixNano())

			tunnel := NewHttpTestTunnel()
			defer tunnel.Close()

			in, connect := start(t, tunnel)
			require.NoError(t, in.Listen(tunnel))
			defer in.Close()

			relay := newSleepyRelay(t, in.Address())
			out, err := connect(relay.front.LocalAddr().(*net.UDPAddr).AddrPort())
			require.NoError(t, err)
			defer out.Close()

			require.NoError(t, requestThrough(out, 5*time.Second))
			require.NoError(t, requestThrough(out, 5*time.Second))
			require.Equal(t, 1, relay.opened(), "an awake process keeps one session")

			relay.sleep()
			C.ProbeBeat(time.Now().Add(-time.Minute).UnixNano())

			var wg sync.WaitGroup
			errs := make([]error, 8)
			for i := range errs {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = requestThrough(out, 5*time.Second)
				}()
			}
			wg.Wait()
			for _, err := range errs {
				assert.NoError(t, err, "the first connections after a sleep must not go into the session it left behind")
			}

			// A pool may open more than one session for dials that come together
			afterSleep := relay.opened()
			assert.Greater(t, afterSleep, 1, "the session the sleep left behind must be replaced")

			require.NoError(t, requestThrough(out, 5*time.Second))
			require.NoError(t, requestThrough(out, 5*time.Second))
			assert.Equal(t, afterSleep, relay.opened(), "a sleep is paid for once, not on every dial after it")
		})
	}
}
