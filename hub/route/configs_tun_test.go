//go:build linux

package route

import (
	"bytes"
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestPatchDuringTunCreationKeepsTheTun(t *testing.T) {
	conf := LC.Tun{
		Enable:       true,
		Device:       "clodtest0",
		Stack:        C.TunGvisor,
		DNSHijack:    []string{"0.0.0.0:53"},
		MTU:          9000,
		Inet4Address: []netip.Prefix{netip.MustParsePrefix("198.18.0.1/30")},
	}
	t.Cleanup(func() { listener.ReCreateTun(LC.Tun{}, tunnel.Tunnel) })

	listener.ReCreateTun(conf, tunnel.Tunnel)
	if !listener.GetTunConf().Enable {
		t.Skip("the TUN device cannot be created here")
	}

	for round := 0; round < 20; round++ {
		listener.ReCreateTun(LC.Tun{}, tunnel.Tunnel)

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				request := httptest.NewRequest(http.MethodPatch, "/configs", bytes.NewBufferString(`{"mode":"rule"}`))
				request.Header.Set("Content-Type", "application/json")
				patchConfigs(httptest.NewRecorder(), request)
			}
		}()
		listener.ReCreateTun(conf, tunnel.Tunnel)
		close(stop)
		<-done

		if !listener.GetTunConf().Enable {
			t.Fatalf("round %d: a PATCH that did not mention the TUN closed the device that was being created", round)
		}
	}
}
