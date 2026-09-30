package route

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

// stallOnceTarget never answers the first connection and answers 204 on the
// next ones.
func stallOnceTarget(t *testing.T) (string, func() int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	conns := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns++
			first := conns == 1
			if first {
				held = append(held, conn)
			}
			mu.Unlock()
			if first {
				continue
			}
			go func() {
				defer conn.Close()
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				// Loopback answers within a millisecond, a delay of 0 means failed.
				time.Sleep(2 * time.Millisecond)
				_, _ = conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	return "http://" + listener.Addr().String() + "/generate_204", func() int {
		mu.Lock()
		defer mu.Unlock()
		return conns
	}
}

func TestADelayRequestConfirmsAFailureLikeTheScheduledCheck(t *testing.T) {
	url, conns := stallOnceTarget(t)
	proxy := adapter.NewProxy(outbound.NewDirect())
	req := httptest.NewRequest(http.MethodGet, "/proxies/DIRECT/delay?timeout=3000&url="+url, nil)
	req = req.WithContext(context.WithValue(req.Context(), CtxKeyProxy, proxy))
	rec := httptest.NewRecorder()
	getProxyDelay(rec, req)
	if rec.Code != http.StatusOK || !proxy.AliveForTestUrl(url) || conns() != 2 {
		t.Fatalf("code %d, body %s, alive %t, connections %d", rec.Code, rec.Body.String(), proxy.AliveForTestUrl(url), conns())
	}
}
