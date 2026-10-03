package route

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
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

// connectProxy is an HTTP proxy that tunnels CONNECT to wherever it is asked.
func connectProxy(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				req, err := http.ReadRequest(reader)
				if err != nil || req.Method != http.MethodConnect {
					return
				}
				upstream, err := net.Dial("tcp", req.Host)
				if err != nil {
					return
				}
				defer upstream.Close()
				_, _ = conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
				go func() { _, _ = io.Copy(upstream, reader) }()
				_, _ = io.Copy(conn, upstream)
			}()
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

func downloadRequest(proxy any, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/proxies/node/download?"+query, nil)
	req = req.WithContext(context.WithValue(req.Context(), CtxKeyProxy, proxy))
	rec := httptest.NewRecorder()
	getProxyDownload(rec, req)
	return rec
}

func TestADownloadCheckIsOnlyForNodes(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 1024))
	}))
	defer target.Close()
	node, err := adapter.ParseProxy(map[string]any{"name": "node", "type": "http", "server": "127.0.0.1", "port": connectProxy(t)})
	if err != nil {
		t.Fatal(err)
	}
	withoutVPN, err := adapter.ParseProxy(map[string]any{"name": "without VPN", "type": "direct"})
	if err != nil {
		t.Fatal(err)
	}
	query := "size=1024&timeout=2000&stall=500&url=" + target.URL

	if rec := downloadRequest(adapter.NewProxy(outbound.NewDirect()), query); rec.Code != http.StatusBadRequest {
		t.Fatalf("DIRECT: code %d, body %s", rec.Code, rec.Body.String())
	}
	if rec := downloadRequest(withoutVPN, query); rec.Code != http.StatusBadRequest {
		t.Fatalf("type direct: code %d, body %s", rec.Code, rec.Body.String())
	}
	for _, bad := range []string{
		"size=2000000&timeout=2000&stall=500&url=" + target.URL,
		"size=1024&timeout=2000&stall=3000&url=" + target.URL,
		"size=1024&timeout=2000&stall=500&url=ftp://example.com/",
		"size=1024&stall=500&url=" + target.URL,
	} {
		if rec := downloadRequest(node, bad); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code %d, body %s", bad, rec.Code, rec.Body.String())
		}
	}
	rec := downloadRequest(node, query)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"verdict":"ok"`) {
		t.Fatalf("code %d, body %s", rec.Code, rec.Body.String())
	}
}
