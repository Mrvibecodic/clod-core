package adapter

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

const testSize = 64 << 10

// downloadTarget serves every connection with answer and keeps the
// connection open until the test ends.
func downloadTarget(t *testing.T, answer func(conn net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go answer(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return "http://" + listener.Addr().String() + "/__down"
}

func readRequest(conn net.Conn) bool {
	_, err := http.ReadRequest(bufio.NewReader(conn))
	return err == nil
}

func serveBody(bytes int) func(conn net.Conn) {
	return func(conn net.Conn) {
		if !readRequest(conn) {
			return
		}
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", testSize)
		_, _ = conn.Write(make([]byte, bytes))
	}
}

func serveStatus(status int) func(conn net.Conn) {
	return func(conn net.Conn) {
		if readRequest(conn) {
			_, _ = fmt.Fprintf(conn, "HTTP/1.1 %d Status\r\nContent-Length: 0\r\n\r\n", status)
		}
	}
}

// closedURL is an address nothing listens on.
func closedURL(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return "http://" + addr + "/generate_204"
}

func directNode(t *testing.T) *Proxy {
	t.Helper()
	proxy, err := ParseProxy(map[string]any{"name": "node", "type": "direct"})
	if err != nil {
		t.Fatal(err)
	}
	return proxy.(*Proxy)
}

func TestADownloadCheckTellsTheVerdicts(t *testing.T) {
	alive := downloadTarget(t, serveStatus(http.StatusNoContent))
	cases := []struct {
		name    string
		answer  func(conn net.Conn)
		ping    string
		verdict string
	}{
		{"the whole body", serveBody(testSize), "", DownloadOK},
		{"cut after 16 KB", serveBody(16 << 10), "", DownloadFrozen},
		{"cut before the headers", func(conn net.Conn) {
			if readRequest(conn) {
				_, _ = conn.Write([]byte("HTTP/1.1 2"))
			}
		}, "", DownloadFrozen},
		{"silent, the probe fails", func(net.Conn) {}, closedURL(t), DownloadDead},
		{"silent, the probe passes", func(net.Conn) {}, alive, DownloadUnknown},
		{"429", serveStatus(http.StatusTooManyRequests), "", DownloadUnknown},
		{"a whole body shorter than asked", func(conn net.Conn) {
			if readRequest(conn) {
				_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\n%s", make([]byte, 1024))
			}
		}, "", DownloadUnknown},
		{"cut with a clean close", func(conn net.Conn) {
			serveBody(16 << 10)(conn)
			_ = conn.Close()
		}, "", DownloadFrozen},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := directNode(t)
			url := downloadTarget(t, c.answer)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result := node.DownloadCheck(ctx, url, testSize, 1500*time.Millisecond, 300*time.Millisecond, c.ping)
			if result.Verdict != c.verdict {
				t.Fatalf("verdict %+v, want %s", result, c.verdict)
			}
			if len(node.DelayHistory()) != 0 || len(node.ExtraDelayHistories()) != 0 {
				t.Fatalf("the check wrote the history %v %v", node.DelayHistory(), node.ExtraDelayHistories())
			}
		})
	}
}

func TestACheckTheCallerGaveUpOnSaysNothing(t *testing.T) {
	url := downloadTarget(t, func(net.Conn) {})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	result := directNode(t).DownloadCheck(ctx, url, testSize, 2*time.Second, 300*time.Millisecond, closedURL(t))
	if result.Verdict != DownloadUnknown {
		t.Fatalf("result %+v", result)
	}
}

func TestADownloadCheckCountsTheBytesThatCame(t *testing.T) {
	url := downloadTarget(t, serveBody(16<<10))
	result := directNode(t).DownloadCheck(context.Background(), url, testSize, 2*time.Second, 300*time.Millisecond, "")
	if result.Received < 16<<10 || result.Received >= testSize || result.Status != http.StatusOK || result.Verdict != DownloadFrozen {
		t.Fatalf("result %+v", result)
	}
}

func TestACutUnderTheProbeHoldSaysNothing(t *testing.T) {
	url := downloadTarget(t, serveBody(16<<10))
	C.SetProbeHoldUntil(time.Now().Add(time.Minute))
	defer C.SetProbeHoldUntil(time.Time{})
	result := directNode(t).DownloadCheck(context.Background(), url, testSize, 2*time.Second, 300*time.Millisecond, "")
	if result.Verdict != DownloadUnknown {
		t.Fatalf("result %+v", result)
	}
}
