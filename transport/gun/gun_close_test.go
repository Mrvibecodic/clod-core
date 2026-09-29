package gun

import (
	"context"
	"io"
	"net"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"
)

// A stream the server accepted but never answered is released by Close, and
// the transport connection stays usable for the next stream.
func TestCloseCancelsAPendingStream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	block := make(chan struct{})
	srv := &stdhttp.Server{Handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		n := calls.Add(1)
		if n == 1 {
			select { // never answer the first stream
			case <-r.Context().Done():
				t.Log("server saw first stream cancelled")
			case <-block:
			}
			return
		}
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		io.Copy(io.Discard, r.Body)
	})}
	srv.Protocols = new(stdhttp.Protocols)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go srv.Serve(ln)
	defer srv.Close()
	defer close(block)

	var dials atomic.Int32
	tr := NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	}, nil, &Config{Host: "example.com"})
	defer tr.Close()

	c1, _ := tr.Dial()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	c1.Close()
	done := make(chan error, 1)
	go func() { done <- c1.(*Conn).Init() }()
	select {
	case err := <-done:
		t.Logf("pending stream finished after close in %v: %v", time.Since(start), err)
	case <-time.After(3 * time.Second):
		t.Fatal("pending stream still waiting after Close")
	}

	c2, _ := tr.Dial()
	if _, err := c2.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := c2.(*Conn).Init(); err != nil {
		t.Fatalf("second stream failed: %v", err)
	}
	c2.Close()
	t.Logf("dials=%d (1 = transport connection reused)", dials.Load())
}
