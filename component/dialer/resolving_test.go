package dialer

import (
	"context"
	"net"
	"sync"
	"testing"
)

// A dial marks the lookup of the address it dials: the mark starts before the
// connection is made and is over by the time the dial returns.
func TestDialContextMarksItsAddressLookup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var mu sync.Mutex
	var marks []string
	mark := func(s string) {
		mu.Lock()
		marks = append(marks, s)
		mu.Unlock()
	}
	ctx := WithResolving(context.Background(), func() func() {
		mark("begin")
		return func() { mark("end") }
	})
	conn, err := DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(marks) != 2 || marks[0] != "begin" || marks[1] != "end" {
		t.Fatalf("lookup marks %v, want begin and end", marks)
	}
}

func TestResolvingWithoutAMarkDoesNothing(t *testing.T) {
	Resolving(context.Background())()
}
