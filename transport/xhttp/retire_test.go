package xhttp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// After Retire the next session gets a new connection even where the pool
// allows one, the session of the old one goes on, and the old connection
// closes when that session ends.
func TestRetireOpensANewConnectionAndSeesTheOldOneOut(t *testing.T) {
	var created atomic.Int64
	manager, err := NewReuseManager(&ReuseConfig{MaxConnections: "1"}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}
	if manager.Retire(nil) {
		t.Fatal("there was nothing to set aside yet")
	}
	first := manager.GetTransport().(*ReuseTransport)
	if again := manager.GetTransport().(*ReuseTransport); transportID(again) != transportID(first) {
		t.Fatal("the pool of one connection gave a second one before Retire")
	} else {
		again.Close()
	}

	if !manager.Retire(nil) {
		t.Fatal("the connection in use was not set aside")
	}
	second := manager.GetTransport().(*ReuseTransport)
	if transportID(second) == transportID(first) {
		t.Fatal("the session after Retire got the connection set aside")
	}
	time.Sleep(1200 * time.Millisecond)
	if first.entry.isClosed() {
		t.Fatal("the connection set aside was closed under its session")
	}

	first.Close()
	deadline := time.Now().Add(4 * time.Second)
	for !first.entry.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the connection set aside stayed open after its last session")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if second.entry.isClosed() {
		t.Fatal("the new connection was closed too")
	}
}

// The connections set aside close with their client, whatever sessions they
// still carry.
func TestRetiredConnectionsCloseWithTheClient(t *testing.T) {
	var created atomic.Int64
	manager, err := NewReuseManager(&ReuseConfig{MaxConnections: "1"}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}
	first := manager.GetTransport().(*ReuseTransport)
	closed := make(chan struct{})
	if !manager.Retire(closed) {
		t.Fatal("the connection in use was not set aside")
	}
	time.Sleep(50 * time.Millisecond)
	if first.entry.isClosed() {
		t.Fatal("the connection set aside was closed under its session")
	}
	close(closed)
	deadline := time.Now().Add(2 * time.Second)
	for !first.entry.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the connection set aside outlived its client")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRetireOfAClientWithoutAPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if (&Client{ctx: ctx}).Retire() {
		t.Fatal("a client without a pool keeps no connection to set aside")
	}
}
