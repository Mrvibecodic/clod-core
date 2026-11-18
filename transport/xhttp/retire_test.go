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

// While two connections set aside still carry sessions, Retire sets nothing
// more aside and the next session goes over the current connection; once one
// of them closes, Retire works again.
func TestRetireKeepsAtMostTwoConnectionsSetAside(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 20 * time.Millisecond

	var created atomic.Int64
	manager, err := NewReuseManager(&ReuseConfig{MaxConnections: "1"}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	defer close(closed)

	first := manager.GetTransport().(*ReuseTransport)
	if !manager.Retire(closed) {
		t.Fatal("the first connection was not set aside")
	}
	second := manager.GetTransport().(*ReuseTransport)
	defer second.Close()
	if !manager.Retire(closed) {
		t.Fatal("the second connection was not set aside")
	}
	third := manager.GetTransport().(*ReuseTransport)
	defer third.Close()
	if manager.Retire(closed) {
		t.Fatal("a third connection was set aside while two still carry sessions")
	}
	fourth := manager.GetTransport().(*ReuseTransport)
	defer fourth.Close()
	if transportID(fourth) != transportID(third) {
		t.Fatal("the session after a refused Retire got a new connection")
	}

	first.Close()
	deadline := time.Now().Add(4 * time.Second)
	for !manager.Retire(closed) {
		if time.Now().After(deadline) {
			t.Fatal("Retire did not work again after a connection set aside closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !first.entry.isClosed() {
		t.Fatal("Retire worked again before a connection set aside closed")
	}
	if fifth := manager.GetTransport().(*ReuseTransport); transportID(fifth) == transportID(third) {
		t.Fatal("the session after Retire got the connection set aside")
	} else {
		fifth.Close()
	}
}

func TestRetireOfAClientWithoutAPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if (&Client{ctx: ctx}).Retire() {
		t.Fatal("a client without a pool keeps no connection to set aside")
	}
}

// A pool of more connections carrying sessions than the limit, with nothing
// set aside yet, is set aside whole: the next session gets a new connection
// and the old ones stay open under their sessions. While those are still open,
// the connection that came after them is not set aside.
func TestRetireOfAPoolLargerThanTheLimitSetsItAsideOnce(t *testing.T) {
	var created atomic.Int64
	manager, err := NewReuseManager(&ReuseConfig{MaxConnections: "3"}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	defer close(closed)
	var held []*ReuseTransport
	for i := 0; i < retireLimit+1; i++ {
		held = append(held, manager.GetTransport().(*ReuseTransport))
	}
	defer func() {
		for _, rt := range held {
			rt.Close()
		}
	}()
	if created.Load() != retireLimit+1 {
		t.Fatalf("the pool holds %d connections, want %d", created.Load(), retireLimit+1)
	}
	if !manager.Retire(closed) {
		t.Fatal("the pool was not set aside though nothing was set aside before")
	}
	if n := currentConns(manager); n != 0 {
		t.Fatalf("%d connections are still current after Retire", n)
	}

	next := manager.GetTransport().(*ReuseTransport)
	held = append(held, next)
	if created.Load() != retireLimit+2 {
		t.Fatal("the session after Retire got a connection set aside")
	}
	if manager.Retire(closed) {
		t.Fatal("more was set aside while the pool set aside still carries sessions")
	}
	if n := currentConns(manager); n != 1 {
		t.Fatalf("%d connections are current after a refused Retire, want 1", n)
	}
	for _, rt := range held {
		if rt.entry.isClosed() {
			t.Fatal("a connection was closed under its session")
		}
	}
}

// newPooledClient makes a client whose upload and download pools hold up to
// the given numbers of connections.
func newPooledClient(t *testing.T, up, down string, created *atomic.Int64) *Client {
	t.Helper()
	upload, err := NewReuseManager(&ReuseConfig{MaxConnections: up}, makeTestTransportFactory(created))
	if err != nil {
		t.Fatal(err)
	}
	download, err := NewReuseManager(&ReuseConfig{MaxConnections: down}, makeTestTransportFactory(created))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Client{ctx: ctx, cancel: cancel, uploadManager: upload, downloadManager: download}
}

func currentConns(m *ReuseManager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// The upload and download connections of a client count together. One
// upload and two download connections, with nothing set aside yet, are set
// aside together. With one of them still open, a new upload and a new
// download connection would each fit, but together they go past the limit:
// neither pool gives up its connection.
func TestRetireCountsUploadAndDownloadConnectionsTogether(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 20 * time.Millisecond

	var created atomic.Int64
	client := newPooledClient(t, "1", "2", &created)
	up := client.uploadManager.GetTransport().(*ReuseTransport)
	defer up.Close()
	down1 := client.downloadManager.GetTransport().(*ReuseTransport)
	down2 := client.downloadManager.GetTransport().(*ReuseTransport)

	if !client.Retire() {
		t.Fatal("the upload and download connections were not set aside though nothing was set aside before")
	}
	if n, m := currentConns(client.uploadManager), currentConns(client.downloadManager); n != 0 || m != 0 {
		t.Fatalf("after Retire %d upload and %d download connections are still current", n, m)
	}
	down1.Close()
	down2.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		retiredMu.Lock()
		n := len(retired[client])
		retiredMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections count as set aside, want 1", n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	up2 := client.uploadManager.GetTransport().(*ReuseTransport)
	defer up2.Close()
	down3 := client.downloadManager.GetTransport().(*ReuseTransport)
	defer down3.Close()
	if client.Retire() {
		t.Fatal("an upload and a download connection were set aside past the limit")
	}
	if n, m := currentConns(client.uploadManager), currentConns(client.downloadManager); n != 1 || m != 1 {
		t.Fatalf("after a refused Retire %d upload and %d download connections are current, want 1 and 1", n, m)
	}
	if again := client.uploadManager.GetTransport().(*ReuseTransport); transportID(again) != transportID(up2) {
		t.Fatal("the upload after a refused Retire got a new connection")
	} else {
		again.Close()
	}
	if up.entry.isClosed() {
		t.Fatal("the upload connection set aside was closed under its session")
	}
}

// One upload and one download connection are set aside together; while both
// still carry sessions, the two that came after them are not, in either pool.
func TestRetireOfTwoConnectionsLeavesNoRoomForMore(t *testing.T) {
	var created atomic.Int64
	client := newPooledClient(t, "1", "1", &created)
	up := client.uploadManager.GetTransport().(*ReuseTransport)
	defer up.Close()
	down := client.downloadManager.GetTransport().(*ReuseTransport)
	defer down.Close()
	if !client.Retire() {
		t.Fatal("the two connections were not set aside")
	}

	up2 := client.uploadManager.GetTransport().(*ReuseTransport)
	defer up2.Close()
	down2 := client.downloadManager.GetTransport().(*ReuseTransport)
	defer down2.Close()
	if created.Load() != 4 {
		t.Fatalf("the sessions after Retire went over %d connections, want four", created.Load())
	}
	if client.Retire() {
		t.Fatal("more connections were set aside while two still carry sessions")
	}
	if n, m := currentConns(client.uploadManager), currentConns(client.downloadManager); n != 1 || m != 1 {
		t.Fatalf("after a refused Retire %d upload and %d download connections are current, want 1 and 1", n, m)
	}
	if again := client.downloadManager.GetTransport().(*ReuseTransport); transportID(again) != transportID(down2) {
		t.Fatal("the download after a refused Retire got a new connection")
	} else {
		again.Close()
	}
}

// A pool of more connections than the limit is set aside whole when only some
// of them carry sessions: the idle ones close at once, the busy one stays open
// until its session ends.
func TestRetireOfAPoolWhereOnlyOneConnectionCarriesASessionSetsItAllAside(t *testing.T) {
	defer func(step time.Duration) { retireStep = step }(retireStep)
	retireStep = 20 * time.Millisecond

	var created atomic.Int64
	manager, err := NewReuseManager(&ReuseConfig{MaxConnections: "3"}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	defer close(closed)
	var held []*ReuseTransport
	for i := 0; i < retireLimit+1; i++ {
		held = append(held, manager.GetTransport().(*ReuseTransport))
	}
	if created.Load() != retireLimit+1 {
		t.Fatalf("the pool holds %d connections, want %d", created.Load(), retireLimit+1)
	}
	busy := held[0]
	defer busy.Close()
	for _, rt := range held[1:] {
		rt.Close()
		if rt.entry.isClosed() {
			t.Fatal("a connection closed with its last session while still in the pool")
		}
	}

	if !manager.Retire(closed) {
		t.Fatal("the pool was not set aside though only one connection carries a session")
	}
	if n := currentConns(manager); n != 0 {
		t.Fatalf("%d connections are still current after Retire", n)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !held[1].entry.isClosed() || !held[2].entry.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the idle connections set aside stayed open")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(5 * retireStep)
	if busy.entry.isClosed() {
		t.Fatal("the connection set aside was closed under its session")
	}
	busy.Close()
	deadline = time.Now().Add(2 * time.Second)
	for !busy.entry.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the connection set aside stayed open after its last session")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// setAside reports how many connections count as set aside under key.
func setAside(key any) int {
	retiredMu.Lock()
	defer retiredMu.Unlock()
	return len(retired[key])
}

// CloseRetired closes the connections set aside, whatever sessions they still
// carry, and leaves none counted, while the client and its pools go on.
func TestCloseRetiredClosesTheConnectionsSetAside(t *testing.T) {
	var created atomic.Int64
	client := newPooledClient(t, "1", "1", &created)
	up := client.uploadManager.GetTransport().(*ReuseTransport)
	defer up.Close()
	down := client.downloadManager.GetTransport().(*ReuseTransport)
	defer down.Close()
	if !client.Retire() {
		t.Fatal("the connections in use were not set aside")
	}
	if n := setAside(client); n != 2 {
		t.Fatalf("%d connections count as set aside, want 2", n)
	}

	client.CloseRetired()
	if !up.entry.isClosed() || !down.entry.isClosed() {
		t.Fatalf("closed: the upload connection %v, the download one %v", up.entry.isClosed(), down.entry.isClosed())
	}
	if n := setAside(client); n != 0 {
		t.Fatalf("%d connections count as set aside after CloseRetired, want none", n)
	}
	if client.ctx.Err() != nil {
		t.Fatal("the client was closed with them")
	}
	again := client.uploadManager.GetTransport().(*ReuseTransport)
	defer again.Close()
	if transportID(again) == transportID(up) {
		t.Fatal("the pool handed out a connection that was closed")
	}
}
