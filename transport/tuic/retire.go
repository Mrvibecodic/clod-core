package tuic

import (
	"sync"
	"time"

	list "github.com/bahlo/generic-list-go"
)

// retireGrace is how long a connection set aside is left alone after it was
// last taken for a stream: one taken a moment ago may still be opening its
// stream and shows none yet.
const retireGrace = 15 * time.Second

// retireLimit is how many connections set aside a pool keeps open at most.
// A node slow under load would have one set aside on every check, and long
// streams keep each of them open: past the limit nothing more is set aside.
const retireLimit = 2

// retireStep is how often a connection set aside is looked at.
var retireStep = time.Second

var (
	retiredMu sync.Mutex
	// retired counts the connections each pool has set aside and not yet
	// closed. A pool is dropped from it at zero, so it does not keep the pool
	// alive.
	retired = map[*PoolClient]int{}
)

// Retire has the next streams and packet sessions open over new connections
// and reports whether there were connections to set aside. Those are left
// alone: what runs over them goes on for as long as it likes, and each
// connection closes when it carries nothing more. A connection that stalled
// is not asked again, one that only lagged loses nothing. With nothing set
// aside and still open, the stream connections are always set aside, however
// many they are: a pool as large as the server asks for is set aside whole
// once. Past that, nothing more is set aside and the streams go on as they did
// while the connections set aside and still open, with the stream connections,
// would go past retireLimit. The stream connections come first: the packet
// connections are set aside with them only while that keeps to retireLimit,
// first time or not, and stay where they are otherwise, so a packet session
// that runs for minutes on a connection set aside does not keep the next
// stream connections in place. A pool with packet connections alone has them
// set aside if they fit.
func (t *PoolClient) Retire() bool {
	retiredMu.Lock()
	defer retiredMu.Unlock()
	t.tcpClientsMutex.Lock()
	defer t.tcpClientsMutex.Unlock()
	t.udpClientsMutex.Lock()
	defer t.udpClientsMutex.Unlock()
	collect := func(clients *list.List[Client]) []Client {
		var found []Client
		for it := clients.Front(); it != nil; it = it.Next() {
			if it.Value != nil {
				found = append(found, it.Value)
			}
		}
		return found
	}
	tcp, udp := collect(&t.tcpClients), collect(&t.udpClients)
	if retired[t] > 0 && retired[t]+len(tcp) > retireLimit {
		return false
	}
	var old []Client
	if len(tcp) > 0 {
		t.tcpClients.Init()
		old = append(old, tcp...)
	}
	if len(udp) > 0 && retired[t]+len(tcp)+len(udp) <= retireLimit {
		t.udpClients.Init()
		old = append(old, udp...)
	}
	if len(old) == 0 {
		return false
	}
	retired[t] += len(old)
	go closeWhenIdle(old, retireStep, retireGrace, func() {
		retiredMu.Lock()
		if retired[t]--; retired[t] <= 0 {
			delete(retired, t)
		}
		retiredMu.Unlock()
	})
	return true
}

// closeWhenIdle calls closed for each connection it closes.
func closeWhenIdle(clients []Client, step, grace time.Duration, closed func()) {
	for len(clients) > 0 {
		time.Sleep(step)
		kept := clients[:0]
		for _, client := range clients {
			if client.OpenStreams() <= 0 && time.Since(client.LastVisited()) > grace {
				// Closing waits for a handshake in progress: not in the way
				// of the others.
				go client.Close()
				closed()
				continue
			}
			kept = append(kept, client)
		}
		clients = kept
	}
}
