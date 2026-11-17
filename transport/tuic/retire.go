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

// Retire has the next streams and packet sessions open over new connections
// and reports whether there were connections to set aside. Those are left
// alone: what runs over them goes on for as long as it likes, and each
// connection closes when it carries nothing more. A connection that stalled
// is not asked again, one that only lagged loses nothing.
func (t *PoolClient) Retire() bool {
	var old []Client
	take := func(clients *list.List[Client], mutex *sync.Mutex) {
		mutex.Lock()
		defer mutex.Unlock()
		for it := clients.Front(); it != nil; it = it.Next() {
			if it.Value != nil {
				old = append(old, it.Value)
			}
		}
		clients.Init()
	}
	take(&t.tcpClients, &t.tcpClientsMutex)
	take(&t.udpClients, &t.udpClientsMutex)
	if len(old) == 0 {
		return false
	}
	go closeWhenIdle(old, time.Second, retireGrace)
	return true
}

func closeWhenIdle(clients []Client, step, grace time.Duration) {
	for len(clients) > 0 {
		time.Sleep(step)
		kept := clients[:0]
		for _, client := range clients {
			if client.OpenStreams() <= 0 && time.Since(client.LastVisited()) > grace {
				// Closing waits for a handshake in progress: not in the way
				// of the others.
				go client.Close()
				continue
			}
			kept = append(kept, client)
		}
		clients = kept
	}
}
