package outbound

import (
	"sync"
	"sync/atomic"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// wakeGuard drops a session kept across a sleep before it is used again.
// Dials that come together wait for the drop instead of taking that session.
type wakeGuard struct {
	mu   sync.Mutex
	seen atomic.Int64
}

func (g *wakeGuard) renew(name string, reset func()) {
	// 0 is a guard that has not dialed yet: there is no session to drop
	wake := C.ProbeWake() + 1
	if seen := g.seen.Load(); seen == wake || seen == 0 && g.seen.CompareAndSwap(0, wake) {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Looked up again: a dial held up on the way here must not undo a newer drop
	if wake = C.ProbeWake() + 1; g.seen.Load() != wake {
		reset()
		g.seen.Store(wake)

		log.Debugln("[Сон] %s: соединение с сервером после сна открывается заново", name)
	}
}
