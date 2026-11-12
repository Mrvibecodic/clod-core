package listener

import (
	"testing"
	"time"

	LC "github.com/metacubex/mihomo/listener/config"
)

func TestPatchTunStartsFromTheSettingsBeingApplied(t *testing.T) {
	applied := LC.Tun{Device: "applied", DNSHijack: []string{"any:53"}}

	tunMux.Lock()
	before := lastTunConf
	seen := make(chan LC.Tun, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		PatchTun(func(last LC.Tun) LC.Tun {
			seen <- last
			return last
		}, nil)
	}()
	time.Sleep(50 * time.Millisecond)
	early := len(seen) > 0
	lastTunConf = applied
	tunMux.Unlock()

	<-done
	last := <-seen
	tunMux.Lock()
	lastTunConf = before
	tunMux.Unlock()

	if early {
		t.Fatalf("the settings were read while a re-creation held the lock: %+v", last)
	}
	if !last.Equal(applied) {
		t.Fatalf("the change started from %+v, want %+v", last, applied)
	}
}
