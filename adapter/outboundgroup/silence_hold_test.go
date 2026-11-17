package outboundgroup

import (
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// While probes are held (the network is switching) unanswered connections say
// nothing about the node: they are not counted, and the count starts once the
// hold is over.
func TestSilenceUnderTheProbeHoldIsNotCounted(t *testing.T) {
	gb, node, asks := silentGroup(), &measuredProxy{name: "A", alive: true}, &askCounter{}
	t.Cleanup(func() { C.SetProbeHoldUntil(time.Time{}) })
	C.SetProbeHoldUntil(time.Now().Add(200 * time.Millisecond))
	sent := time.Now()
	silent(gb, node, asks, 2*silenceCount)
	asks.want(t, 0)

	// A request sent under the hold and found unanswered after it.
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 2*silenceCount; i++ {
		gb.noteSilence(node, born, sent, asks.ask)
	}
	asks.want(t, 0)

	silent(gb, node, asks, silenceCount)
	asks.want(t, 1)
}
