package outboundgroup

import (
	"context"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// recheckPauses are the waits before the probes of a node the group left: a
// check out of schedule found it dead, the scheduled one may be minutes
// away, and until the node is probed again the group stays where it went.
// One probe after each wait, until the node answers or the scheduled check
// has probed it.
var recheckPauses = []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute}

// leftDead has p, the node the group was on when a check out of schedule
// found it dead, probed again ahead of the scheduled check. Only the last
// node left is probed.
func (gb *GroupBase) leftDead(p C.Proxy, d doubt) {
	gb.recheck(p, d, recheckPauses)
}

func (gb *GroupBase) recheck(p C.Proxy, d doubt, pauses []time.Duration) {
	if p.AliveForTestUrl(d.url) {
		return
	}
	series := gb.rechecks.Add(1)
	seen := lastProbe(p, d.url)
	go func() {
		timeout := time.Duration(gb.testTimeout) * time.Millisecond
		expected, _ := utils.NewUnsignedRanges[uint16](d.expected)
		for _, pause := range pauses {
			time.Sleep(pause)
			// A probe of the node since the last one of this series is the
			// scheduled check's or another's: the node is theirs from now on.
			if gb.rechecks.Load() != series || p.AliveForTestUrl(d.url) || !lastProbe(p, d.url).Equal(seen) {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), provider.ProbeHedgeDelay+timeout+C.ProbeReserve)
			_, _ = provider.ProbeNode(ctx, p, d.url, expected, timeout)
			cancel()
			if p.AliveForTestUrl(d.url) {
				log.Infoln("[Проба] %s снова отвечает", p.Name())
				if d.again != nil {
					d.again()
				}
				return
			}
			seen = lastProbe(p, d.url)
		}
	}()
}
