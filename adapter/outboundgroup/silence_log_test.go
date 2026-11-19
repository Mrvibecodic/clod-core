package outboundgroup

import (
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/log"
)

// logged collects what the core logs while f runs.
func logged(t *testing.T, f func()) []string {
	t.Helper()
	was := log.Level()
	log.SetLevel(log.DEBUG)
	sub := log.Subscribe()
	defer func() {
		log.UnSubscribe(sub)
		log.SetLevel(was)
	}()
	f()
	var lines []string
	for {
		select {
		case e := <-sub:
			lines = append(lines, e.Payload)
		case <-time.After(200 * time.Millisecond):
			return lines
		}
	}
}

func checksOf(lines []string, by string) (found []string) {
	for _, line := range lines {
		if strings.Contains(line, "проверен из-за "+by) {
			found = append(found, line)
		}
	}
	return
}

// A check that found the node alive marks nothing, and so was not to be seen
// anywhere: the log now tells what started it and for how long it holds the
// next one back.
func TestACheckThatFoundTheNodeAliveIsLogged(t *testing.T) {
	gb, node, asks := cutGroup()
	lines := logged(t, func() {
		cuts(gb, node, asks, silenceCount)
		gb.asked(node, verdictAlive, false)
	})
	if got := checksOf(lines, "оборванных соединений"); len(got) != 1 || !strings.Contains(got[0], "через 30 с") {
		t.Fatalf("a check of cut connections that found the node alive: logged %q", lines)
	}

	gb, node, asks = cutGroup()
	lines = logged(t, func() {
		silent(gb, node, asks, silenceCount)
		gb.asked(node, verdictAlive, false)
		gb.asked(node, verdictAlive, true)
	})
	if got := checksOf(lines, "запросов без ответа"); len(got) != 1 || !strings.Contains(got[0], "через 30 с") {
		t.Fatalf("a check of silence that found the node alive: logged %q", lines)
	}
	if got := checksOf(lines, "неоткрывшихся соединений"); len(got) != 1 || !strings.Contains(got[0], "через 3 с") {
		t.Fatalf("a check of failed connections that found the node alive: logged %q", lines)
	}
}

// Nothing is logged of a check that found nothing out, of one that marked the
// node (it has its own line), or of a node the group has left meanwhile.
func TestOnlyACheckThatFoundTheNodeAliveIsLoggedSo(t *testing.T) {
	gb, node, asks := cutGroup()
	lines := logged(t, func() {
		silent(gb, node, asks, silenceCount)
		gb.asked(node, verdictNone, false)
		gb.asked(node, verdictNoNetwork, false)
		gb.asked(node, verdictDead, false)
	})
	if got := checksOf(lines, ""); len(got) != 0 {
		t.Fatalf("logged as a check that found the node alive: %q", got)
	}
}
