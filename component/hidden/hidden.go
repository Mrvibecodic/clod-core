// Package hidden — серверы, которые клиент на время убрал (на Android — серверы
// «только для мобильной сети» вне сети SIM): их нет среди членов групп, их не
// проверяют. Пустой набор — ядро ведёт себя как без этого пакета.
package hidden

import (
	"sync/atomic"

	C "github.com/metacubex/mihomo/constant"
)

// Сервер — узел из конфига или провайдера (adapter.Proxy.Server): через что
// он соединяется (dialer-proxy); ok=false у групп и встроенных узлов.
type server interface {
	Server() (dialerProxy string, ok bool)
}

// Цепочка dialer-proxy длиннее — не цепочка, а петля.
const maxChain = 32

var (
	names      atomic.Pointer[map[string]struct{}]
	generation atomic.Uint64
	lookup     atomic.Pointer[func(name string) C.Proxy]
)

// SetLookup — где ядро ищет узел по имени из dialer-proxy (tunnel.Proxies);
// отвечает прежним.
func SetLookup(find func(name string) C.Proxy) (previous func(name string) C.Proxy) {
	if prev := lookup.Swap(&find); prev != nil {
		return *prev
	}
	return nil
}

// LookupChanged — ядро сменило узлы, среди которых ищется dialer-proxy:
// скрытость серверов из цепочек решается заново.
func LookupChanged() {
	if names.Load() != nil {
		generation.Add(1)
	}
}

// Hides — скрыт ли узел: это сервер с именем из набора или сервер, который
// соединяется (dialer-proxy, сколько угодно звеньев) через такой. Решается
// при каждом вопросе, поэтому узлы провайдеров, пришедшие после смены
// набора, скрыты сразу. Без набора — одна атомарная загрузка.
func Hides(p C.Proxy) bool {
	set := names.Load()
	if set == nil {
		return false
	}

	return hides(*set, p)
}

// HidesBy — Hides по набору names с поиском dialer-proxy в find, мимо набора
// ядра: для узлов профиля, который ядро не держит.
func HidesBy(names []string, find func(name string) C.Proxy) func(C.Proxy) bool {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	return func(p C.Proxy) bool {
		return len(set) > 0 && hidesWith(set, p, find)
	}
}

func hides(set map[string]struct{}, p C.Proxy) bool {
	var find func(string) C.Proxy
	if ptr := lookup.Load(); ptr != nil {
		find = *ptr
	}

	return hidesWith(set, p, find)
}

func hidesWith(set map[string]struct{}, p C.Proxy, find func(string) C.Proxy) bool {
	for i := 0; i < maxChain && p != nil; i++ {
		s, ok := p.(server)
		if !ok {
			return false
		}
		dialer, ok := s.Server()
		if !ok {
			return false
		}
		if _, ok := set[p.Name()]; ok {
			return true
		}
		if dialer == "" || find == nil {
			return false
		}
		p = find(dialer)
	}

	return false
}

// Active — набор не пуст.
func Active() bool {
	return names.Load() != nil
}

// Generation меняется с каждой сменой набора и узлов для dialer-proxy: ею
// помечаются кеши списков.
func Generation() uint64 {
	return generation.Load()
}

// Replace ставит новый набор имён; changed — набор сменился, before отвечает,
// был ли узел скрыт прежним набором.
func Replace(list []string) (before func(C.Proxy) bool, changed bool) {
	var next map[string]struct{}
	for _, name := range list {
		if name == "" {
			continue
		}
		if next == nil {
			next = map[string]struct{}{}
		}
		next[name] = struct{}{}
	}

	prev := names.Load()

	same := (prev == nil) == (next == nil) && (prev == nil || len(*prev) == len(next))
	for name := range next {
		if !same {
			break
		}
		_, same = (*prev)[name]
	}
	if same {
		return nil, false
	}

	if next == nil {
		names.Store(nil)
	} else {
		names.Store(&next)
	}
	generation.Add(1)

	return func(p C.Proxy) bool {
		return prev != nil && hides(*prev, p)
	}, true
}
