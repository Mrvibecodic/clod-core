package hidden

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// node — сервер (dialer — через что соединяется) или, с group, группа.
type node struct {
	C.Proxy
	name, dialer string
	group        bool
}

func (n *node) Name() string           { return n.name }
func (n *node) Server() (string, bool) { return n.dialer, !n.group }

func useLookup(t *testing.T, nodes ...*node) {
	byName := map[string]C.Proxy{}
	for _, n := range nodes {
		byName[n.name] = n
	}
	prev := SetLookup(func(name string) C.Proxy { return byName[name] })
	t.Cleanup(func() { SetLookup(prev) })
}

func TestReplace(t *testing.T) {
	t.Cleanup(func() { Replace(nil) })

	lte, wifi := &node{name: "lte"}, &node{name: "wifi"}

	if Active() || Hides(lte) {
		t.Fatal("без набора ничего не скрыто")
	}

	gen := Generation()
	before, changed := Replace([]string{"lte", "b", ""})
	if !changed || !Hides(lte) || Hides(wifi) || !Active() || before(lte) {
		t.Fatal("новый набор")
	}
	if Generation() == gen {
		t.Fatal("смена набора меняет поколение")
	}

	gen = Generation()
	if _, changed := Replace([]string{"b", "lte"}); changed || Generation() != gen {
		t.Fatal("тот же набор ничего не меняет")
	}

	before, changed = Replace([]string{"b", "wifi"})
	if !changed || Hides(lte) || !Hides(wifi) || !before(lte) || before(wifi) {
		t.Fatal("сужение и добавление")
	}

	if _, changed := Replace(nil); !changed || Active() || Hides(wifi) {
		t.Fatal("пустой набор снимает всё")
	}
}

func TestOnlyServersAndWhatGoesThroughThemAreHidden(t *testing.T) {
	t.Cleanup(func() { Replace(nil) })

	lte := &node{name: "lte"}
	viaLTE := &node{name: "via-lte", dialer: "lte"}
	viaVia := &node{name: "via-via", dialer: "via-lte"}
	group := &node{name: "Proxy", group: true}
	viaGroup := &node{name: "via-group", dialer: "Proxy"}
	loop := &node{name: "loop", dialer: "loop"}
	useLookup(t, lte, viaLTE, viaVia, group, viaGroup, loop)

	Replace([]string{"lte", "Proxy"})

	for _, n := range []*node{lte, viaLTE, viaVia} {
		if !Hides(n) {
			t.Fatalf("%s не скрыт", n.name)
		}
	}
	for _, n := range []*node{group, viaGroup, loop, {name: "DIRECT", group: true}} {
		if Hides(n) {
			t.Fatalf("%s скрыт: скрываются только серверы", n.name)
		}
	}

	// Сервер провайдера, пришедший после смены набора, решается при вопросе.
	if !Hides(&node{name: "late", dialer: "via-lte"}) || !Hides(&node{name: "lte"}) {
		t.Fatal("поздний узел не скрыт")
	}
}

func TestLookupChangeRenewsTheGeneration(t *testing.T) {
	t.Cleanup(func() { Replace(nil) })

	gen := Generation()
	LookupChanged()
	if Generation() != gen {
		t.Fatal("без набора смена узлов кешей не трогает")
	}

	Replace([]string{"lte"})
	gen = Generation()
	LookupChanged()
	if Generation() == gen {
		t.Fatal("смена узлов для dialer-proxy меняет поколение")
	}
}

func TestAnEmptySetCostsNothing(t *testing.T) {
	p := &node{name: "lte"}
	if allocs := testing.AllocsPerRun(100, func() { _ = Hides(p) || Active() }); allocs != 0 {
		t.Fatalf("без набора — выделения памяти: %v", allocs)
	}
}

func TestHidesByAProfileTheCoreDoesNotHold(t *testing.T) {
	lte := &node{name: "lte"}
	viaLTE := &node{name: "via-lte", dialer: "lte"}
	find := func(name string) C.Proxy {
		if name == "lte" {
			return lte
		}
		return nil
	}

	hides := HidesBy([]string{"lte"}, find)
	if !hides(lte) || !hides(viaLTE) || hides(&node{name: "wifi"}) || Active() {
		t.Fatal("набор профиля")
	}
	if HidesBy(nil, find)(lte) {
		t.Fatal("пустой набор ничего не скрывает")
	}
}
