package adapter

import "github.com/metacubex/mihomo/common/structure"

// Node — из чего клиент складывает подпись узла: сеть (как в конфиге, ядро
// сравнивает её с учётом регистра), TLS, ключ Reality, ключ TLSMirror и
// HTTPUpgrade у ws. Есть только у узлов из конфига и провайдеров — после
// override провайдера, как их разобрало ядро; у групп и встроенных узлов его
// нет.
type Node struct {
	Network      string
	TLS          bool
	RealityKey   bool
	TLSMirrorKey bool
	HTTPUpgrade  bool
	// DialerProxy — через какой узел или группу узел соединяется (dialer-proxy).
	DialerProxy string
}

// Node — см. Node; nil у групп и встроенных узлов.
func (p *Proxy) Node() *Node {
	return p.node
}

// Server — узел из конфига или провайдера (не группа и не встроенный) и
// через что он соединяется; ok=false у остальных.
func (p *Proxy) Server() (dialerProxy string, ok bool) {
	if p.node == nil {
		return "", false
	}
	return p.node.DialerProxy, true
}

// nodeOption — поля узла под теми же ключами и с тем же разбором, что у
// ParseProxy: ключи без учёта регистра и «_» как «-», мягкие типы.
type nodeOption struct {
	Network     string `proxy:"network,omitempty"`
	TLS         bool   `proxy:"tls,omitempty"`
	DialerProxy string `proxy:"dialer-proxy,omitempty"`
	RealityOpts struct {
		PublicKey string `proxy:"public-key,omitempty"`
	} `proxy:"reality-opts,omitempty"`
	TLSMirrorOpts struct {
		PrimaryKey string `proxy:"primary-key,omitempty"`
	} `proxy:"tlsmirror-opts,omitempty"`
	WSOpts struct {
		V2rayHttpUpgrade bool `proxy:"v2ray-http-upgrade,omitempty"`
	} `proxy:"ws-opts,omitempty"`
}

func nodeOf(mapping map[string]any) *Node {
	var opt nodeOption
	// Узел уже разобран ядром теми же правилами. Поле не того типа (у
	// протокола, где его нет) остаётся пустым, остальные читаются.
	_ = structure.NewDecoder(structure.Option{TagName: "proxy", WeaklyTypedInput: true, KeyReplacer: structure.DefaultKeyReplacer}).Decode(mapping, &opt)

	return &Node{
		Network:      opt.Network,
		TLS:          opt.TLS,
		RealityKey:   opt.RealityOpts.PublicKey != "",
		TLSMirrorKey: opt.TLSMirrorOpts.PrimaryKey != "",
		HTTPUpgrade:  opt.WSOpts.V2rayHttpUpgrade,
		DialerProxy:  opt.DialerProxy,
	}
}
