package adapter

import (
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
)

func TestAParsedNodeKeepsWhatItsLabelIsMadeOf(t *testing.T) {
	proxy := parseNode(t, `{name: n, type: vless, server: a.example.com, port: 443, uuid: 00000000-0000-0000-0000-000000000000,
network: ws, tls: 1, ws-opts: {path: /, v2ray-http-upgrade: true}, reality-opts: {public-key: ""}}`)
	if got := *proxy.Node(); got != (Node{Network: "ws", TLS: true, HTTPUpgrade: true}) {
		t.Fatalf("node: %+v", got)
	}

	proxy = parseNode(t, `{name: n, type: vless, server: a.example.com, port: 443, uuid: 00000000-0000-0000-0000-000000000000,
tls: true, servername: a.example.com, reality-opts: {public-key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA}}`)
	if got := *proxy.Node(); got != (Node{TLS: true, RealityKey: true}) {
		t.Fatalf("node: %+v", got)
	}

	proxy = parseNode(t, `{name: n, type: vmess, server: a.example.com, port: 443, uuid: 00000000-0000-0000-0000-000000000000,
alterId: 0, cipher: auto, network: ws, tls: true, tlsmirror-opts: {primary-key: k}}`)
	if got := *proxy.Node(); got != (Node{Network: "ws", TLS: true, TLSMirrorKey: true}) {
		t.Fatalf("node: %+v", got)
	}

	if NewProxy(outbound.NewDirect()).Node() != nil {
		t.Fatal("у встроенного узла описания нет")
	}
}

// Ключи — как их читает ParseProxy: без учёта регистра и с «_» вместо «-».
func TestANodeIsReadWithTheKeysTheCoreAccepts(t *testing.T) {
	proxy := parseNode(t, `{Name: n, type: vless, Server: a.example.com, port: 443, UUID: 00000000-0000-0000-0000-000000000000,
Network: ws, TLS: true, ws_opts: {path: /, V2RAY_HTTP_UPGRADE: true}, dialer_proxy: relay}`)
	if got := *proxy.Node(); got != (Node{Network: "ws", TLS: true, HTTPUpgrade: true, DialerProxy: "relay"}) {
		t.Fatalf("node: %+v", got)
	}

	proxy = parseNode(t, `{name: n, type: vless, server: a.example.com, port: 443, uuid: 00000000-0000-0000-0000-000000000000,
tls: true, Reality_Opts: {Public_Key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA}}`)
	if got := *proxy.Node(); !got.RealityKey {
		t.Fatalf("node: %+v", got)
	}

	if dialer, ok := proxy.Server(); !ok || dialer != "" {
		t.Fatalf("сервер: %q %v", dialer, ok)
	}
}
