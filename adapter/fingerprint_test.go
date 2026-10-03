package adapter

import (
	"encoding/json"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"

	"go.yaml.in/yaml/v3"
)

func parseNode(t *testing.T, text string) *Proxy {
	t.Helper()
	mapping := map[string]any{}
	if err := yaml.Unmarshal([]byte(text), &mapping); err != nil {
		t.Fatal(err)
	}
	proxy, err := ParseProxy(mapping)
	if err != nil {
		t.Fatal(err)
	}
	return proxy.(*Proxy)
}

func TestAFingerprintIgnoresTheNameAndTheOrderOfKeys(t *testing.T) {
	first := parseNode(t, `
name: first
type: ss
server: example.com
port: 8388
cipher: aes-128-gcm
password: secret
plugin: obfs
plugin-opts: {mode: tls, host: example.org}
`)
	renamed := parseNode(t, `
plugin-opts: {host: example.org, mode: tls}
password: secret
cipher: aes-128-gcm
port: 8388
server: example.com
type: ss
name: renamed
plugin: obfs
`)
	moved := parseNode(t, `
name: first
type: ss
server: example.com
port: 8389
cipher: aes-128-gcm
password: secret
plugin: obfs
plugin-opts: {mode: tls, host: example.org}
`)
	if len(first.Fingerprint()) != 16 || first.Fingerprint() != renamed.Fingerprint() {
		t.Fatalf("fingerprints %q and %q", first.Fingerprint(), renamed.Fingerprint())
	}
	if first.Fingerprint() == moved.Fingerprint() {
		t.Fatalf("a new port kept the fingerprint %q", first.Fingerprint())
	}

	var described struct {
		Fingerprint string `json:"fingerprint"`
	}
	data, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &described); err != nil || described.Fingerprint != first.Fingerprint() {
		t.Fatalf("JSON %s", data)
	}
}

func TestTheBuiltInProxiesHaveNoFingerprint(t *testing.T) {
	data, err := json.Marshal(NewProxy(outbound.NewDirect()))
	if err != nil {
		t.Fatal(err)
	}
	var described map[string]any
	if err := json.Unmarshal(data, &described); err != nil {
		t.Fatal(err)
	}
	if _, ok := described["fingerprint"]; ok {
		t.Fatalf("JSON %s", data)
	}
}

func TestMapsWithUntypedKeysHaveAFingerprint(t *testing.T) {
	typed := fingerprint(map[string]any{"type": "ss", "opts": map[string]any{"mode": "tls"}})
	untyped := fingerprint(map[string]any{"type": "ss", "opts": map[any]any{"mode": "tls"}})
	if typed == "" || typed != untyped {
		t.Fatalf("fingerprints %q and %q", typed, untyped)
	}
}

func TestEntriesThatLeadToNoServerHaveNoFingerprint(t *testing.T) {
	for _, text := range []string{
		"{name: without VPN, type: direct}",
		"{name: block, type: reject}",
		"{name: dns, type: dns}",
		"{name: Devices limit, type: vless, server: 0.0.0.0, port: 443, uuid: 0d2f7f7c-55b3-4e8f-9c4d-2b8f2a6d1e11}",
		"{name: Expired, type: vless, server: example.com, port: 443, uuid: 00000000-0000-0000-0000-000000000000}",
		"{name: Info, type: socks5, server: example.com, port: 1}",
		"{name: v6, type: trojan, server: '::', port: 443, password: secret}",
	} {
		if fp := parseNode(t, text).Fingerprint(); fp != "" {
			t.Fatalf("%s: fingerprint %q", text, fp)
		}
	}
	for _, text := range []string{
		"{name: node, type: trojan, server: example.com, port: 443, password: secret}",
		"{name: low port with a key, type: ss, server: example.com, port: 1, cipher: aes-128-gcm, password: secret}",
		"{name: local, type: socks5, server: 127.0.0.1, port: 1080}",
	} {
		if fp := parseNode(t, text).Fingerprint(); len(fp) != 16 {
			t.Fatalf("%s: fingerprint %q", text, fp)
		}
	}
}
