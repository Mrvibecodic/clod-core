package tls

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	utls "github.com/metacubex/utls"
	"golang.org/x/exp/slices"
)

type helloExtension struct {
	id   uint16
	data []byte
}

type parsedHello struct {
	ciphers    []uint16
	extensions []helloExtension
}

func isGREASE(v uint16) bool {
	return v&0x0f0f == 0x0a0a && v>>8 == v&0xff
}

func maskGREASE(values []uint16) []uint16 {
	masked := slices.Clone(values)
	for i, v := range masked {
		if isGREASE(v) {
			masked[i] = 0x0a0a
		}
	}
	return masked
}

func uint16s(data []byte) []uint16 {
	values := make([]uint16, 0, len(data)/2)
	for ; len(data) >= 2; data = data[2:] {
		values = append(values, binary.BigEndian.Uint16(data))
	}
	return values
}

func parseHello(t *testing.T, raw []byte) parsedHello {
	t.Helper()
	var hello parsedHello
	rest := raw[4+2+32:]
	rest = rest[1+int(rest[0]):]
	n := int(binary.BigEndian.Uint16(rest))
	hello.ciphers = uint16s(rest[2 : 2+n])
	rest = rest[2+n:]
	rest = rest[1+int(rest[0]):]
	rest = rest[2:]
	for len(rest) > 0 {
		n = int(binary.BigEndian.Uint16(rest[2:]))
		hello.extensions = append(hello.extensions, helloExtension{binary.BigEndian.Uint16(rest), rest[4 : 4+n]})
		rest = rest[4+n:]
	}
	return hello
}

func (h parsedHello) ids() []uint16 {
	ids := make([]uint16, len(h.extensions))
	for i, ext := range h.extensions {
		ids[i] = ext.id
	}
	return ids
}

func (h parsedHello) extension(t *testing.T, id uint16) []byte {
	t.Helper()
	for _, ext := range h.extensions {
		if ext.id == id {
			return ext.data
		}
	}
	t.Fatalf("no extension %#04x in the hello", id)
	return nil
}

func (h parsedHello) keyShares(t *testing.T) (groups []uint16) {
	t.Helper()
	for data := h.extension(t, 0x0033)[2:]; len(data) > 0; {
		groups = append(groups, binary.BigEndian.Uint16(data))
		data = data[4+int(binary.BigEndian.Uint16(data[2:])):]
	}
	return groups
}

func builtHello(t *testing.T, name string, build func(*UConn) error) parsedHello {
	t.Helper()
	fingerprint, ok := GetFingerprint(name)
	if !ok {
		t.Fatalf("no fingerprint %q", name)
	}
	uConn := UClient(nil, &utls.Config{ServerName: "example.com"}, fingerprint)
	if err := build(uConn); err != nil {
		t.Fatal(err)
	}
	return parseHello(t, uConn.HandshakeState.Hello.Raw)
}

func assertUint16s(t *testing.T, what string, got, want []uint16) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s\n got %04x\nwant %04x", what, got, want)
	}
}

func TestChromeHelloIsChrome155(t *testing.T) {
	for i := 0; i < 16; i++ {
		hello := builtHello(t, "chrome", (*UConn).BuildHandshakeState)

		assertUint16s(t, "cipher suites", maskGREASE(hello.ciphers), []uint16{
			0x0a0a, 0x1301, 0x1302, 0x1303, 0xc02b, 0xc02f, 0xc02c, 0xc030,
			0xcca9, 0xcca8, 0xc013, 0xc014, 0x009c, 0x009d, 0x002f, 0x0035,
		})

		ids := hello.ids()
		if !isGREASE(ids[0]) || !isGREASE(ids[len(ids)-1]) || ids[0] == ids[len(ids)-1] {
			t.Fatalf("the hello does not start and end with two different GREASE extensions: %04x", ids)
		}
		sorted := slices.Clone(ids[1 : len(ids)-1])
		slices.Sort(sorted)
		assertUint16s(t, "extensions", sorted, []uint16{
			0x0000, 0x0005, 0x000a, 0x000b, 0x000d, 0x0010, 0x0012, 0x0017, 0x001b,
			0x0023, 0x002b, 0x002d, 0x0033, 0x44cd, 0xca34, 0xfe0d, 0xff01,
		})

		algorithms := uint16s(hello.extension(t, 0x000d)[2:])
		if !isGREASE(algorithms[0]) {
			t.Fatalf("signature algorithms do not start with GREASE: %04x", algorithms)
		}
		assertUint16s(t, "signature algorithms", algorithms[1:], []uint16{
			0x0904, 0x0905, 0x0906, 0x0403, 0x0804, 0x0401, 0x0503, 0x0805, 0x0501, 0x0806, 0x0601,
		})
		assertUint16s(t, "supported groups", maskGREASE(uint16s(hello.extension(t, 0x000a)[2:])), []uint16{
			0x0a0a, 0x11ec, 0x001d, 0x0017, 0x0018,
		})
		assertUint16s(t, "key shares", maskGREASE(hello.keyShares(t)), []uint16{0x0a0a, 0x11ec, 0x001d})

		// the list a Chrome 155 sends, as a digest of the extension body
		anchors := sha256.Sum256(hello.extension(t, extensionTrustAnchors))
		if hex.EncodeToString(anchors[:]) != "734236b257e52e7d202b222a0694fedf187c12b11a4fcc733dd00f3331b7f16c" {
			t.Fatalf("trust anchors %x", hello.extension(t, extensionTrustAnchors))
		}
	}
}

func TestChromeHelloPermutesExtensionsEvenly(t *testing.T) {
	const rounds = 20000
	places := map[string][]int{}
	greases := map[utls.SignatureScheme]struct{}{}
	var middle int
	for i := 0; i < rounds; i++ {
		spec, err := utls.UTLSIdToSpec(utls.HelloChrome_133)
		if err != nil {
			t.Fatal(err)
		}
		updateChrome155(&spec)
		middle = len(spec.Extensions) - 2
		for place, ext := range spec.Extensions[1 : middle+1] {
			name := fmt.Sprintf("%T", ext)
			if places[name] == nil {
				places[name] = make([]int, middle)
			}
			places[name][place]++
			if algorithms, ok := ext.(*utls.SignatureAlgorithmsExtension); ok {
				greases[algorithms.SupportedSignatureAlgorithms[0]] = struct{}{}
			}
		}
	}
	if len(places) != middle {
		t.Fatalf("%d kinds of extensions take %d places", len(places), middle)
	}
	even := rounds / middle
	for name, counts := range places {
		for place, count := range counts {
			if count < even*3/4 || count > even*5/4 {
				t.Fatalf("%s is at place %d in %d of %d hellos, an even shuffle gives about %d", name, place+1, count, rounds, even)
			}
		}
	}
	for grease := range greases {
		if !isGREASE(uint16(grease)) {
			t.Fatalf("signature algorithms start with %04x", uint16(grease))
		}
	}
	if len(greases) != 16 {
		t.Fatalf("%d GREASE signature algorithms out of 16", len(greases))
	}
}

func TestFirefoxHelloIsFirefox156(t *testing.T) {
	hello := builtHello(t, "firefox", (*UConn).BuildHandshakeState)

	assertUint16s(t, "cipher suites", hello.ciphers, []uint16{
		0x1301, 0x1303, 0x1302, 0xc02b, 0xc02f, 0xcca9, 0xcca8, 0xc02c,
		0xc030, 0xc013, 0xc014, 0x009c, 0x009d, 0x002f, 0x0035,
	})
	assertUint16s(t, "extensions", hello.ids(), []uint16{
		0x0000, 0x0017, 0xff01, 0x000a, 0x000b, 0x0023, 0x0010, 0x0005, 0x0022,
		0x0012, 0x0033, 0x002b, 0x000d, 0x002d, 0x001c, 0x001b, 0xfe0d,
	})
	assertUint16s(t, "supported groups", uint16s(hello.extension(t, 0x000a)[2:]), []uint16{
		0x11ec, 0x001d, 0x0017, 0x0018, 0x0019,
	})
	assertUint16s(t, "key shares", hello.keyShares(t), []uint16{0x11ec, 0x001d, 0x0017})
	if ticket := hello.extension(t, 0x0023); len(ticket) != 0 {
		t.Fatalf("session ticket %x", ticket)
	}
	if modes := hello.extension(t, 0x002d); hex.EncodeToString(modes) != "0101" {
		t.Fatalf("psk key exchange modes %x", modes)
	}
	// type, kdf, aead, config id, a 32 byte key and the payload, both with their length
	if ech := hello.extension(t, 0xfe0d); len(ech) != 1+2+2+1+2+32+2+240 {
		t.Fatalf("GREASE ECH takes %d bytes", len(ech))
	}
}

var updatedHellos = []string{"chrome", "firefox"}

func TestOnlyTheListedHellosAreUpdated(t *testing.T) {
	for name, fingerprint := range fingerprints {
		if _, updated := helloUpdates[fingerprint]; updated != slices.Contains(updatedHellos, name) {
			t.Fatalf("%q updated: %v", name, updated)
		}
	}
}

func TestUpdatedHelloSurvivesTheRebuilds(t *testing.T) {
	for _, name := range updatedHellos {
		classical := builtHello(t, name, BuildRemovedX25519MLKEM768HandshakeState)
		if groups := classical.keyShares(t); slices.Contains(groups, uint16(utls.X25519MLKEM768)) || !slices.Contains(groups, uint16(utls.X25519)) {
			t.Fatalf("%s: key shares %04x", name, groups)
		}
		if groups := uint16s(classical.extension(t, 0x000a)[2:]); slices.Contains(groups, uint16(utls.X25519MLKEM768)) {
			t.Fatalf("%s: supported groups %04x", name, groups)
		}

		websocket := builtHello(t, name, BuildWebsocketHandshakeState)
		if alpn := websocket.extension(t, 0x0010); string(alpn) != "\x00\x09\x08http/1.1" {
			t.Fatalf("%s: ALPN %q", name, alpn)
		}
	}
}

func TestUpdatedHelloWithoutAConfig(t *testing.T) {
	for _, name := range updatedHellos {
		fingerprint, _ := GetFingerprint(name)
		if err := UClient(nil, nil, fingerprint).BuildHandshakeState(); err == nil {
			t.Fatalf("%s: a hello was built without a server name", name)
		}
	}
}

func TestAHelloThatCannotBeUpdatedStaysTheOneItStartedFrom(t *testing.T) {
	broken := UClientHelloID{Client: utls.HelloChrome_133.Client, Version: "broken"}
	helloUpdates[broken] = helloUpdate{utls.HelloChrome_133, func(spec *utls.ClientHelloSpec) {
		spec.Extensions = append(spec.Extensions, &utls.UtlsGREASEExtension{})
	}}
	t.Cleanup(func() { delete(helloUpdates, broken) })

	uConn := UClient(nil, &utls.Config{ServerName: "example.com"}, broken)
	if err := uConn.BuildHandshakeState(); err != nil {
		t.Fatal(err)
	}
	base := utls.UClient(nil, &utls.Config{ServerName: "example.com"}, utls.HelloChrome_133)
	if err := base.BuildHandshakeState(); err != nil {
		t.Fatal(err)
	}
	ids := maskGREASE(parseHello(t, uConn.HandshakeState.Hello.Raw).ids())
	want := maskGREASE(parseHello(t, base.HandshakeState.Hello.Raw).ids())
	slices.Sort(ids)
	slices.Sort(want)
	assertUint16s(t, "extensions", ids, want)
}
