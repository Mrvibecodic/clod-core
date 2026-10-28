package inbound

import (
	"fmt"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/transport/socks5"
)

func TestParseSocksAddrDomainLengths(t *testing.T) {
	for length := 0; length <= 255; length++ {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			host := strings.Repeat("a", length)
			addr := append(socks5.Addr{socks5.AtypDomainName, byte(length)}, []byte(host)...)
			addr = append(addr, 0x12, 0x34)
			got := parseSocksAddr(addr)
			if got.Host != host || got.DstPort != 0x1234 {
				t.Fatalf("length %d: host=%q port=%d", length, got.Host, got.DstPort)
			}
		})
	}
}

func TestParseSocksAddrFQDN(t *testing.T) {
	host := "example.com."
	addr := append(socks5.Addr{socks5.AtypDomainName, byte(len(host))}, []byte(host)...)
	addr = append(addr, 0x01, 0xbb)
	got := parseSocksAddr(addr)
	if got.Host != "example.com" || got.DstPort != 443 {
		t.Fatalf("host=%q port=%d", got.Host, got.DstPort)
	}
}

func TestParseSocksAddrMalformed(t *testing.T) {
	for _, addr := range []socks5.Addr{
		nil,
		{},
		{socks5.AtypDomainName},
		{socks5.AtypDomainName, 5, 'a', 'b'},
		{socks5.AtypIPv4, 127, 0, 0, 1},
		{socks5.AtypIPv6, 0, 0},
		{0x7f, 1, 2, 3},
	} {
		if got := parseSocksAddr(addr); got.Valid() {
			t.Fatalf("%v: %#v", addr, got)
		}
	}
	if got := parseSocksAddr(socks5.ParseAddr(strings.Repeat("a", 256) + ":80")); got.Valid() {
		t.Fatalf("256-byte host: %#v", got)
	}
}
