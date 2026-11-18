package outboundgroup

import (
	"github.com/metacubex/mihomo/adapter"
	C "github.com/metacubex/mihomo/constant"
)

// kindOf is how a node looks on the wire: its protocol and, for a node from a
// config or a provider, its transport and its security. REALITY, XHTTP and
// gRPC of one protocol are three kinds: one of them can stop on every server
// while the others go on. A node without a description (a built-in one) is
// of the kind of its protocol.
func kindOf(p C.Proxy) string {
	kind := p.Type().String()
	described, ok := p.(interface{ Node() *adapter.Node })
	if !ok {
		return kind
	}
	node := described.Node()
	if node == nil {
		return kind
	}
	network := node.Network
	switch {
	case network == "":
		network = "tcp"
	case network == "ws" && node.HTTPUpgrade:
		network = "httpupgrade"
	}
	security := "none"
	switch {
	case node.RealityKey:
		security = "reality"
	case node.TLSMirrorKey:
		security = "tlsmirror"
	case node.TLS:
		security = "tls"
	}
	return kind + "/" + network + "/" + security
}
