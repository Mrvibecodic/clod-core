package adapter

import (
	"errors"
	"net"
	"strings"

	N "github.com/metacubex/mihomo/common/net"
)

// isDefiniteAnswer reports whether a failed probe got an answer from the
// network instead of stalling: the node refused the connection or the system
// resolver says its name does not exist. Such a probe is not worth repeating.
//
// The core's own resolver is left out on purpose: it reports a failed lookup
// and an empty answer with the same ErrIPNotFound, so a DNS hiccup would kill
// every node at once.
func isDefiniteAnswer(err error) bool {
	if err == nil {
		return false
	}
	if N.IsConnRefused(err) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	return strings.Contains(err.Error(), "no such host")
}
