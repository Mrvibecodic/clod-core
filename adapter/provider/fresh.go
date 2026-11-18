package provider

import C "github.com/metacubex/mihomo/constant"

// FreshConns has the streams p opens next go over new connections, if p keeps
// shared ones, and reports whether it set any aside: a node in doubt is then
// probed over a new connection, as the nodes beside it are, and not over the
// one that is under suspicion. The streams of now go on over theirs. A node
// that opens a connection per stream, or one whose transport refuses to set
// more aside, is left as it is.
func FreshConns(p C.Proxy) bool {
	return keepsConns(p) && retireConns(p)
}
