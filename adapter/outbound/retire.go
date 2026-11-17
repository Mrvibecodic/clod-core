package outbound

// RetireConns has the node's next streams open over new connections and
// reports whether it kept any: the streams of now go on over theirs. Nodes
// that open a connection per stream keep none.
func (v *Vless) RetireConns() bool {
	retired := v.gunClient != nil && v.gunClient.Retire()
	if v.xhttpClient != nil && v.xhttpClient.Retire() {
		retired = true
	}
	return retired
}

func (t *Trojan) RetireConns() bool {
	return t.gunClient != nil && t.gunClient.Retire()
}

func (v *Vmess) RetireConns() bool {
	return v.gunClient != nil && v.gunClient.Retire()
}

func (p *autoCloseProxyAdapter) RetireConns() bool {
	r, ok := p.ProxyAdapter.(interface{ RetireConns() bool })
	return ok && r.RetireConns()
}
