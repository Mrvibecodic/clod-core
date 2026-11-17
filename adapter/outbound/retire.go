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

func (t *Tuic) RetireConns() bool {
	return t.client != nil && t.client.Retire()
}

func (p *autoCloseProxyAdapter) RetireConns() bool {
	r, ok := p.ProxyAdapter.(interface{ RetireConns() bool })
	return ok && r.RetireConns()
}

// KeepsConns reports whether the node carries all its streams over connections
// it keeps: a new stream then opens no new connection.
func (v *Vless) KeepsConns() bool {
	return v.gunClient != nil || v.xhttpClient != nil && v.xhttpClient.Pooled()
}

func (t *Trojan) KeepsConns() bool {
	return t.gunClient != nil
}

func (v *Vmess) KeepsConns() bool {
	return v.gunClient != nil
}

func (t *Tuic) KeepsConns() bool {
	return t.client != nil
}

func (p *autoCloseProxyAdapter) KeepsConns() bool {
	k, ok := p.ProxyAdapter.(interface{ KeepsConns() bool })
	return ok && k.KeepsConns()
}
