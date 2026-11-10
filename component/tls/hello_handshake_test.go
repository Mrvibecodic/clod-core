package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/metacubex/tls"
	utls "github.com/metacubex/utls"
)

func helloTestServer(t *testing.T, maxVersion uint16, curves ...tls.CurveID) (addr string, roots *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "example.com"},
		DNSNames:     []string{"example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots = x509.NewCertPool()
	roots.AddCert(cert)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	config := &tls.Config{
		Certificates:     []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:       []string{"h2", "http/1.1"},
		CurvePreferences: curves,
		MaxVersion:       maxVersion,
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				server := tls.Server(conn, config)
				if server.Handshake() == nil {
					_, _ = server.Write([]byte("ok"))
				}
			}()
		}
	}()
	return listener.Addr().String(), roots
}

func helloTestDial(t *testing.T, name, addr string, config *utls.Config, build func(*UConn) error) utls.ConnectionState {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	fingerprint, _ := GetFingerprint(name)
	uConn := UClient(conn, config, fingerprint)
	if err = build(uConn); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if err = uConn.Handshake(); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	reply := make([]byte, 2)
	if _, err = io.ReadFull(uConn, reply); err != nil || string(reply) != "ok" {
		t.Fatalf("%s: reply %q, %v", name, reply, err)
	}
	return uConn.ConnectionState()
}

func TestUpdatedHelloCompletesHandshakes(t *testing.T) {
	builds := []struct {
		name  string
		build func(*UConn) error
		alpn  string
	}{
		{"as is", (*UConn).BuildHandshakeState, "h2"},
		{"without the hybrid key share", BuildRemovedX25519MLKEM768HandshakeState, "h2"},
		{"websocket", BuildWebsocketHandshakeState, "http/1.1"},
	}
	servers := []struct {
		name   string
		curves []tls.CurveID
	}{
		{"any key share", nil},
		{"hybrid", []tls.CurveID{tls.X25519MLKEM768}},
		{"x25519", []tls.CurveID{tls.X25519}},
		{"retry", []tls.CurveID{tls.CurveP384}},
	}
	for _, server := range servers {
		addr, roots := helloTestServer(t, 0, server.curves...)
		for _, build := range builds {
			if server.name == "hybrid" && build.name == "without the hybrid key share" {
				continue
			}
			for _, name := range updatedHellos {
				state := helloTestDial(t, name, addr, &utls.Config{ServerName: "example.com", RootCAs: roots}, build.build)
				if state.Version != utls.VersionTLS13 || state.NegotiatedProtocol != build.alpn {
					t.Fatalf("%s, %s, %s: TLS %04x, ALPN %q", name, server.name, build.name, state.Version, state.NegotiatedProtocol)
				}
			}
		}
	}
}

func TestUpdatedHelloWithASessionCache(t *testing.T) {
	for _, maxVersion := range []uint16{tls.VersionTLS13, tls.VersionTLS12} {
		addr, roots := helloTestServer(t, maxVersion)
		for _, name := range updatedHellos {
			config := &utls.Config{ServerName: "example.com", RootCAs: roots, ClientSessionCache: utls.NewLRUClientSessionCache(4)}
			for i := 0; i < 3; i++ {
				state := helloTestDial(t, name, addr, config, (*UConn).BuildHandshakeState)
				if state.Version != maxVersion || state.DidResume != (maxVersion == tls.VersionTLS12 && i > 0) {
					t.Fatalf("%s, handshake %d: TLS %04x, resumed %v", name, i+1, state.Version, state.DidResume)
				}
			}
		}
	}
}
