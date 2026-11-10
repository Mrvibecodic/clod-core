package tls

import (
	"encoding/hex"
	"net"

	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/randv2"
	utls "github.com/metacubex/utls"
	"golang.org/x/exp/slices"
)

var (
	helloChrome155  = UClientHelloID{Client: utls.HelloChrome_133.Client, Version: "155"}
	helloFirefox156 = UClientHelloID{Client: utls.HelloFirefox_148.Client, Version: "156"}
)

type helloUpdate struct {
	base   UClientHelloID
	update func(*utls.ClientHelloSpec)
}

var helloUpdates = map[UClientHelloID]helloUpdate{
	helloChrome155:  {utls.HelloChrome_133, updateChrome155},
	helloFirefox156: {utls.HelloFirefox_148, updateFirefox156},
}

func UClient(c net.Conn, config *utls.Config, fingerprint UClientHelloID) *UConn {
	if hello, ok := helloUpdates[fingerprint]; ok {
		spec, err := utls.UTLSIdToSpec(hello.base)
		if err == nil {
			hello.update(&spec)
			if config == nil {
				config = &utls.Config{}
			}
			config.PreferSkipResumptionOnNilExtension = true
			uConn := utls.UClient(c, config, utls.HelloCustom)
			if err = uConn.ApplyPreset(&spec); err == nil {
				return uConn
			}
		}
		log.Debugln("[TLS] %s %s hello: %v", fingerprint.Client, fingerprint.Version, err)
		fingerprint = hello.base
	}
	return utls.UClient(c, config, fingerprint)
}

const extensionTrustAnchors = 0xca34

var chromeTrustAnchors = func() []byte {
	data, err := hex.DecodeString("00b8" +
		"0582df1302010582df1302060582df13020d0582df13020e0582df13020f0582df1302120582df1302130582df130214" +
		"08839a648c9b2d010708839a648c9b2d010808839a648c9b2d010908839a648c9b2d010a08839a648c9b2d010b" +
		"08839a648c9b2d010c08839a648c9b2d010d08839a648c9b2d011208839a648c9b2d0113" +
		"04d679090104d679090404d679090504d679090604d679090704d679090804d679090a04d679090b04d679090c04d679090d04d679090f")
	if err != nil {
		panic(err)
	}
	return data
}()

func updateChrome155(spec *utls.ClientHelloSpec) {
	for _, ext := range spec.Extensions {
		if algorithms, ok := ext.(*utls.SignatureAlgorithmsExtension); ok {
			grease := utls.SignatureScheme(0x0a0a + 0x1010*randv2.IntN(16))
			algorithms.SupportedSignatureAlgorithms = append(
				[]utls.SignatureScheme{grease, 0x0904, 0x0905, 0x0906},
				algorithms.SupportedSignatureAlgorithms...,
			)
		}
	}
	trustAnchors := &utls.GenericExtension{Id: extensionTrustAnchors, Data: chromeTrustAnchors}
	extensions := slices.Insert(spec.Extensions, len(spec.Extensions)-1, utls.TLSExtension(trustAnchors))
	randv2.Shuffle(len(extensions)-2, func(i, j int) {
		extensions[i+1], extensions[j+1] = extensions[j+1], extensions[i+1]
	})
	spec.Extensions = extensions
}

func updateFirefox156(spec *utls.ClientHelloSpec) {
	spec.CipherSuites = slices.DeleteFunc(spec.CipherSuites, func(suite uint16) bool {
		return suite == utls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA || suite == utls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
	})
	extensions := make([]utls.TLSExtension, 0, len(spec.Extensions)+2)
	for _, ext := range spec.Extensions {
		extensions = append(extensions, ext)
		switch ext := ext.(type) {
		case *utls.SupportedCurvesExtension:
			ext.Curves = slices.DeleteFunc(ext.Curves, func(curve utls.CurveID) bool {
				return curve == 0x0100 || curve == 0x0101
			})
		case *utls.SupportedPointsExtension:
			extensions = append(extensions, &utls.SessionTicketExtension{})
		case *utls.SignatureAlgorithmsExtension:
			extensions = append(extensions, &utls.PSKKeyExchangeModesExtension{Modes: []uint8{utls.PskModeDHE}})
		case *utls.GREASEEncryptedClientHelloExtension:
			ext.CandidatePayloadLens = []uint16{224}
		}
	}
	spec.Extensions = extensions
}
