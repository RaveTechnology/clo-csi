package channel

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"math/big"
	"net"
	"strings"
	"time"
)

type TLSMaterial struct{ CAPEM, CertPEM, KeyPEM []byte }

func certificatePEM(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}
func GenerateTLS(dns []string, ips []net.IP, validFor time.Duration) (TLSMaterial, error) {
	var out TLSMaterial
	if len(dns)+len(ips) == 0 || validFor <= 0 || validFor > 365*24*time.Hour {
		return out, protocol.Fail(protocol.Invalid, "TLS SANs and bounded certificate lifetime required")
	}
	for _, name := range dns {
		if name == "" || strings.ContainsAny(name, "*\r\n\x00") {
			return out, protocol.Fail(protocol.Invalid, "exact DNS SANs required")
		}
	}
	caKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return out, e
	}
	serverKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return out, e
	}
	serial := func() (*big.Int, error) { return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) }
	caSerial, e := serial()
	if e != nil {
		return out, e
	}
	serverSerial, e := serial()
	if e != nil {
		return out, e
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: caSerial, Subject: pkix.Name{CommonName: "CLO CSI private CA"}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(validFor + 24*time.Hour), KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, IsCA: true, BasicConstraintsValid: true}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		return out, e
	}
	server := &x509.Certificate{SerialNumber: serverSerial, Subject: pkix.Name{CommonName: "CLO CSI controller"}, DNSNames: dns, IPAddresses: ips, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(validFor), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	serverDER, e := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	if e != nil {
		return out, e
	}
	keyDER, e := x509.MarshalPKCS8PrivateKey(serverKey)
	if e != nil {
		return out, e
	}
	return TLSMaterial{CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), KeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}, nil
}
