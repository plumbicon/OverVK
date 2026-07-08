package overvk

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	mitmMu     sync.RWMutex
	mitmActive bool
	mitmCA     *x509.Certificate
	mitmCAKey  *ecdsa.PrivateKey
	certCache  sync.Map
)

func ConfigureMITM(enabled bool, caCertPath, caKeyPath string) error {
	if !enabled {
		return nil
	}

	mitmMu.Lock()
	mitmActive = true
	mitmMu.Unlock()

	if caCertPath == "" && caKeyPath == "" {
		return nil
	}

	certPEM, certErr := os.ReadFile(caCertPath)
	keyPEM, keyErr := os.ReadFile(caKeyPath)

	if certErr != nil || keyErr != nil {
		log.Printf("Generating new MITM CA certificate...")
		return generateAndSaveCA(caCertPath, caKeyPath)
	}

	return loadCA(certPEM, keyPEM)
}

func MITMEnabled() bool {
	mitmMu.RLock()
	defer mitmMu.RUnlock()
	return mitmActive
}

func generateAndSaveCA(certPath, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "OverVK MITM CA",
			Organization: []string{"OverVK"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create CA certificate: %w", err)
	}

	ca, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("parse CA certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("write CA cert to %s: %w", certPath, err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal CA key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write CA key to %s: %w", keyPath, err)
	}

	mitmMu.Lock()
	mitmCA = ca
	mitmCAKey = key
	mitmMu.Unlock()

	log.Printf("MITM CA certificate saved to %s (add to browser/OS trust store)", certPath)
	return nil
}

func loadCA(certPEM, keyPEM []byte) error {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return fmt.Errorf("failed to decode CA certificate PEM")
	}
	ca, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return fmt.Errorf("parse CA certificate: %w", err)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return fmt.Errorf("failed to decode CA key PEM")
	}

	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		pkcs8Key, err2 := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if err2 != nil {
			return fmt.Errorf("parse CA key: %w", err)
		}
		var ok bool
		key, ok = pkcs8Key.(*ecdsa.PrivateKey)
		if !ok {
			return fmt.Errorf("CA key is not ECDSA")
		}
	}

	mitmMu.Lock()
	mitmCA = ca
	mitmCAKey = key
	mitmMu.Unlock()

	return nil
}

func generateHostCert(hostname string) (*tls.Certificate, error) {
	if cached, ok := certCache.Load(hostname); ok {
		return cached.(*tls.Certificate), nil
	}

	mitmMu.RLock()
	ca := mitmCA
	caKey := mitmCAKey
	mitmMu.RUnlock()

	if ca == nil {
		return nil, fmt.Errorf("MITM CA not loaded")
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate host key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: hostname},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	if ip := net.ParseIP(hostname); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{hostname}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create host cert: %w", err)
	}

	tlsCert := &tls.Certificate{
		Certificate: [][]byte{certDER, ca.Raw},
		PrivateKey:  key,
	}

	certCache.Store(hostname, tlsCert)
	return tlsCert, nil
}

func WrapClientConn(conn net.Conn, hostname string) (net.Conn, error) {
	cert, err := generateHostCert(hostname)
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Server(conn, &tls.Config{
		Certificates: []tls.Certificate{*cert},
	})

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("set TLS handshake deadline: %w", err)
	}
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("TLS handshake for %s: %w", hostname, err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear deadline: %w", err)
	}

	return tlsConn, nil
}

func DialTLS(ctx context.Context, host string, port int) (net.Conn, error) {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := &tls.Dialer{
		Config: &tls.Config{
			ServerName: host,
		},
	}
	return dialer.DialContext(ctx, "tcp", address)
}
