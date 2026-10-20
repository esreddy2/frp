// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// transport.tls.useSystemCa must actually reach the TLS configuration that the
// connector builds. Without it the option is accepted by the config validator
// but silently ignored, so the server certificate is never verified.
func TestConnectorSystemCaVerification(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, caFile, serverName string
		useSystemCa              bool
		wantError                bool
	}{
		{"verification is off by default", "", "example.com", false, false},
		{"system roots reject an unknown issuer", "", "example.com", true, true},
		{"custom root alone verifies", ca, "example.com", false, false},
		{"custom root combines with the system roots", ca, "example.com", true, false},
		{"custom root rejects a wrong server name", ca, "wrong.example", false, true},
		{"system roots reject a wrong server name", "", "wrong.example", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &v1.ClientCommonConfig{ServerAddr: host}
			cfg.ServerPort, _ = strconv.Atoi(port)
			if err := cfg.Complete(); err != nil {
				t.Fatal(err)
			}
			cfg.Transport.TLS.UseSystemCa = tc.useSystemCa
			cfg.Transport.TLS.TrustedCaFile = tc.caFile
			cfg.Transport.TLS.ServerName = tc.serverName

			conn, err := (&defaultConnectorImpl{ctx: context.Background(), cfg: cfg}).realConnect()
			if conn != nil {
				if err == nil {
					_, err = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
				}
				conn.Close()
			}
			if (err != nil) != tc.wantError {
				t.Fatalf("connection error = %v, want error %v", err, tc.wantError)
			}
		})
	}
}

// The QUIC transport always uses TLS, so the same option must be honored on
// that path as well.
func TestQUICSystemCaVerification(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificate.Close()
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: certificate.TLS.Certificates,
		NextProtos:   []string{"frp"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx := t.Context()
	go func() {
		for {
			if _, err := listener.Accept(ctx); err != nil {
				return
			}
		}
	}()

	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, caFile string
		wantError    bool
	}{
		{"system roots reject an unknown issuer", "", true},
		{"custom root combines with the system roots", ca, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &v1.ClientCommonConfig{ServerAddr: "127.0.0.1", ServerPort: listener.Addr().(*net.UDPAddr).Port}
			cfg.Transport.Protocol = "quic"
			if err := cfg.Complete(); err != nil {
				t.Fatal(err)
			}
			cfg.Transport.TLS.UseSystemCa = true
			cfg.Transport.TLS.TrustedCaFile = tc.caFile
			cfg.Transport.TLS.ServerName = "example.com"

			attemptCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			connector := NewConnector(attemptCtx, cfg)
			err := connector.Open()
			connector.Close()
			stop()

			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "unknown authority") {
					t.Fatalf("QUIC error = %v, want unknown authority", err)
				}
			} else if err != nil {
				t.Fatalf("QUIC error = %v, want success", err)
			}
		})
	}
}

// generateSelfSignedCert returns a PEM encoded self signed certificate and its
// private key. Every call returns a distinct certificate, so a test can trust
// one of them and reject the other.
func generateSelfSignedCert(t *testing.T, commonName string) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{commonName},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

// QUIC always runs over TLS, so the configured material must be honored even
// when transport.tls.enable is false. Otherwise the client skipped the server
// certificate verification and silently ignored a wrong CA.
func TestQUICImplicitTLSMaterial(t *testing.T) {
	serverCert, serverKey := generateSelfSignedCert(t, "example.com")
	certificate, err := tls.X509KeyPair(serverCert, serverKey)
	require.NoError(t, err)
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{"frp"},
	}, nil)
	require.NoError(t, err)
	defer listener.Close()

	ctx := t.Context()
	go func() {
		for {
			if _, err := listener.Accept(ctx); err != nil {
				return
			}
		}
	}()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, serverCert, 0o600))

	otherCert, _ := generateSelfSignedCert(t, "example.com")
	otherCAFile := filepath.Join(dir, "other-ca.pem")
	require.NoError(t, os.WriteFile(otherCAFile, otherCert, 0o600))

	for _, tc := range []struct {
		name        string
		caFile      string
		useSystemCa bool
		wantError   bool
	}{
		{name: "no material keeps the connection unverified"},
		{name: "the configured ca verifies the server certificate", caFile: caFile},
		{name: "a wrong ca rejects the server certificate", caFile: otherCAFile, wantError: true},
		{name: "the system roots reject an unknown issuer", useSystemCa: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &v1.ClientCommonConfig{ServerAddr: "127.0.0.1", ServerPort: listener.Addr().(*net.UDPAddr).Port}
			cfg.Transport.Protocol = "quic"
			require.NoError(t, cfg.Complete())
			cfg.Transport.TLS.Enable = new(false)
			cfg.Transport.TLS.UseSystemCa = tc.useSystemCa
			cfg.Transport.TLS.TrustedCaFile = tc.caFile
			cfg.Transport.TLS.ServerName = "example.com"

			attemptCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			connector := NewConnector(attemptCtx, cfg)
			err := connector.Open()
			connector.Close()
			stop()

			if tc.wantError {
				require.ErrorContains(t, err, "unknown authority")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The wss and quic transports always run over TLS, so their material is
// applied even when transport.tls.enable is false. The other transports do
// not use TLS at all while it is disabled.
func TestClientTLSConfigMaterial(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedCert(t, "client")
	certPath := filepath.Join(dir, "client.crt")
	keyPath := filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	caPEM, _ := generateSelfSignedCert(t, "ca")
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	for _, tc := range []struct {
		name     string
		protocol string
		enable   bool
		wantTLS  bool
	}{
		{name: "tcp without tls", protocol: "tcp"},
		{name: "tcp with tls", protocol: "tcp", enable: true, wantTLS: true},
		{name: "websocket without tls", protocol: "websocket"},
		{name: "websocket with tls", protocol: "websocket", enable: true, wantTLS: true},
		{name: "wss keeps tls", protocol: "wss", wantTLS: true},
		{name: "quic keeps tls", protocol: "quic", wantTLS: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &v1.ClientCommonConfig{ServerAddr: "127.0.0.1", ServerPort: 7000}
			cfg.Transport.Protocol = tc.protocol
			require.NoError(t, cfg.Complete())
			cfg.Transport.TLS.Enable = new(tc.enable)
			cfg.Transport.TLS.CertFile = certPath
			cfg.Transport.TLS.KeyFile = keyPath
			cfg.Transport.TLS.TrustedCaFile = caFile

			connector := &defaultConnectorImpl{ctx: context.Background(), cfg: cfg}
			tlsConfig, err := connector.clientTLSConfig()
			require.NoError(t, err)
			if !tc.wantTLS {
				require.Nil(t, tlsConfig)
				return
			}
			require.NotNil(t, tlsConfig)
			require.Len(t, tlsConfig.Certificates, 1, "the client certificate must be presented")
			require.NotNil(t, tlsConfig.RootCAs, "the configured ca must be trusted")
			require.False(t, tlsConfig.InsecureSkipVerify, "the server certificate must be verified")
		})
	}
}
