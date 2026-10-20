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
	"crypto/tls"
	"net"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// frp must never use QUIC 0-RTT. The first message frp sends on a QUIC
// connection carries the client's credential, and 0-RTT early data is
// replayable, so an on-path attacker could capture the client's first flight
// and replay it to authenticate as that client. frp therefore neither offers
// nor accepts early data.
//
// This test points the connector at a server that supports everything 0-RTT
// needs (session tickets, an early listener and Allow0RTT) and checks that
// frp still performs a full, non-resumed handshake.
func TestQUICConnectorDoesNotUse0RTT(t *testing.T) {
	serverCert, serverKey := generateSelfSignedCert(t, "example.com")
	certificate, err := tls.X509KeyPair(serverCert, serverKey)
	require.NoError(t, err)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{"frp"},
		// The server offers session resumption, so a client that wanted to
		// resume or to use 0-RTT could do so here.
		SessionTicketsDisabled: false,
	}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	quicTransport := &quic.Transport{Conn: udpConn}
	defer quicTransport.Close()

	listener, err := quicTransport.ListenEarly(serverTLS, &quic.Config{Allow0RTT: true})
	require.NoError(t, err)
	defer listener.Close()

	accepted := make(chan *quic.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	cfg := &v1.ClientCommonConfig{
		ServerAddr: "127.0.0.1",
		ServerPort: udpConn.LocalAddr().(*net.UDPAddr).Port,
	}
	cfg.Transport.Protocol = "quic"
	require.NoError(t, cfg.Complete())
	cfg.Transport.QUIC.Enable0RTT = new(true)

	connector := NewConnector(context.Background(), cfg)
	defer connector.Close()

	// Two connections: the first one collects the server's session ticket, so
	// a client that keeps a session cache could resume or use 0-RTT on the
	// second one.
	for i := range 2 {
		require.NoError(t, connector.Open(), "connection %d", i)

		impl := connector.(*defaultConnectorImpl)
		// quic.DialAddr returns after the handshake completed, while
		// quic.DialAddrEarly returns before it. A completed handshake
		// therefore shows that the client did not use the early dial API.
		select {
		case <-impl.quicConn.HandshakeComplete():
		default:
			t.Fatalf("the handshake must be complete when the connector opens connection %d", i)
		}
		// A client session cache is the other prerequisite for 0-RTT:
		// without a session ticket the client cannot derive 0-RTT keys.
		tlsConfig, err := impl.clientTLSConfig()
		require.NoError(t, err)
		require.Nil(t, tlsConfig.ClientSessionCache, "the client must not keep a TLS session cache")

		state := impl.quicConn.ConnectionState()
		require.False(t, state.Used0RTT, "the client must not use 0-RTT on connection %d", i)
		require.False(t, state.TLS.DidResume, "the client must not resume a TLS session on connection %d", i)

		select {
		case serverConn := <-accepted:
			require.False(t, serverConn.ConnectionState().Used0RTT, "the server must not see 0-RTT on connection %d", i)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for the server to accept connection %d", i)
		}
		// The server sends its session ticket right after the handshake. Keep
		// the connection open long enough for the client to receive it, so
		// that a client which keeps a session cache could use it on the next
		// connection.
		time.Sleep(300 * time.Millisecond)
		require.NoError(t, impl.quicConn.CloseWithError(0, ""))
	}
}
