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

package server

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/config/v1/validation"
)

func freeLifecycleUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	port := conn.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, conn.Close())
	return port
}

// frp's QUIC listener must never issue a TLS session ticket and must never
// accept 0-RTT early data.
//
// The first message frp sends on a QUIC connection carries the client's
// credential, and 0-RTT early data is replayable, so an on-path attacker
// could capture a client's first flight and replay it to authenticate as
// that client. quic-go builds 0-RTT on TLS session resumption, so refusing to
// issue session tickets keeps 0-RTT unreachable.
//
// The test connects with a client that is as eager as possible: it keeps a
// TLS session cache and dials with quic.DialEarly, so it would use 0-RTT
// against any server that allows it. The server must neither resume the
// session nor negotiate 0-RTT.
func TestQUICListenerDoesNotAllow0RTT(t *testing.T) {
	cfg := &v1.ServerConfig{
		BindAddr: "127.0.0.1", ProxyBindAddr: "127.0.0.1",
		QUICBindPort: freeLifecycleUDPPort(t),
	}
	require.NoError(t, cfg.Complete())
	cfg.BindPort = 0
	_, err := validation.NewConfigValidator(nil).ValidateServerConfig(cfg)
	require.NoError(t, err)

	svr, err := NewService(cfg)
	require.NoError(t, err)
	defer svr.Close()
	require.NotNil(t, svr.quicListener)

	addr := svr.quicConn.LocalAddr().String()
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	require.NoError(t, err)

	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"frp"},
		ClientSessionCache: tls.NewLRUClientSessionCache(10),
	}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	quicTransport := &quic.Transport{Conn: udpConn}
	defer quicTransport.Close()

	for i := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := quicTransport.DialEarly(ctx, udpAddr, clientTLS, &quic.Config{})
		cancel()
		require.NoError(t, err, "connection %d", i)

		state := conn.ConnectionState()
		require.False(t, state.Used0RTT, "the client must not use 0-RTT on connection %d", i)
		require.False(t, state.TLS.DidResume, "the server must not issue a session ticket, connection %d", i)

		// The server sends its session ticket right after the handshake. Keep
		// the connection open long enough for the client to receive it, so
		// that a server which issues tickets is caught on the next
		// connection.
		time.Sleep(300 * time.Millisecond)
		require.NoError(t, conn.CloseWithError(0, ""))
	}
}
