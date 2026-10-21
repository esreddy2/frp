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

//go:build !frps

package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/naming"
	"github.com/fatedier/frp/pkg/proto/udp"
	"github.com/fatedier/frp/pkg/util/limit"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
)

// scriptedReadWriter hands out udp packets that the test pushes, and reports
// every read attempt so that the test can tell when the reader is waiting
// for the next packet.
type scriptedReadWriter struct {
	packets   chan *msg.UDPPacket
	readCalls chan struct{}
}

func newScriptedReadWriter() *scriptedReadWriter {
	return &scriptedReadWriter{
		packets:   make(chan *msg.UDPPacket),
		readCalls: make(chan struct{}, 16),
	}
}

func (r *scriptedReadWriter) ReadMsg() (msg.Message, error) {
	m := &msg.UDPPacket{}
	if err := r.ReadMsgInto(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (r *scriptedReadWriter) ReadMsgInto(m msg.Message) error {
	r.readCalls <- struct{}{}
	pkt := <-r.packets
	*(m.(*msg.UDPPacket)) = *pkt
	return nil
}

func (r *scriptedReadWriter) WriteMsg(msg.Message) error { return nil }

// A full read channel means the consumer is stalled, not that the reader
// must stop. The reader has to survive the stall: once the consumer catches
// up the flow must continue, instead of staying dead until the work
// connection is re-established.
func TestReadUDPWorkConnKeepsReadingAfterAFullChannel(t *testing.T) {
	require := require.New(t)

	rw := newScriptedReadWriter()
	// The channel has one slot, like the read channel of a proxy it has
	// more than one, and the reader sends without waiting. A send
	// therefore succeeds while the slot is free and is dropped while the
	// slot is taken: the state of the channel is decided by the test, not
	// by a race between the reader and the consumer.
	readCh := make(chan *msg.UDPPacket, 1)

	done := make(chan struct{})
	go func() {
		readUDPWorkConn(rw, readCh, xlog.New())
		close(done)
	}()

	// Wait until the reader asks for the first packet.
	select {
	case <-rw.readCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not start")
	}
	rw.packets <- &msg.UDPPacket{Content: []byte("first")}

	// The request for the next packet proves that the reader handed the
	// first packet over: the slot of the channel is taken now.
	select {
	case <-rw.readCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not ask for the second packet")
	}

	// The consumer is stalled, so the next packet has to be dropped
	// instead of blocking the reader.
	rw.packets <- &msg.UDPPacket{Content: []byte("dropped")}

	// The reader must still be running and ask for the next packet.
	select {
	case <-rw.readCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader stopped after the channel was full")
	}

	// The consumer catches up, so the flow continues.
	var pkt *msg.UDPPacket
	select {
	case pkt = <-readCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the first packet was not delivered")
	}
	require.Equal("first", string(pkt.Content))

	rw.packets <- &msg.UDPPacket{Content: []byte("third")}

	select {
	case <-rw.readCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not ask for the fourth packet")
	}
	select {
	case pkt = <-readCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the packet after the dropped one was not delivered")
	}
	require.Equal("third", string(pkt.Content))

	select {
	case <-done:
		t.Fatal("the reader returned while the work connection was still usable")
	default:
	}
}

// The read channel is closed when the proxy shuts down. A send on a closed
// channel panics even when the send sits in a select with a default branch,
// so the reader must contain that panic: an uncontained panic in a goroutine
// takes the whole client process down.
func TestReadUDPWorkConnStopsWhenReadChannelIsClosed(t *testing.T) {
	rw := newScriptedReadWriter()
	readCh := make(chan *msg.UDPPacket, 1)
	close(readCh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		readUDPWorkConn(rw, readCh, xlog.New())
	}()

	// The reader picks the packet up and hands it to the closed channel.
	rw.packets <- &msg.UDPPacket{Content: []byte("payload")}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not stop after the read channel was closed")
	}
}

// The send channel is closed when the proxy shuts down, and the heartbeat
// writes to it. A send on a closed channel panics even when the send sits in
// a select with a default branch, so the heartbeat must contain that panic.
func TestUDPHeartbeatStopsWhenSendChannelIsClosed(t *testing.T) {
	sendCh := make(chan msg.Message, 1)
	close(sendCh)
	closeCh := make(chan struct{})
	tick := make(chan time.Time)

	done := make(chan struct{})
	go func() {
		defer close(done)
		udpHeartbeat(sendCh, closeCh, tick, xlog.New())
	}()

	// The tick makes the heartbeat write to the closed channel.
	tick <- time.Now()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the heartbeat did not stop after the send channel was closed")
	}
}

// The QUIC datagram lane bypasses the limiter-wrapped work connection, so
// the datagrams are accounted explicitly. The accounting has to actually
// delay the packets: a context that is already canceled makes the limiter's
// WaitN return immediately, which would silently disable the bandwidth
// limit for the whole life of the work connection.
//
// The reply of the local service is much larger than the limit's burst, so
// the charge of a single reply needs more than one burst worth of tokens
// and the sender has to wait for the limiter to refill. The request is
// empty on purpose: an empty request is not charged, so it is not dropped
// while the limiter is busy.
func TestUDPProxyDatagramLaneEnforcesBandwidthLimit(t *testing.T) {
	require := require.New(t)

	echoAddr := startEchoService(t)
	serverConn, clientConn := quicConnPair(t)

	// 500 bytes per second, so the burst is 500 bytes as well.
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "udp-limit",
			Type: string(v1.ProxyTypeUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   echoAddr.IP.String(),
				LocalPort: echoAddr.Port,
			},
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{UDPPacketSize: 1500},
			limiter:   limit.NewBandwidthLimiter(500),
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg: cfg,
	}
	require.NoError(pxy.Run())
	t.Cleanup(pxy.Close)

	stream, err := clientConn.OpenStreamSync(context.Background())
	require.NoError(err)
	pxy.InWorkConn(netpkg.QuicStreamToNetConn(stream, clientConn), &msg.StartWorkConn{UDPDatagram: true})

	// The server side of the relay.
	serverMux := udp.DatagramMuxFor(serverConn)
	replies := make(chan time.Time, 4)
	serverMux.Register(cfg.Name, func(_ *net.UDPAddr, _ []byte) {
		replies <- time.Now()
	})

	userAddr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242}
	require.NoError(serverMux.Send(cfg.Name, userAddr, nil))
	time.Sleep(100 * time.Millisecond)
	require.NoError(serverMux.Send(cfg.Name, userAddr, nil))

	var sent []time.Time
	for range 2 {
		select {
		case at := <-replies:
			sent = append(sent, at)
		case <-time.After(15 * time.Second):
			t.Fatal("the client did not relay all replies")
		}
	}

	// The second reply is held back until the limiter has refilled the
	// tokens that the first reply consumed.
	require.Greater(sent[1].Sub(sent[0]), 700*time.Millisecond,
		"the datagram lane did not enforce the bandwidth limit")
}

// TestUDPProxyDatagramNameIncludesUserPrefix verifies that when the client is
// configured with a user, datagramName returns the wire-level proxy name with
// the user prefix. The server registers and sends datagrams under this same
// wire name; without the prefix, datagrams are silently dropped on both sides.
func TestUDPProxyDatagramNameIncludesUserPrefix(t *testing.T) {
	require := require.New(t)

	user := "dg-user"
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "udp-dg-wire",
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{User: user},
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg: cfg,
	}

	require.Equal(naming.AddUserPrefix(user, cfg.Name), pxy.datagramName(),
		"datagramName must include the user prefix")

	// With no user configured, datagramName returns the raw name.
	pxy.clientCfg.User = ""
	require.Equal(cfg.Name, pxy.datagramName(),
		"datagramName must return the raw name without a user")
}

// TestUDPProxyDatagramLaneDeliversToWireName verifies that a QUIC datagram
// addressed to the wire-level proxy name is delivered to a handler registered
// under that same name on the peer's mux. This is the cross-proxy contract:
// frpc registers under naming.AddUserPrefix(user, name) and frps registers
// under pxy.GetName() (the wire name); they must match.
func TestUDPProxyDatagramLaneDeliversToWireName(t *testing.T) {
	require := require.New(t)

	serverConn, clientConn := quicConnPair(t)

	user := "dg-user"
	name := "udp-dg-wire"
	wireName := naming.AddUserPrefix(user, name)

	// Client-side mux registers under the wire name (as the proxy does).
	clientMux := udp.DatagramMuxFor(clientConn)
	clientReceived := make(chan []byte, 4)
	clientMux.Register(wireName, func(_ *net.UDPAddr, payload []byte) {
		clientReceived <- payload
	})

	// Server-side mux sends under the wire name (as the server proxy does).
	serverMux := udp.DatagramMuxFor(serverConn)
	userAddr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242}
	require.NoError(serverMux.Send(wireName, userAddr, []byte("hello-datagram")))

	select {
	case payload := <-clientReceived:
		require.Equal([]byte("hello-datagram"), payload)
	case <-time.After(5 * time.Second):
		t.Fatal("the client mux did not receive the datagram addressed to the wire name")
	}

	// The raw name (without user prefix) is not registered on the client mux.
	serverMux.Send(name, userAddr, []byte("stray-datagram"))
	select {
	case <-clientReceived:
		t.Fatal("the client mux received a datagram addressed to the raw name")
	case <-time.After(100 * time.Millisecond):
	}
}

// The datagram lane identifies a proxy by its wire-level name, and the
// identifier field of a datagram frame holds a single length byte. A proxy
// whose wire-level name does not fit cannot be addressed by the lane at
// all, so its packets have to go over the reliable work-connection stream
// instead of being dropped.
//
// The wire-level name includes the user prefix, so a proxy name that fits on
// its own can still overflow once the prefix is added. Both cases are
// covered: the boundary is the effective name on the wire, not the local
// configuration name.
func TestUDPProxyDatagramFallsBackToStreamWhenTheWireNameDoesNotFit(t *testing.T) {
	tests := []struct {
		name      string
		user      string
		proxyName string
	}{
		{
			name:      "the proxy name alone does not fit",
			proxyName: strings.Repeat("n", naming.MaxDatagramIdentifierLength+1),
		},
		{
			name:      "the user prefix pushes the name over the limit",
			user:      "dg-user",
			proxyName: strings.Repeat("n", naming.MaxDatagramIdentifierLength-len("dg-user.")+1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			serverConn, clientConn := quicConnPair(t)

			cfg := &v1.UDPProxyConfig{
				ProxyBaseConfig: v1.ProxyBaseConfig{
					Name: tc.proxyName,
					Type: string(v1.ProxyTypeUDP),
					ProxyBackend: v1.ProxyBackend{
						LocalIP:   "127.0.0.1",
						LocalPort: 9,
					},
				},
			}
			pxy := &UDPProxy{
				BaseProxy: &BaseProxy{
					baseCfg:   &cfg.ProxyBaseConfig,
					clientCfg: &v1.ClientCommonConfig{User: tc.user, UDPPacketSize: 1500},
					xl:        xlog.New(),
					ctx:       context.Background(),
				},
				cfg: cfg,
			}
			require.NoError(pxy.Run())
			t.Cleanup(pxy.Close)

			wireName := naming.AddUserPrefix(tc.user, cfg.Name)
			require.Greater(len(wireName), naming.MaxDatagramIdentifierLength,
				"the test needs a wire name that does not fit the datagram identifier field")

			stream, err := clientConn.OpenStreamSync(context.Background())
			require.NoError(err)
			pxy.InWorkConn(netpkg.QuicStreamToNetConn(stream, clientConn), &msg.StartWorkConn{UDPDatagram: true})

			// The peer of the datagram lane: a datagram addressed to the
			// wire name must not be delivered, the packet has to travel
			// over the work-connection stream.
			serverMux := udp.DatagramMuxFor(serverConn)
			datagrams := make(chan []byte, 4)
			serverMux.Register(wireName, func(_ *net.UDPAddr, payload []byte) {
				datagrams <- payload
			})

			sendCh := waitForProxySendChannel(t, pxy)
			sendCh <- &msg.UDPPacket{
				Content:    []byte("payload"),
				RemoteAddr: &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242},
			}

			require.Equal([]byte("payload"), readWorkConnPacket(t, serverConn),
				"the packet did not fall back to the work-connection stream")

			select {
			case payload := <-datagrams:
				t.Fatalf("the packet was delivered as a datagram: %q", payload)
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// The longest addressable wire name still uses the datagram lane: the
// identifier field holds a single length byte, so 255 bytes is the exact
// boundary between the datagram lane and the stream fallback.
func TestUDPProxyDatagramLaneWorksForTheLongestAddressableWireName(t *testing.T) {
	require := require.New(t)

	serverConn, clientConn := quicConnPair(t)

	user := "dg-user"
	proxyName := strings.Repeat("n", naming.MaxDatagramIdentifierLength-len("dg-user."))
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: proxyName,
			Type: string(v1.ProxyTypeUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   "127.0.0.1",
				LocalPort: 9,
			},
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{User: user, UDPPacketSize: 1500},
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg: cfg,
	}
	require.NoError(pxy.Run())
	t.Cleanup(pxy.Close)

	wireName := naming.AddUserPrefix(user, cfg.Name)
	require.Len(wireName, naming.MaxDatagramIdentifierLength)

	stream, err := clientConn.OpenStreamSync(context.Background())
	require.NoError(err)
	pxy.InWorkConn(netpkg.QuicStreamToNetConn(stream, clientConn), &msg.StartWorkConn{UDPDatagram: true})

	serverMux := udp.DatagramMuxFor(serverConn)
	datagrams := make(chan []byte, 4)
	serverMux.Register(wireName, func(_ *net.UDPAddr, payload []byte) {
		datagrams <- payload
	})

	sendCh := waitForProxySendChannel(t, pxy)
	sendCh <- &msg.UDPPacket{
		Content:    []byte("payload"),
		RemoteAddr: &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242},
	}

	select {
	case payload := <-datagrams:
		require.Equal([]byte("payload"), payload)
	case <-time.After(5 * time.Second):
		t.Fatal("the datagram addressed to the longest addressable wire name was not delivered")
	}
}

// waitForProxySendChannel returns the send channel of the work connection
// that the proxy installed.
func waitForProxySendChannel(t *testing.T, pxy *UDPProxy) chan msg.Message {
	t.Helper()

	var sendCh chan msg.Message
	require.Eventually(t, func() bool {
		pxy.mu.RLock()
		defer pxy.mu.RUnlock()
		sendCh = pxy.sendCh
		return sendCh != nil
	}, 5*time.Second, 10*time.Millisecond)
	return sendCh
}

// readWorkConnPacket returns the payload of the first udp packet that the
// proxy writes to the work-connection stream.
//
// A QUIC stream is only flushed when something is written to it, so the
// accept of the stream completes exactly when the packet is written.
func readWorkConnPacket(t *testing.T, serverConn *quic.Conn) []byte {
	t.Helper()

	type readResult struct {
		content []byte
		err     error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		stream, err := serverConn.AcceptStream(context.Background())
		if err != nil {
			resultCh <- readResult{err: err}
			return
		}
		netConn := netpkg.QuicStreamToNetConn(stream, serverConn)
		rw, err := msg.NewUDPPacketReadWriter(netConn, "", "")
		if err != nil {
			resultCh <- readResult{err: err}
			return
		}
		raw, err := msg.NewConn(netConn, rw).ReadMsg()
		if err != nil {
			resultCh <- readResult{err: err}
			return
		}
		packet, ok := raw.(*msg.UDPPacket)
		if !ok {
			resultCh <- readResult{err: fmt.Errorf("unexpected message type %T", raw)}
			return
		}
		resultCh <- readResult{content: packet.Content}
	}()

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		return result.content
	case <-time.After(10 * time.Second):
		t.Fatal("the proxy did not write a packet to the work-connection stream")
		return nil
	}
}

// startEchoService starts a udp service that sends every packet it receives
// back to its sender.
func startEchoService(t *testing.T) *net.UDPAddr {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// The reply is much larger than the request, so that the charge of a
	// single reply exceeds the burst of the limiter.
	go func() {
		buf := make([]byte, 2048)
		reply := make([]byte, 1000)
		for {
			_, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(reply, addr)
		}
	}()

	return conn.LocalAddr().(*net.UDPAddr)
}

// quicConnPair returns a connected pair of quic connections that both
// support datagrams.
func quicConnPair(t *testing.T) (serverConn, clientConn *quic.Conn) {
	t.Helper()

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{generateProxyTestCertificate(t)},
		NextProtos:   []string{"frp"},
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{EnableDatagrams: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	accepted := make(chan *quic.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept(ctx)
		if acceptErr != nil {
			return
		}
		accepted <- conn
	}()

	clientTLS := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"frp"}}
	clientConn, err = quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{EnableDatagrams: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.CloseWithError(0, "") })

	serverConn = <-accepted
	require.NotNil(t, serverConn)
	t.Cleanup(func() { _ = serverConn.CloseWithError(0, "") })

	return serverConn, clientConn
}

func generateProxyTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"frp test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// Two work connections for the same proxy can be handled at the same time:
// the client starts a goroutine per work connection. The goroutines of a
// work connection have to use the channels of that work connection, not the
// ones the proxy holds after another work connection replaced them.
//
// The test is meaningful under the race detector: the unsynchronized access
// to the proxy's channel fields is reported as a data race.
func TestUDPProxyConcurrentWorkConnsUseTheirOwnChannels(t *testing.T) {
	pxy := newUDPProxyForLifecycleTest(t, "udp-concurrent")

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			pxy.InWorkConn(server, &msg.StartWorkConn{})
		})
	}
	wg.Wait()
}

// newUDPProxyForLifecycleTest returns a udp proxy that is ready to receive
// work connections.
func newUDPProxyForLifecycleTest(t *testing.T, name string) *UDPProxy {
	t.Helper()
	require := require.New(t)

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: string(v1.ProxyTypeUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   "127.0.0.1",
				LocalPort: 9,
			},
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{UDPPacketSize: 1500},
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg: cfg,
	}
	require.NoError(pxy.Run())
	t.Cleanup(pxy.Close)
	return pxy
}

// A work connection that arrives after the proxy was retired must not be
// installed. The proxy is gone at that point: nobody would close the
// connection or its channels again, so the reader, the sender, the heartbeat
// and the forwarding goroutines of the connection would run until the peer
// goes away, and the socket towards the local service would stay open.
func TestUDPProxyDropsAWorkConnThatArrivesAfterClose(t *testing.T) {
	require := require.New(t)

	pxy := newUDPProxyForLifecycleTest(t, "udp-closed")

	// The proxy is retired by its owner.
	pxy.Close()

	conn := newTrackedConn(t)
	pxy.InWorkConn(conn, &msg.StartWorkConn{})

	require.Eventually(conn.isClosed, 5*time.Second, 10*time.Millisecond,
		"the work connection of a closed proxy was kept alive")

	pxy.mu.RLock()
	closed := pxy.closed
	workConn := pxy.workConn
	pxy.mu.RUnlock()
	require.True(closed, "the proxy must stay closed")
	require.Nil(workConn, "the closed proxy must not hold a work connection")
}

// A work connection that is replaced must be closed by the proxy: the reader,
// the sender, the heartbeat and the forwarding goroutines of a replaced work
// connection are only stopped when its channels are closed.
//
// Two work connections for the same proxy can be handled at the same time,
// so the replacement has to be atomic. A replacement that closes the
// previous work connection and installs the new one in two separate steps
// leaves a work connection behind when the two steps interleave: it is
// installed after the last close, so nobody closes it and its goroutines
// keep running.
//
// The proxy under test compresses the work connection, which makes the
// setup of a work connection slow enough for the two steps to interleave
// regularly.
func TestUDPProxyClosesEveryReplacedWorkConn(t *testing.T) {
	require := require.New(t)

	const workConns = 8

	pxy := newUDPProxyForLifecycleTest(t, "udp-replaced")
	pxy.cfg.Transport.UseCompression = true

	conns := make([]*trackedConn, 0, workConns)
	for range workConns {
		conns = append(conns, newTrackedConn(t))
	}

	// All the work connections are handed to the proxy at the same time.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Go(func() {
			<-start
			pxy.InWorkConn(conn, &msg.StartWorkConn{})
		})
	}
	close(start)
	wg.Wait()

	// The proxy keeps exactly one work connection. Every other one has to
	// be closed, otherwise its goroutines keep running.
	require.Eventually(func() bool {
		open := 0
		for _, conn := range conns {
			if !conn.isClosed() {
				open++
			}
		}
		return open <= 1
	}, 5*time.Second, 10*time.Millisecond,
		"a replaced work connection was not closed by the proxy")
}

// The channels of a retired work connection are not closed: the goroutines
// of the work connection send on them, and a send on a closed channel
// panics even when it is in a select with a default branch. The panic
// happens in a goroutine that does not always recover it, and closing a
// channel while another goroutine sends on it is a data race that the race
// detector reports.
//
// The stop channel ends the goroutines of the work connection instead.
func TestUDPProxyCloseLeavesTheFlowChannelsOpen(t *testing.T) {
	require := require.New(t)

	pxy := newUDPProxyForLifecycleTest(t, "udp-flow-channels")

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	pxy.InWorkConn(server, &msg.StartWorkConn{})

	// Wait until the work connection is installed and take its channels.
	var readCh chan *msg.UDPPacket
	var sendCh chan msg.Message
	require.Eventually(func() bool {
		pxy.mu.RLock()
		defer pxy.mu.RUnlock()
		readCh, sendCh = pxy.readCh, pxy.sendCh
		return readCh != nil && sendCh != nil
	}, 5*time.Second, 10*time.Millisecond)

	pxy.Close()

	// A late packet of the work connection does not panic: the channels of
	// a retired proxy are not closed.
	require.NotPanics(func() {
		select {
		case readCh <- &msg.UDPPacket{Content: []byte("late")}:
		default:
		}
		select {
		case sendCh <- &msg.Ping{}:
		default:
		}
	})
}

// The retirement of a work connection must not race with the reader of that
// work connection. The reader sends on the read channel of the proxy, and the
// retirement used to close that channel while the reader was still running.
//
// The runtime records a send as a read of the channel and a close as a write
// of it, so the race detector reports the two as a data race. The panic that
// the send on a closed channel causes is contained by a recover, but the race
// itself is not: the test is meaningful under the race detector.
func TestUDPProxyRetirementDoesNotRaceWithTheReader(t *testing.T) {
	require := require.New(t)

	localAddr, received := startRecordingService(t)
	pxy := newUDPProxyForLocalService(t, "udp-retire-race", localAddr)

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	pxy.InWorkConn(server, &msg.StartWorkConn{})

	// Deliver a packet, so that the reader of the work connection sends it
	// on the read channel of the proxy.
	rw, err := msg.NewUDPPacketReadWriter(client, "", "")
	require.NoError(err)
	require.NoError(msg.NewConn(client, rw).WriteMsg(&msg.UDPPacket{Content: []byte("payload")}))

	// The local service received the packet, so the reader has sent it on
	// the read channel of the proxy.
	select {
	case payload := <-received:
		require.Contains(string(payload), "payload")
	case <-time.After(5 * time.Second):
		t.Fatal("the local service did not receive the packet")
	}

	// The proxy is retired while the reader of the work connection is
	// still running.
	pxy.Close()
	time.Sleep(100 * time.Millisecond)
}

// startRecordingService starts a udp service that records every packet it
// receives.
func startRecordingService(t *testing.T) (*net.UDPAddr, <-chan []byte) {
	t.Helper()
	require := require.New(t)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(err)
	t.Cleanup(func() { _ = conn.Close() })

	received := make(chan []byte, 8)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, _, readErr := conn.ReadFromUDP(buf)
			if readErr != nil {
				return
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			received <- payload
		}
	}()

	return conn.LocalAddr().(*net.UDPAddr), received
}

// newUDPProxyForLocalService returns a udp proxy that forwards to the given
// local service and that is ready to receive work connections.
func newUDPProxyForLocalService(t *testing.T, name string, localAddr *net.UDPAddr) *UDPProxy {
	t.Helper()
	require := require.New(t)

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: string(v1.ProxyTypeUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   localAddr.IP.String(),
				LocalPort: localAddr.Port,
			},
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{UDPPacketSize: 1500},
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg: cfg,
	}
	require.NoError(pxy.Run())
	t.Cleanup(pxy.Close)
	return pxy
}

// trackedConn is a work connection that records whether the proxy closed it.
type trackedConn struct {
	net.Conn

	mu     sync.Mutex
	closed bool
}

func newTrackedConn(t *testing.T) *trackedConn {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	// The proxy reads from its side of the connection; drain the peer so
	// that a write never blocks.
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()
	return &trackedConn{Conn: server}
}

// isClosed reports whether the proxy closed its side of the connection.
func (c *trackedConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *trackedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.Conn.Close()
}
