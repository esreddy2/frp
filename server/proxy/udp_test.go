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

package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/naming"
	"github.com/fatedier/frp/pkg/proto/udp"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/ports"
)

// A udp work connection that keeps receiving messages must not be torn down.
// The read deadline is an idle timeout: it is pushed forward while messages
// arrive, so a busy work connection stays up for as long as it is busy.
// Arming the deadline only once would expire it after the timeout even on a
// connection that is receiving messages all the time, which tears down and
// recreates the work connection of every udp proxy once per minute.
func TestReadUDPWorkConnKeepsBusyConnectionAlive(t *testing.T) {
	const idleTimeout = 200 * time.Millisecond

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	clientMsgConn := msg.NewConn(client, msg.NewV1ReadWriter(client))
	serverMsgConn := msg.NewConn(server, msg.NewV1ReadWriter(server))

	readCh := make(chan *msg.UDPPacket, 64)
	closed := make(chan struct{})

	go func() {
		readUDPWorkConn(serverMsgConn, idleTimeout,
			func(m *msg.UDPPacket) error {
				readCh <- m
				return nil
			},
			func() { close(closed) },
			xlog.New())
	}()

	// Keep the connection busy for much longer than the idle timeout.
	busyUntil := time.Now().Add(10 * idleTimeout)
	for time.Now().Before(busyUntil) {
		require.NoError(t, clientMsgConn.WriteMsg(&msg.UDPPacket{Content: []byte("payload")}))
		select {
		case <-readCh:
		case <-time.After(5 * time.Second):
			t.Fatal("the work connection reader stopped delivering packets")
		}
		time.Sleep(idleTimeout / 10)
	}

	select {
	case <-closed:
		t.Fatal("the work connection was torn down while it was busy")
	default:
	}
}

// A work connection that stops sending messages is torn down once it stays
// idle for longer than the timeout.
func TestReadUDPWorkConnTearsDownIdleConnection(t *testing.T) {
	const idleTimeout = 200 * time.Millisecond

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	serverMsgConn := msg.NewConn(server, msg.NewV1ReadWriter(server))

	closed := make(chan struct{})
	go func() {
		readUDPWorkConn(serverMsgConn, idleTimeout,
			func(*msg.UDPPacket) error { return nil },
			func() { close(closed) },
			xlog.New())
	}()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the idle work connection was not torn down")
	}
}

// The udp proxy releases its port exactly once. The proxy's own teardown
// goroutine calls Close again after the control retired the proxy, and a
// second release would free a port that another proxy may have acquired in
// the meantime.
func TestUDPProxyCloseReleasesThePortOnlyOnce(t *testing.T) {
	require := require.New(t)

	rc := &controller.ResourceController{
		UDPPortManager: ports.NewManager("udp", "127.0.0.1", nil),
	}
	t.Cleanup(func() { rc.UDPPortManager.Close() })

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp", Type: string(v1.ProxyTypeUDP)},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			name:       "udp",
			rc:         rc,
			serverCfg:  &v1.ServerConfig{ProxyBindAddr: "127.0.0.1"},
			xl:         xlog.New(),
			ctx:        context.Background(),
			configurer: cfg,
			getWorkConnFn: func() (*WorkConn, error) {
				return nil, errors.New("no work connection available")
			},
		},
		cfg: cfg,
	}

	_, err := pxy.Run()
	require.NoError(err)
	port := pxy.realBindPort
	require.NotZero(port)

	pxy.Close()

	// Another proxy takes over the released port.
	acquired, err := rc.UDPPortManager.Acquire("other", port)
	require.NoError(err)
	require.Equal(port, acquired)

	// The second close of the retired proxy must not release the port that
	// now belongs to the other proxy.
	pxy.Close()

	_, err = rc.UDPPortManager.Acquire("third", port)
	require.ErrorIs(err, ports.ErrPortAlreadyUsed)
}

// A work connection that is acquired while the proxy is shutting down
// belongs to nobody: the proxy is already closed, so nobody would close
// the connection afterwards. Keeping it would leak the connection and the
// goroutine that serves it for as long as the client keeps the connection
// alive.
func TestUDPProxyDoesNotKeepWorkConnAcquiredAfterClose(t *testing.T) {
	require := require.New(t)

	rc := &controller.ResourceController{
		UDPPortManager: ports.NewManager("udp", "127.0.0.1", nil),
	}
	t.Cleanup(func() { rc.UDPPortManager.Close() })

	// The work connection is only handed out when the test releases it.
	release := make(chan struct{})
	acquired := make(chan struct{})
	var once sync.Once

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	clientMsgConn := msg.NewConn(client, msg.NewV1ReadWriter(client))
	workConn := NewWorkConn(msg.NewConn(server, msg.NewV1ReadWriter(server)))

	// The client side of the work connection: it reads the start message
	// and reports when the connection is closed.
	connClosed := make(chan struct{})
	go func() {
		defer close(connClosed)
		for {
			if _, err := clientMsgConn.ReadMsg(); err != nil {
				return
			}
		}
	}()

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp", Type: string(v1.ProxyTypeUDP)},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			name:       "udp",
			rc:         rc,
			serverCfg:  &v1.ServerConfig{ProxyBindAddr: "127.0.0.1"},
			xl:         xlog.New(),
			ctx:        context.Background(),
			configurer: cfg,
			getWorkConnFn: func() (*WorkConn, error) {
				once.Do(func() { close(acquired) })
				<-release
				return workConn, nil
			},
		},
		cfg: cfg,
	}

	_, err := pxy.Run()
	require.NoError(err)

	// Wait until the proxy asks for a work connection.
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("the proxy did not ask for a work connection")
	}

	// The proxy is closed while the work connection is still in flight.
	pxy.Close()
	close(release)

	// The connection must not survive the closed proxy.
	select {
	case <-connClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("the work connection of a closed proxy was kept alive")
	}
}

// A checkCloseCh signal received while the proxy is in backoff waiting to
// acquire a new work connection must not terminate the worker goroutine.
// checkCloseCh notifies that a previously active work connection dropped;
// only stopCh indicates proxy shutdown.
func TestUDPProxyWorkerSurvivesCheckCloseSignalDuringBackoff(t *testing.T) {
	require := require.New(t)

	rc := &controller.ResourceController{
		UDPPortManager: ports.NewManager("udp", "127.0.0.1", nil),
	}
	t.Cleanup(func() { rc.UDPPortManager.Close() })

	var attempts atomic.Int32
	secondAttemptCalled := make(chan struct{})

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	workConn := NewWorkConn(msg.NewConn(server, msg.NewV1ReadWriter(server)))

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp-backoff", Type: string(v1.ProxyTypeUDP)},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			name:       "udp-backoff",
			rc:         rc,
			serverCfg:  &v1.ServerConfig{ProxyBindAddr: "127.0.0.1"},
			xl:         xlog.New(),
			ctx:        context.Background(),
			configurer: cfg,
			getWorkConnFn: func() (*WorkConn, error) {
				attempt := attempts.Add(1)
				if attempt == 1 {
					return nil, errors.New("initial connection failure")
				}
				close(secondAttemptCalled)
				return workConn, nil
			},
		},
		cfg: cfg,
	}

	_, err := pxy.Run()
	require.NoError(err)
	defer pxy.Close()

	// Wait for the first attempt to fail and enter backoff.
	require.Eventually(func() bool {
		return attempts.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Send on checkCloseCh during backoff. With the defect (case <-pxy.checkCloseCh: return),
	// this caused premature worker termination and prevented future retry attempts.
	go func() {
		pxy.checkCloseCh <- 1
	}()

	// The worker loop must survive and perform the second attempt after backoff.
	select {
	case <-secondAttemptCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("the worker goroutine terminated prematurely during backoff on checkCloseCh")
	}
}

// The datagram lane identifies a proxy by its wire-level name, and the
// identifier field of a datagram frame holds a single length byte. A proxy
// whose wire-level name does not fit cannot be addressed by the lane at
// all, so the server must not enable it for that proxy: the traffic goes
// over the reliable work-connection stream instead.
func TestUDPProxyDoesNotUseTheDatagramLaneWhenTheWireNameDoesNotFit(t *testing.T) {
	require := require.New(t)

	tooLong := strings.Repeat("n", naming.MaxDatagramIdentifierLength+1)
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: tooLong, Type: string(v1.ProxyTypeUDP)},
		QUICDatagrams:   true,
	}
	base := &BaseProxy{
		name:       tooLong,
		xl:         xlog.New(),
		ctx:        context.Background(),
		configurer: cfg,
	}

	require.NotNil(NewUDPProxy(base))
	require.False(base.udpDatagramWanted,
		"the datagram lane must not be enabled for a wire name that does not fit")

	// The longest addressable name still uses the lane.
	fits := strings.Repeat("n", naming.MaxDatagramIdentifierLength)
	cfg.Name = fits
	base.name = fits
	require.NotNil(NewUDPProxy(base))
	require.True(base.udpDatagramWanted,
		"the datagram lane must be enabled for the longest addressable wire name")
}

// The work connection reader of the proxy sends the packets of the work
// connection on the channel of the flow. A send on a closed channel panics
// even when it is in a select with a default branch, and closing a channel
// while another goroutine sends on it is a data race. The proxy therefore
// ends its goroutines with the stop channel instead of closing the channels.
func TestUDPProxyCloseLeavesTheFlowChannelsOpen(t *testing.T) {
	require := require.New(t)

	rc := &controller.ResourceController{
		UDPPortManager: ports.NewManager("udp", "127.0.0.1", nil),
	}
	t.Cleanup(func() { rc.UDPPortManager.Close() })

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp", Type: string(v1.ProxyTypeUDP)},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			name:       "udp",
			rc:         rc,
			serverCfg:  &v1.ServerConfig{ProxyBindAddr: "127.0.0.1"},
			xl:         xlog.New(),
			ctx:        context.Background(),
			configurer: cfg,
			getWorkConnFn: func() (*WorkConn, error) {
				return nil, errors.New("no work connection available")
			},
		},
		cfg: cfg,
	}

	_, err := pxy.Run()
	require.NoError(err)

	// A sender that fills the channel of the flow and blocks on the next
	// send, like the work connection reader of a busy proxy does.
	senderStopped := make(chan struct{})
	senderPanic := make(chan any, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				senderPanic <- r
			}
		}()
		for {
			select {
			case pxy.readCh <- &msg.UDPPacket{Content: []byte("payload")}:
			case <-senderStopped:
				return
			}
		}
	}()
	// Wait until the channel of the flow is full, so that the sender is
	// blocked in a send when the proxy is closed.
	for len(pxy.readCh) < cap(pxy.readCh) {
		time.Sleep(time.Millisecond)
	}

	pxy.Close()
	close(senderStopped)

	select {
	case r := <-senderPanic:
		t.Fatalf("the send on the channel of a retired proxy panicked: %v", r)
	case <-time.After(time.Second):
	}

	// The channel of a retired proxy is not closed, so a late packet of
	// the work connection does not panic.
	require.NotPanics(func() {
		select {
		case pxy.readCh <- &msg.UDPPacket{Content: []byte("late")}:
		default:
		}
	})
}

// A datagram whose header does not fit is not a transport failure: the
// packet goes over the reliable work-connection stream and the work
// connection stays open. Tearing the connection down would leave the proxy
// without a usable work connection.
func TestUDPProxyDatagramFallsBackToStreamWhenTheHeaderDoesNotFit(t *testing.T) {
	require := require.New(t)

	pxy, peerConn, messages := startUDPProxyWithQUICWorkConn(t, &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp", Type: string(v1.ProxyTypeUDP)},
		QUICDatagrams:   true,
	})

	// A user address whose zone does not fit the address length byte of
	// the frame.
	longZone := &net.UDPAddr{
		IP:   net.IPv6loopback,
		Port: 4242,
		Zone: strings.Repeat("z", 300),
	}
	pxy.sendCh <- &msg.UDPPacket{Content: []byte("payload"), RemoteAddr: longZone}

	select {
	case raw := <-messages:
		packet, isPacket := raw.(*msg.UDPPacket)
		require.True(isPacket, "unexpected message type %T", raw)
		require.Equal([]byte("payload"), packet.Content)
	case <-time.After(10 * time.Second):
		t.Fatal("the packet did not fall back to the work-connection stream")
	}

	// The work connection is still usable: a second packet is relayed as
	// well, so the sender did not tear the connection down.
	pxy.sendCh <- &msg.UDPPacket{Content: []byte("second"), RemoteAddr: longZone}

	select {
	case raw := <-messages:
		packet, isPacket := raw.(*msg.UDPPacket)
		require.True(isPacket, "unexpected message type %T", raw)
		require.Equal([]byte("second"), packet.Content)
	case <-time.After(10 * time.Second):
		t.Fatal("the work connection was torn down after the first packet")
	}

	_ = peerConn
}

// The server sends datagrams under the wire-level name of the proxy, which
// is the name the client sent in the new-proxy message. A datagram that is
// addressed to the raw proxy name instead is not delivered, because the
// server binds the handler to the wire-level name.
func TestUDPProxyDatagramLaneUsesTheWireName(t *testing.T) {
	require := require.New(t)

	wireName := "dg-user.udp-proxy"
	pxy, peerConn, _ := startUDPProxyWithQUICWorkConn(t, &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: wireName, Type: string(v1.ProxyTypeUDP)},
		QUICDatagrams:   true,
	})

	// The peer of the datagram lane. The handler is bound to the
	// wire-level name, exactly like the client binds it.
	peerMux := udp.DatagramMuxFor(peerConn)
	datagrams := make(chan []byte, 4)
	peerMux.Register(wireName, func(_ *net.UDPAddr, payload []byte) {
		datagrams <- payload
	})

	pxy.sendCh <- &msg.UDPPacket{
		Content:    []byte("payload"),
		RemoteAddr: &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242},
	}

	select {
	case payload := <-datagrams:
		require.Equal([]byte("payload"), payload)
	case <-time.After(10 * time.Second):
		t.Fatal("the datagram was not delivered under the wire-level name")
	}
}

// startUDPProxyWithQUICWorkConn starts a udp proxy whose work connection
// is a stream of a quic connection, so that the datagram lane is available.
// It returns the proxy, the quic connection of the peer and the messages
// that the peer receives on the work connection.
func startUDPProxyWithQUICWorkConn(
	t *testing.T,
	cfg *v1.UDPProxyConfig,
) (pxy *UDPProxy, peerConn *quic.Conn, messages chan msg.Message) {
	t.Helper()
	require := require.New(t)

	serverConn, clientConn := quicConnPair(t)

	// The client side of the work connection.
	stream, err := clientConn.OpenStreamSync(context.Background())
	require.NoError(err)
	clientNetConn := netpkg.QuicStreamToNetConn(stream, clientConn)
	clientMsgConn := msg.NewConn(clientNetConn, msg.NewV1ReadWriter(clientNetConn))
	// The announcement is what makes the stream visible to the server.
	go func() {
		_ = clientMsgConn.WriteMsg(&msg.NewWorkConn{})
	}()
	messages = make(chan msg.Message, 4)
	go func() {
		defer close(messages)
		for {
			raw, readErr := clientMsgConn.ReadMsg()
			if readErr != nil {
				return
			}
			messages <- raw
		}
	}()

	// The server side of the work connection.
	accepted := make(chan *quic.Stream, 1)
	go func() {
		s, acceptErr := serverConn.AcceptStream(context.Background())
		if acceptErr == nil {
			accepted <- s
		}
	}()
	var serverNetConn net.Conn
	select {
	case s := <-accepted:
		serverNetConn = netpkg.QuicStreamToNetConn(s, serverConn)
	case <-time.After(10 * time.Second):
		t.Fatal("the work connection stream was not accepted")
	}
	workConn := NewWorkConn(msg.NewConn(serverNetConn, msg.NewV1ReadWriter(serverNetConn)))

	rc := &controller.ResourceController{
		UDPPortManager: ports.NewManager("udp", "127.0.0.1", nil),
	}
	t.Cleanup(func() { rc.UDPPortManager.Close() })

	// The work connection is only handed out once. A second request shows
	// that the proxy noticed that the work connection is gone.
	var firstWorkConn atomic.Bool
	firstWorkConn.Store(true)
	workConnRetried := make(chan struct{}, 1)

	base := &BaseProxy{
		name:       cfg.Name,
		rc:         rc,
		serverCfg:  &v1.ServerConfig{ProxyBindAddr: "127.0.0.1", UDPPacketSize: 1500},
		xl:         xlog.New(),
		ctx:        context.Background(),
		configurer: cfg,
		getWorkConnFn: func() (*WorkConn, error) {
			if !firstWorkConn.CompareAndSwap(true, false) {
				select {
				case workConnRetried <- struct{}{}:
				default:
				}
				return nil, errors.New("no work connection available")
			}
			return workConn, nil
		},
	}
	proxy, ok := NewUDPProxy(base).(*UDPProxy)
	require.True(ok)
	_, err = proxy.Run()
	require.NoError(err)
	t.Cleanup(func() {
		// The peer closes the work connection first, so that the reader of
		// the work connection is done before the proxy is closed. Closing
		// the proxy while that reader is still running makes it send on a
		// channel that the proxy has already closed.
		_ = clientNetConn.Close()
		select {
		case <-workConnRetried:
		case <-time.After(10 * time.Second):
			t.Error("the proxy did not notice the closed work connection")
		}
		proxy.Close()
	})

	// The start message of the work connection.
	select {
	case raw := <-messages:
		require.IsType(&msg.StartWorkConn{}, raw)
	case <-time.After(10 * time.Second):
		t.Fatal("the work connection was not started")
	}

	// Wait until the work connection is installed.
	require.Eventually(func() bool {
		proxy.mu.RLock()
		defer proxy.mu.RUnlock()
		return proxy.workConn != nil
	}, 10*time.Second, 10*time.Millisecond)

	return proxy, clientConn, messages
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
