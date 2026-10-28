package udp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/naming"
)

func TestEncodeDecodeDgramFrameRoundTrip(t *testing.T) {
	require := require.New(t)

	addr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 4242}
	payload := []byte("hello datagram")

	frame, err := encodeDgramFrame("proxy-a", addr, payload)
	require.NoError(err)

	name, gotAddr, gotPayload, err := decodeDgramFrame(frame)
	require.NoError(err)
	require.Equal("proxy-a", name)
	require.Equal(addr.String(), gotAddr.String())
	require.Equal(payload, gotPayload)
}

func TestEncodeDecodeDgramFrameEmptyPayload(t *testing.T) {
	require := require.New(t)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	frame, err := encodeDgramFrame("p", addr, nil)
	require.NoError(err)

	name, gotAddr, gotPayload, err := decodeDgramFrame(frame)
	require.NoError(err)
	require.Equal("p", name)
	require.Equal(addr.String(), gotAddr.String())
	require.Empty(gotPayload)
}

// The decoded name and payload must not alias the receive buffer: the
// quic receive buffer is reused by the next ReceiveDatagram, so a
// handler that keeps either value would observe it change underneath.
func TestDecodeDgramFrameDoesNotAliasReceiveBuffer(t *testing.T) {
	require := require.New(t)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	frame, err := encodeDgramFrame("proxy-alias", addr, []byte("payload-alias"))
	require.NoError(err)

	name, gotAddr, gotPayload, err := decodeDgramFrame(frame)
	require.NoError(err)

	// Scribble over the whole source buffer, as a reused receive buffer
	// would be overwritten.
	for i := range frame {
		frame[i] = 0xFF
	}

	require.Equal("proxy-alias", name)
	require.Equal(addr.String(), gotAddr.String())
	require.Equal([]byte("payload-alias"), gotPayload)
}

func TestDecodeDgramFrameRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{"empty", nil},
		{"too short", []byte{dgramFrameVersion}},
		{"bad version", []byte{0x00, 0x01, 'a'}},
		{"name truncated", []byte{dgramFrameVersion, 0x05, 'a', 'b'}},
		{"addr truncated", []byte{dgramFrameVersion, 0x01, 'a', 0x05, '1'}},
		{"addr not numeric", []byte{dgramFrameVersion, 0x01, 'a', 0x03, 'h', 'o', 's'}},
		{"addr not ip:port", []byte{dgramFrameVersion, 0x01, 'a', 0x05, 'n', 'o', 'p', 'o', 'r', 't'}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := decodeDgramFrame(tc.frame)
			require.Error(t, err)
		})
	}
}

func TestEncodeDgramFrameRejectsInvalidInput(t *testing.T) {
	require := require.New(t)

	_, err := encodeDgramFrame("p", nil, []byte("x"))
	require.Error(err)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

	_, err = encodeDgramFrame(string(make([]byte, 256)), addr, nil)
	require.Error(err)

	longAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1, Zone: string(make([]byte, 256))}
	_, err = encodeDgramFrame("p", longAddr, nil)
	require.Error(err)
}

// The identifier field of the frame holds a single length byte, so the
// longest addressable proxy name is 255 bytes. A longer name is a property
// of the proxy rather than of the packet, so the error has to be
// distinguishable from the "the frame is too large" error: the caller falls
// back to the reliable stream for a header that does not fit, while an
// oversized payload falls back per packet.
func TestEncodeDgramFrameRejectsAProxyNameThatDoesNotFit(t *testing.T) {
	require := require.New(t)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

	// The identifier field of the frame holds a single length byte, so the
	// longest addressable proxy name is 255 bytes. The limit is part of
	// the wire format, so it is pinned here instead of being derived from
	// the constant that the rest of the code uses.
	require.Equal(255, naming.MaxDatagramIdentifierLength)

	_, err := encodeDgramFrame(strings.Repeat("n", 255), addr, nil)
	require.NoError(err)

	// One byte more is rejected as a header that does not fit.
	_, err = encodeDgramFrame(strings.Repeat("n", 256), addr, nil)
	require.ErrorIs(err, ErrDatagramHeaderTooLong)
	require.NotErrorIs(err, ErrDatagramTooLarge)
}

// The same holds for the user address: an address that does not fit the
// length byte of the frame is reported as a header that does not fit, so
// the caller sends that packet over the reliable stream instead of tearing
// the work connection down.
func TestEncodeDgramFrameRejectsAnAddressThatDoesNotFit(t *testing.T) {
	require := require.New(t)

	longAddr := &net.UDPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 1,
		Zone: strings.Repeat("z", maxDatagramAddrLength),
	}
	_, err := encodeDgramFrame("p", longAddr, nil)
	require.ErrorIs(err, ErrDatagramHeaderTooLong)
	require.NotErrorIs(err, ErrDatagramTooLarge)
}

func TestEncodeDgramFrameRejectsOversizedPayload(t *testing.T) {
	require := require.New(t)

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

	// A payload that fits the frame budget is accepted.
	_, err := encodeDgramFrame("p", addr, make([]byte, DatagramFrameBudget-32))
	require.NoError(err)

	// One that does not is rejected before the frame is built, so the
	// caller can fall back to the reliable stream.
	_, err = encodeDgramFrame("p", addr, make([]byte, DatagramFrameBudget))
	require.ErrorIs(err, ErrDatagramTooLarge)
}

func TestDatagramMuxForRejectsNilConnection(t *testing.T) {
	require := require.New(t)

	require.Nil(DatagramMuxFor(nil))
}

func TestDatagramMuxRegisterReplacesHandler(t *testing.T) {
	require := require.New(t)

	mux := &DatagramMux{handlers: make(map[string]dgramHandler)}

	var first, second int
	mux.Register("p", func(_ *net.UDPAddr, _ []byte) { first++ })
	mux.Register("p", func(_ *net.UDPAddr, _ []byte) { second++ })

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	frame, err := encodeDgramFrame("p", addr, []byte("x"))
	require.NoError(err)

	mux.dispatch(frame)
	require.Equal(0, first)
	require.Equal(1, second)
}

func TestDatagramMuxUnregisterRemovesHandler(t *testing.T) {
	require := require.New(t)

	mux := &DatagramMux{handlers: make(map[string]dgramHandler)}

	called := false
	mux.Register("p", func(_ *net.UDPAddr, _ []byte) { called = true })
	mux.Unregister("p")

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	frame, err := encodeDgramFrame("p", addr, []byte("x"))
	require.NoError(err)

	mux.dispatch(frame)
	require.False(called)
}

func TestDatagramMuxDispatchDropsMalformedFrames(t *testing.T) {
	require := require.New(t)

	mux := &DatagramMux{handlers: make(map[string]dgramHandler)}

	called := false
	mux.Register("p", func(_ *net.UDPAddr, _ []byte) { called = true })

	mux.dispatch([]byte{0x00, 0x01, 'a'})
	require.False(called)
}

func TestDatagramMuxDispatchDropsUnknownProxy(t *testing.T) {
	require := require.New(t)

	mux := &DatagramMux{handlers: make(map[string]dgramHandler)}

	called := false
	mux.Register("p", func(_ *net.UDPAddr, _ []byte) { called = true })

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	frame, err := encodeDgramFrame("other", addr, []byte("x"))
	require.NoError(err)

	mux.dispatch(frame)
	require.False(called)
}

// A panicking handler must not take down the shared receive loop for
// every other proxy on the connection.
func TestDatagramMuxDispatchContainsHandlerPanic(t *testing.T) {
	require := require.New(t)

	mux := &DatagramMux{handlers: make(map[string]dgramHandler)}
	mux.Register("boom", func(_ *net.UDPAddr, _ []byte) { panic("handler failure") })

	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	frame, err := encodeDgramFrame("boom", addr, []byte("x"))
	require.NoError(err)

	require.NotPanics(func() { mux.dispatch(frame) })

	// The mux stays usable for the other proxies.
	other := false
	mux.Register("fine", func(_ *net.UDPAddr, _ []byte) { other = true })
	frame, err = encodeDgramFrame("fine", addr, []byte("y"))
	require.NoError(err)
	mux.dispatch(frame)
	require.True(other)
}

// TestDatagramMuxRelaysOverQUIC exercises the whole relay path over a
// real QUIC connection: encode, send, receive, decode and dispatch.
func TestDatagramMuxRelaysOverQUIC(t *testing.T) {
	require := require.New(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{generateTestCertificate(t)},
		NextProtos:   []string{"frp"},
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{
		EnableDatagrams: true,
	})
	require.NoError(err)
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

	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"frp"},
	}
	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{
		EnableDatagrams: true,
	})
	require.NoError(err)
	t.Cleanup(func() { _ = clientConn.CloseWithError(0, "") })

	serverConn := <-accepted
	require.NotNil(serverConn)
	t.Cleanup(func() { _ = serverConn.CloseWithError(0, "") })

	require.True(clientConn.ConnectionState().SupportsDatagrams.Remote)
	require.True(serverConn.ConnectionState().SupportsDatagrams.Remote)

	type received struct {
		addr    *net.UDPAddr
		payload []byte
	}
	got := make(chan received, 1)

	serverMux := DatagramMuxFor(serverConn)
	serverMux.Register("proxy-quic", func(addr *net.UDPAddr, payload []byte) {
		got <- received{addr: addr, payload: payload}
	})

	clientMux := DatagramMuxFor(clientConn)
	sendAddr := &net.UDPAddr{IP: net.IPv4(192, 168, 5, 4), Port: 5555}
	err = clientMux.Send("proxy-quic", sendAddr, []byte("quic payload"))
	require.NoError(err)

	select {
	case r := <-got:
		require.Equal(sendAddr.String(), r.addr.String())
		require.Equal([]byte("quic payload"), r.payload)
	case <-time.After(10 * time.Second):
		t.Fatal("datagram was not relayed over the quic connection")
	}
}

// TestDatagramMuxSendFallsBackWhenTooLarge checks that an oversized
// packet is reported with ErrDatagramTooLarge so the caller can fall
// back to the reliable stream.
func TestDatagramMuxSendFallsBackWhenTooLarge(t *testing.T) {
	require := require.New(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{generateTestCertificate(t)},
		NextProtos:   []string{"frp"},
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{
		EnableDatagrams: true,
	})
	require.NoError(err)
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

	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"frp"},
	}
	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{
		EnableDatagrams: true,
	})
	require.NoError(err)
	t.Cleanup(func() { _ = clientConn.CloseWithError(0, "") })

	serverConn := <-accepted
	require.NotNil(serverConn)
	t.Cleanup(func() { _ = serverConn.CloseWithError(0, "") })

	clientMux := DatagramMuxFor(clientConn)
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

	err = clientMux.Send("proxy", addr, make([]byte, DatagramFrameBudget+1))
	require.ErrorIs(err, ErrDatagramTooLarge)
}

func generateTestCertificate(t *testing.T) tls.Certificate {
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
