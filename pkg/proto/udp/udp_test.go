package udp

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pp "github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/msg"
)

func TestUdpPacket(t *testing.T) {
	require := require.New(t)

	buf := []byte("hello world")
	udpMsg := NewUDPPacket(buf, nil, nil)

	newBuf, err := GetContent(udpMsg)
	require.NoError(err)
	require.EqualValues(buf, newBuf)
}

func TestForwardUserConnReturnsWhenSendChannelIsClosed(t *testing.T) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	readCh := make(chan *msg.UDPPacket)
	sendCh := make(chan *msg.UDPPacket)
	close(sendCh)
	t.Cleanup(func() { close(readCh) })

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	done := make(chan struct{})
	go func() {
		ForwardUserConn(listener, readCh, sendCh, stopCh, 1500)
		close(done)
	}()

	sender, err := net.DialUDP("udp4", nil, listener.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sender.Close() })

	_, err = sender.Write([]byte("trigger"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForwardUserConn did not return after sending to a closed channel")
	}
}

// The stop channel ends the reader of the user connection. The caller
// cannot close readCh instead: the goroutine that sends on it would panic
// on a closed channel, and the panic is only contained by a recover.
func TestForwardUserConnStopsTheReaderOnTheStopChannel(t *testing.T) {
	require := require.New(t)

	service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = service.Close() })

	// The user side of the flow: the forwarder writes the packets of
	// readCh to it.
	user, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = user.Close() })

	readCh := make(chan *msg.UDPPacket, 8)
	sendCh := make(chan *msg.UDPPacket, 8)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(readCh) })

	go ForwardUserConn(service, readCh, sendCh, stopCh, 1500)

	readCh <- NewUDPPacket([]byte("before"), nil, user.LocalAddr().(*net.UDPAddr))

	buf := make([]byte, 64)
	require.NoError(user.SetReadDeadline(time.Now().Add(5 * time.Second)))
	n, _, err := user.ReadFromUDP(buf)
	require.NoError(err)
	require.Equal("before", string(buf[:n]))

	// The stop channel ends the reader of the forwarder.
	close(stopCh)
	time.Sleep(100 * time.Millisecond)

	readCh <- NewUDPPacket([]byte("after"), nil, user.LocalAddr().(*net.UDPAddr))

	require.NoError(user.SetReadDeadline(time.Now().Add(500 * time.Millisecond)))
	_, _, err = user.ReadFromUDP(buf)
	require.Error(err, "the reader of the forwarder was not stopped")
}

// The stop channel ends the reader of the forwarder. The caller cannot
// close readCh instead: the goroutine that sends on it would panic on a
// closed channel, and closing a channel while another goroutine sends on it
// is a data race that the race detector reports.
func TestForwarderStopsTheReaderOnTheStopChannel(t *testing.T) {
	require := require.New(t)

	// The local service of the flow: the forwarder writes the packets of
	// readCh to it.
	service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = service.Close() })

	readCh := make(chan *msg.UDPPacket, 8)
	sendCh := make(chan msg.Message, 8)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(readCh) })

	Forwarder(service.LocalAddr().(*net.UDPAddr), readCh, sendCh, stopCh, 1500, "")

	userAddr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 5), Port: 1234}
	readCh <- NewUDPPacket([]byte("before"), nil, userAddr)

	buf := make([]byte, 64)
	require.NoError(service.SetReadDeadline(time.Now().Add(5 * time.Second)))
	n, _, err := service.ReadFromUDP(buf)
	require.NoError(err)
	require.Equal("before", string(buf[:n]))

	// The stop channel ends the reader of the forwarder.
	close(stopCh)
	time.Sleep(100 * time.Millisecond)

	readCh <- NewUDPPacket([]byte("after"), nil, userAddr)

	require.NoError(service.SetReadDeadline(time.Now().Add(500 * time.Millisecond)))
	_, _, err = service.ReadFromUDP(buf)
	require.Error(err, "the reader of the forwarder was not stopped")
}

// When stopCh is closed, the forwarder must close all active flow sockets in
// udpConnMap immediately so ephemeral ports are not held open for the 30s idle timeout.
func TestForwarderClosesFlowSocketsOnStopChannel(t *testing.T) {
	require := require.New(t)

	service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = service.Close() })

	readCh := make(chan *msg.UDPPacket, 8)
	sendCh := make(chan msg.Message, 8)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(readCh) })

	Forwarder(service.LocalAddr().(*net.UDPAddr), readCh, sendCh, stopCh, 1500, "")

	userAddr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 5), Port: 1234}
	readCh <- NewUDPPacket([]byte("hello"), nil, userAddr)

	buf := make([]byte, 64)
	require.NoError(service.SetReadDeadline(time.Now().Add(5 * time.Second)))
	n, fromAddr, err := service.ReadFromUDP(buf)
	require.NoError(err)
	require.Equal("hello", string(buf[:n]))

	// Close stopCh. The forwarder must immediately close all active flow sockets
	// in udpConnMap rather than leaving them open for the 30-second idle timeout.
	close(stopCh)

	time.Sleep(100 * time.Millisecond)
	_, _ = service.WriteToUDP([]byte("late-reply"), fromAddr)

	select {
	case m := <-sendCh:
		t.Fatalf("unexpected packet forwarded after stopCh: %v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

// The proxy protocol header describes the start of a UDP flow, so it must
// only be prepended to the first packet of a flow. Every later packet of the
// same flow is plain payload: a service that parses the header once would
// otherwise see the payload of every subsequent packet prefixed with a
// second header.
func TestForwarderSendsProxyProtocolHeaderOnlyOnFirstPacketOfFlow(t *testing.T) {
	require := require.New(t)

	service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = service.Close() })

	received := make(chan []byte, 8)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, _, readErr := service.ReadFromUDP(buf)
			if readErr != nil {
				return
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			received <- payload
		}
	}()

	readCh := make(chan *msg.UDPPacket, 8)
	sendCh := make(chan msg.Message, 8)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(readCh) })

	userAddr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 5), Port: 1234}
	Forwarder(service.LocalAddr().(*net.UDPAddr), readCh, sendCh, stopCh, 1500, "v1")

	readCh <- NewUDPPacket([]byte("first"), nil, userAddr)
	readCh <- NewUDPPacket([]byte("second"), nil, userAddr)

	var first, second []byte
	select {
	case first = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("the first packet was not delivered to the local service")
	}
	select {
	case second = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("the second packet was not delivered to the local service")
	}

	require.True(bytes.HasPrefix(first, []byte("PROXY ")),
		"the first packet of a flow must carry the proxy protocol header, got %q", first)
	require.Contains(string(first), "first")

	require.Equal([]byte("second"), second,
		"only the first packet of a flow may carry the proxy protocol header")
}

func TestForwarderSendsProxyProtocolHeaderPerPacket(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			require := require.New(t)

			service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			require.NoError(err)
			t.Cleanup(func() { _ = service.Close() })

			received := make(chan []byte, 8)
			go func() {
				buf := make([]byte, 2048)
				for {
					n, _, readErr := service.ReadFromUDP(buf)
					if readErr != nil {
						return
					}
					payload := make([]byte, n)
					copy(payload, buf[:n])
					received <- payload
				}
			}()

			readCh := make(chan *msg.UDPPacket, 8)
			sendCh := make(chan msg.Message, 8)
			stopCh := make(chan struct{})
			t.Cleanup(func() { close(readCh) })

			userAddr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 5), Port: 1234}
			Forwarder(service.LocalAddr().(*net.UDPAddr), readCh, sendCh, stopCh, 1500, version, "per-packet")

			readCh <- NewUDPPacket([]byte("first"), nil, userAddr)
			readCh <- NewUDPPacket([]byte("second"), nil, userAddr)

			var first, second []byte
			select {
			case first = <-received:
			case <-time.After(5 * time.Second):
				t.Fatal("the first packet was not delivered to the local service")
			}
			select {
			case second = <-received:
			case <-time.After(5 * time.Second):
				t.Fatal("the second packet was not delivered to the local service")
			}

			// In per-packet mode, BOTH first and second packets carry the proxy protocol header.
			for _, pkt := range [][]byte{first, second} {
				rd := bufio.NewReader(bytes.NewReader(pkt))
				header, err := pp.Read(rd)
				require.NoError(err)
				if version == "v1" {
					require.Equal(byte(1), header.Version)
				} else {
					require.Equal(byte(2), header.Version)
					require.NotNil(header.SourceAddr)
					host, _, err := net.SplitHostPort(header.SourceAddr.String())
					require.NoError(err)
					require.Equal(userAddr.IP.String(), host)
				}
				rem, err := io.ReadAll(rd)
				require.NoError(err)
				require.True(string(rem) == "first" || string(rem) == "second")
			}
		})
	}
}

// serviceDatagram is a datagram that the local service of a test received.
type serviceDatagram struct {
	payload []byte
	from    *net.UDPAddr
}

// receiveServiceDatagram returns the next datagram the local service
// received.
func receiveServiceDatagram(t *testing.T, ch <-chan serviceDatagram) serviceDatagram {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("the local service did not receive the packet")
		return serviceDatagram{}
	}
}

// A udp flow that stays idle is retired: the socket towards the local
// service is closed and the flow is removed from the flow map, so the next
// packet of the same user starts a new flow on a new socket.
//
// A flow that keeps the socket of a retired flow leaks that socket and its
// file descriptor for the lifetime of the process, and a flow that keeps the
// map entry of a retired flow writes into a closed socket, which loses every
// packet of the user.
func TestForwarderRetiresIdleFlow(t *testing.T) {
	require := require.New(t)

	const idleTimeout = 300 * time.Millisecond

	// The local service: it records every datagram it receives together
	// with the address it came from, and echoes the payload back.
	receivedCh := make(chan serviceDatagram, 8)
	service, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = service.Close() })
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, readErr := service.ReadFromUDP(buf)
			if readErr != nil {
				return
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			receivedCh <- serviceDatagram{payload: payload, from: from}
			_, _ = service.WriteToUDP(payload, from)
		}
	}()

	readCh := make(chan *msg.UDPPacket, 8)
	sendCh := make(chan msg.Message, 8)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(readCh) })

	userAddr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 5), Port: 1234}
	forwarderWithIdleTimeout(service.LocalAddr().(*net.UDPAddr), readCh, sendCh, stopCh, 1500, "v1", "", idleTimeout)

	// The first packet of a user opens the flow, so it carries the proxy
	// protocol header.
	readCh <- NewUDPPacket([]byte("first"), nil, userAddr)
	first := receiveServiceDatagram(t, receivedCh)
	require.True(bytes.HasPrefix(first.payload, []byte("PROXY ")),
		"the first packet of a flow must carry the proxy protocol header, got %q", first.payload)
	require.Contains(string(first.payload), "first")

	// The reply of the service travels back through the same flow.
	select {
	case raw := <-sendCh:
		require.IsType(&msg.UDPPacket{}, raw)
	case <-time.After(5 * time.Second):
		t.Fatal("the reply of the local service did not reach the user")
	}

	// A later packet of the same flow is plain payload: the header
	// describes the start of the flow.
	readCh <- NewUDPPacket([]byte("second"), nil, userAddr)
	second := receiveServiceDatagram(t, receivedCh)
	require.False(bytes.HasPrefix(second.payload, []byte("PROXY ")),
		"only the first packet of a flow may carry the proxy protocol header, got %q", second.payload)
	require.Equal([]byte("second"), second.payload)

	// The flow is idle now. Once the idle timeout expires, the socket
	// towards the service is closed, which releases the port it used.
	require.Eventually(func() bool {
		probe, probeErr := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: first.from.Port})
		if probeErr != nil {
			return false
		}
		_ = probe.Close()
		return true
	}, 10*time.Second, 20*time.Millisecond,
		"the socket of the idle flow was not closed")

	// The retired flow was removed from the flow map as well, so the next
	// packet of the same user starts a new flow and carries the proxy
	// protocol header again.
	readCh <- NewUDPPacket([]byte("third"), nil, userAddr)
	third := receiveServiceDatagram(t, receivedCh)
	require.True(bytes.HasPrefix(third.payload, []byte("PROXY ")),
		"a packet that follows the retirement of a flow must start a new flow, got %q", third.payload)
	require.Contains(string(third.payload), "third")
}

// A socket that has already been replaced must not drop its replacement
// from the flow map: the flow would then open a second socket towards the
// local service for the same user while the first one is still alive, and
// the replies of the service would be split over both sockets.
func TestRemoveUDPConnDoesNotDropAReplacementSocket(t *testing.T) {
	require := require.New(t)

	var mu sync.RWMutex
	udpConnMap := make(map[string]*net.UDPConn)

	first := &net.UDPConn{}
	second := &net.UDPConn{}
	udpConnMap["user"] = first
	udpConnMap["user"] = second

	// The socket that was replaced cleans up after itself.
	removeUDPConn(udpConnMap, &mu, "user", first)

	current, ok := udpConnMap["user"]
	require.True(ok, "the replacement socket must still be registered")
	require.Same(second, current)

	// The owner of the entry removes it.
	removeUDPConn(udpConnMap, &mu, "user", second)
	_, ok = udpConnMap["user"]
	require.False(ok, "the socket must be removed by its owner")
}
