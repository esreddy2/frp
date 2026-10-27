// Copyright 2017 fatedier, fatedier@gmail.com
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

package udp

import (
	"net"
	"sync"
	"time"

	"github.com/fatedier/golib/pool"

	"github.com/fatedier/frp/pkg/msg"
	netpkg "github.com/fatedier/frp/pkg/util/net"
)

func NewUDPPacket(buf []byte, laddr, raddr *net.UDPAddr) *msg.UDPPacket {
	content := make([]byte, len(buf))
	copy(content, buf)
	return &msg.UDPPacket{
		Content:    content,
		LocalAddr:  laddr,
		RemoteAddr: raddr,
	}
}

func GetContent(m *msg.UDPPacket) (buf []byte, err error) {
	return m.Content, nil
}

// ForwardUserConn forwards the packets of readCh to udpConn and the replies
// of udpConn to sendCh.
//
// The function returns when the connection fails or when stopCh is closed.
// The caller must not close readCh or sendCh while the function is running: a
// send on a closed channel panics, and the goroutine that sends is not always
// able to recover from it. The stop channel ends the function instead.
func ForwardUserConn(udpConn *net.UDPConn, readCh <-chan *msg.UDPPacket, sendCh chan<- *msg.UDPPacket, stopCh <-chan struct{}, bufSize int) {
	// read
	go func() {
		for {
			select {
			case udpMsg, ok := <-readCh:
				if !ok {
					return
				}
				buf, err := GetContent(udpMsg)
				if err != nil {
					continue
				}
				_, _ = udpConn.WriteToUDP(buf, udpMsg.RemoteAddr)
			case <-stopCh:
				return
			}
		}
	}()

	// write
	buf := pool.GetBuf(bufSize)
	defer pool.PutBuf(buf)

	// Wrap the write loop in a function that recovers from send-on-closed-channel panic.
	func() {
		defer func() {
			_ = recover() // Ignore panic from send on closed channel; function returns
		}()
		for {
			n, remoteAddr, err := udpConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			// NewUDPPacket copies buf[:n], so the read buffer can be reused
			udpMsg := NewUDPPacket(buf[:n], nil, remoteAddr)

			select {
			case sendCh <- udpMsg:
			default:
				// Channel full, drop packet
			}
		}
	}()
}

// udpSocketIdleTimeout bounds how long a udp socket towards the local
// service may stay silent before it is closed and the flow is restarted on a
// new socket.
const udpSocketIdleTimeout = 30 * time.Second

// removeUDPConn drops conn from the flow map, but only when it is still the
// socket registered for addr. A socket that has already been replaced must
// not drop its replacement from the map: the flow would then start a second
// socket for the same user while the first one is still alive.
func removeUDPConn(udpConnMap map[string]*net.UDPConn, mu *sync.RWMutex, addr string, conn *net.UDPConn) {
	mu.Lock()
	defer mu.Unlock()
	if current, ok := udpConnMap[addr]; ok && current == conn {
		delete(udpConnMap, addr)
	}
}

// Forwarder forwards the packets of readCh to the local service at dstAddr
// and the replies of the service back to sendCh.
//
// The function returns when the connection fails or when stopCh is closed.
// The caller must not close readCh or sendCh while the function is running: a
// send on a closed channel panics, and the goroutine that sends is not always
// able to recover from it. Closing a channel while another goroutine sends on
// it is a data race as well: the runtime records a send as a read of the
// channel and a close as a write of it. The stop channel ends the goroutines
// that use the channels instead.

// ForwarderOptions configures options for Forwarder.
type ForwarderOptions struct {
	ProxyProtocol     string
	ProxyProtocolMode string
	SocketMark        int
	OnError           func(err error, raddr net.Addr, dstAddr net.Addr)
}

// Forwarder forwards UDP packets between the local service and work connections.
// It creates a socket to the local service for each user address that sends a
// packet, and it tears the socket down when the user goes idle.
//
// In addition to proxyProtocol, optional extra arguments can be passed:
// - string: sets ProxyProtocolMode ("first-packet" or "per-packet")
// - ForwarderOptions / *ForwarderOptions: sets full options including SocketMark
func Forwarder(
	dstAddr *net.UDPAddr,
	readCh <-chan *msg.UDPPacket,
	sendCh chan<- msg.Message,
	stopCh <-chan struct{},
	bufSize int,
	proxyProtocol string,
	extra ...any,
) {
	opts := ForwarderOptions{
		ProxyProtocol: proxyProtocol,
	}
	for _, arg := range extra {
		switch v := arg.(type) {
		case string:
			opts.ProxyProtocolMode = v
		case ForwarderOptions:
			opts = v
		case *ForwarderOptions:
			if v != nil {
				opts = *v
			}
		}
	}
	if opts.ProxyProtocol == "" {
		opts.ProxyProtocol = proxyProtocol
	}
	forwarderWithOptions(dstAddr, readCh, sendCh, stopCh, bufSize, opts, udpSocketIdleTimeout)
}

// forwarderWithIdleTimeout is Forwarder with a configurable idle timeout, so
// that the retirement of an idle flow can be exercised without waiting for
// the production timeout.
func forwarderWithIdleTimeout(
	dstAddr *net.UDPAddr,
	readCh <-chan *msg.UDPPacket,
	sendCh chan<- msg.Message,
	stopCh <-chan struct{},
	bufSize int,
	proxyProtocol string,
	proxyProtocolMode string,
	idleTimeout time.Duration,
) {
	forwarderWithOptions(dstAddr, readCh, sendCh, stopCh, bufSize, ForwarderOptions{
		ProxyProtocol:     proxyProtocol,
		ProxyProtocolMode: proxyProtocolMode,
	}, idleTimeout)
}

func forwarderWithOptions(
	dstAddr *net.UDPAddr,
	readCh <-chan *msg.UDPPacket,
	sendCh chan<- msg.Message,
	stopCh <-chan struct{},
	bufSize int,
	options ForwarderOptions,
	idleTimeout time.Duration,
) {
	var mu sync.RWMutex
	udpConnMap := make(map[string]*net.UDPConn)

	// read from dstAddr and write to sendCh
	writerFn := func(raddr *net.UDPAddr, udpConn *net.UDPConn) {
		addr := raddr.String()
		defer func() {
			removeUDPConn(udpConnMap, &mu, addr, udpConn)
			udpConn.Close()
		}()

		buf := pool.GetBuf(bufSize)
		defer pool.PutBuf(buf)
		// The read deadline is an idle timeout: it is pushed forward while
		// packets keep arriving, so a flow whose service pushes data without
		// the user sending anything is not torn down. Arming the deadline only
		// once would expire it even on a socket that keeps receiving packets.
		deadline := netpkg.NewIdleReadDeadline(udpConn, idleTimeout)

		// Wrap the read loop in a function that recovers from send-on-closed-channel panic.
		func() {
			defer func() {
				_ = recover() // Ignore panic from send on closed channel; function returns
			}()
			for {
				n, _, err := udpConn.ReadFromUDP(buf)
				if err != nil {
					return
				}
				deadline.Refresh()

				udpMsg := NewUDPPacket(buf[:n], nil, raddr)

				// The stop channel is checked before the send, so that the
				// flow of a retired work connection ends with the first
				// packet of the service instead of with a random choice
				// between the send and the stop.
				select {
				case <-stopCh:
					return
				default:
				}
				select {
				case sendCh <- udpMsg:
				default:
					// Channel full, drop packet
				}
			}
		}()
	}

	// read from readCh
	go func() {
		for {
			select {
			case udpMsg, ok := <-readCh:
				if !ok {
					return
				}
				buf, err := GetContent(udpMsg)
				if err != nil {
					continue
				}

				mu.RLock()
				udpConn, ok := udpConnMap[udpMsg.RemoteAddr.String()]
				mu.RUnlock()

				// isNewFlow is true only when this packet creates the udp flow,
				// that is when the socket for the user address did not exist yet.
				isNewFlow := false
				if !ok {
					// Dial outside the lock to avoid blocking other readers/writers.
					var err error
					if options.ProxyProtocol == "tproxy" && udpMsg.RemoteAddr != nil {
						udpConn, err = netpkg.DialUDPTransparent(udpMsg.RemoteAddr, dstAddr, options.SocketMark)
					} else {
						udpConn, err = net.DialUDP("udp", nil, dstAddr)
					}
					if err != nil {
						if options.OnError != nil {
							options.OnError(err, udpMsg.RemoteAddr, dstAddr)
						}
						continue
					}

					mu.Lock()
					// Double-check after acquiring write lock.
					if existing, exists := udpConnMap[udpMsg.RemoteAddr.String()]; exists {
						udpConn.Close()
						udpConn = existing
					} else {
						udpConnMap[udpMsg.RemoteAddr.String()] = udpConn
						isNewFlow = true
					}
					mu.Unlock()

					if isNewFlow {
						go writerFn(udpMsg.RemoteAddr, udpConn)
					}
				}

				// Add proxy protocol header if configured.
				// By default ("first-packet"), only the first packet of a new flow carries the header.
				// In "per-packet" mode, every UDP datagram carries the header (required by HAProxy/NGINX).
				shouldAddProxyProtocol := (options.ProxyProtocol == "v1" || options.ProxyProtocol == "v2") &&
					udpMsg.RemoteAddr != nil &&
					(isNewFlow || options.ProxyProtocolMode == "per-packet")

				if shouldAddProxyProtocol {
					ppBuf, err := netpkg.BuildProxyProtocolHeader(udpMsg.RemoteAddr, dstAddr, options.ProxyProtocol)
					if err == nil {
						// Prepend proxy protocol header to the UDP payload
						finalBuf := make([]byte, len(ppBuf)+len(buf))
						copy(finalBuf, ppBuf)
						copy(finalBuf[len(ppBuf):], buf)
						buf = finalBuf
					}
				}

				_, err = udpConn.Write(buf)
				if err != nil {
					udpConn.Close()
					removeUDPConn(udpConnMap, &mu, udpMsg.RemoteAddr.String(), udpConn)
				} else {
					// Reset the idle deadline only when a write succeeds. Doing this
					// per packet is fine here because writes are relatively infrequent
					// compared to reads, and it keeps the socket from being closed
					// during a burst of incoming traffic.
					_ = udpConn.SetReadDeadline(time.Now().Add(idleTimeout))
				}
			case <-stopCh:
				mu.Lock()
				for _, conn := range udpConnMap {
					_ = conn.Close()
				}
				clear(udpConnMap)
				mu.Unlock()
				return
			}
		}
	}()
}
