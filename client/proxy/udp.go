// Copyright 2023 The frp Authors
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
	"errors"
	"net"
	"reflect"
	"strconv"
	"time"

	liberrors "github.com/fatedier/golib/errors"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/naming"
	"github.com/fatedier/frp/pkg/proto/udp"
	"github.com/fatedier/frp/pkg/util/limit"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
)

func init() {
	RegisterProxyFactory(reflect.TypeFor[*v1.UDPProxyConfig](), NewUDPProxy)
}

type UDPProxy struct {
	*BaseProxy

	cfg *v1.UDPProxyConfig

	localAddr *net.UDPAddr
	readCh    chan *msg.UDPPacket

	// include msg.UDPPacket and msg.Ping
	sendCh   chan msg.Message
	workConn net.Conn
	closed   bool
	closeCh  chan struct{}

	// QUIC datagram relay (RFC 9221). Non-nil while the server has
	// confirmed datagram support for this proxy on the current work
	// connection; packets are then sent as unreliable QUIC datagrams
	// instead of over the work-connection stream.
	dgMux *udp.DatagramMux
}

func NewUDPProxy(baseProxy *BaseProxy, cfg v1.ProxyConfigurer) Proxy {
	unwrapped, ok := cfg.(*v1.UDPProxyConfig)
	if !ok {
		return nil
	}
	return &UDPProxy{
		BaseProxy: baseProxy,
		cfg:       unwrapped,
	}
}

// datagramName returns the wire-level proxy name used for QUIC datagram
// mux registration. It includes the user prefix when the client is
// configured with a user, matching the server side which registers under
// the wire name (the name carried in the new-proxy message).
func (pxy *UDPProxy) datagramName() string {
	return naming.AddUserPrefix(pxy.clientCfg.User, pxy.cfg.Name)
}

func (pxy *UDPProxy) Run() (err error) {
	pxy.localAddr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(pxy.cfg.LocalIP, strconv.Itoa(pxy.cfg.LocalPort)))
	if err != nil {
		return err
	}
	// The local UDP socket is dialed per destination by the Forwarder,
	// so no persistent local connection is needed here.
	return nil
}

// Close retires the proxy. It is called by the owner of the proxy when the
// proxy is stopped, and it is the only place that stops the work connection
// that is currently installed.
func (pxy *UDPProxy) Close() {
	pxy.mu.Lock()
	defer pxy.mu.Unlock()

	if pxy.closed {
		return
	}
	pxy.closed = true
	pxy.retireWorkConnLocked()
}

// retireWorkConnLocked stops the work connection that is currently installed
// on the proxy: the connection is closed and its goroutines are ended, which
// stops the reader, the sender, the heartbeat and the forwarding goroutines
// that belong to it.
//
// The caller must hold pxy.mu.
func (pxy *UDPProxy) retireWorkConnLocked() {
	if pxy.workConn != nil {
		_ = pxy.workConn.Close()
		pxy.workConn = nil
	}
	// Unregister from the datagram mux first to prevent send-on-closed-channel race
	if pxy.dgMux != nil {
		pxy.dgMux.Unregister(pxy.datagramName())
		pxy.dgMux = nil
	}
	// The channels of the work connection are not closed. The goroutines of
	// the work connection send on them, and a send on a closed channel
	// panics even when it is in a select with a default branch. Closing a
	// channel while another goroutine sends on it is a data race as well:
	// the runtime records a send as a read of the channel and a close as a
	// write of it, so the race detector reports the retirement of the work
	// connection against the reader, the heartbeat, the datagram lane and
	// the forwarding goroutine. The stop channel ends those goroutines
	// instead, and the channels are collected with the work connection.
	if pxy.closeCh != nil {
		close(pxy.closeCh)
		pxy.closeCh = nil
	}
	pxy.readCh = nil
	pxy.sendCh = nil
}

// readUDPWorkConn reads udp packets from the work connection and forwards
// them to readCh until the connection fails.
//
// A packet is dropped when readCh is full, because udp is lossy anyway and
// the consumer is stalled. The reader keeps running in that case: stopping
// it would leave the flow dead until the work connection is re-established,
// which turns a temporary stall into a broken proxy.
//
// The read channel is not closed when the proxy shuts down, so a send on it
// cannot panic. The recover is kept as a safety net: a shutdown that races
// with this reader must not take the whole client process down.
func readUDPWorkConn(rw msg.ReadWriter, readCh chan<- *msg.UDPPacket, xl *xlog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			xl.Infof("reader goroutine for udp work connection closed: %v", r)
		}
	}()
	for {
		var udpMsg msg.UDPPacket
		if errRet := rw.ReadMsgInto(&udpMsg); errRet != nil {
			xl.Warnf("read from workConn for udp error: %v", errRet)
			return
		}
		// Avoid wrapping the send in errors.PanicToError, which allocates a
		// closure and a defer/recover on every UDP packet. A non-blocking
		// select is sufficient: a full channel means the proxy is stalled
		// and the packet is dropped, not that the reader must stop.
		xl.Tracef("get udp package from workConn, len: %d", len(udpMsg.Content))
		select {
		case readCh <- &udpMsg:
		default:
			xl.Tracef("udp readCh is full, drop a udp package")
		}
	}
}

// udpHeartbeat sends a ping on the work connection on every tick until the
// connection is closed.
//
// The send channel is not closed when the proxy shuts down, so a send on it
// cannot panic. The recover is kept as a safety net: a shutdown that races
// with a tick must not take the whole client process down.
func udpHeartbeat(sendCh chan msg.Message, closeCh <-chan struct{}, tick <-chan time.Time, xl *xlog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			xl.Tracef("heartbeat goroutine for udp work connection closed: %v", r)
		}
	}()
	for {
		select {
		case <-tick:
			select {
			case sendCh <- &msg.Ping{}:
			default:
				xl.Tracef("udp sendCh is full, drop ping")
			}
		case <-closeCh:
			xl.Tracef("heartbeat goroutine for udp work connection closed")
			return
		}
	}
}

// InWorkConn is called when a new work connection is established.
// For QUIC with datagrams enabled, the datagram path is set up eagerly in
// Run(), so work connections are not used for UDP traffic. We still accept
// the work connection to keep the client's pool healthy but do not forward
// data through it.
func (pxy *UDPProxy) InWorkConn(conn net.Conn, startMsg *msg.StartWorkConn) {
	xl := pxy.xl
	xl.Infof("incoming a new work connection for udp proxy, %s", conn.RemoteAddr().String())

	// QUIC datagram lane, confirmed by the server for this work connection.
	// Requires the negotiated transport to be QUIC and the peer to support
	// datagrams; mixed-version peers fall back to the stream path silently.
	var dgMux *udp.DatagramMux
	if startMsg != nil && startMsg.UDPDatagram &&
		!pxy.cfg.Transport.UseEncryption && !pxy.cfg.Transport.UseCompression {
		if qc := netpkg.GetQuicConnFromNetConn(conn); qc != nil && qc.ConnectionState().SupportsDatagrams.Remote {
			dgMux = udp.DatagramMuxFor(qc)
			xl.Infof("udp proxy relaying via quic datagrams")
		} else {
			xl.Warnf("server enabled quic datagrams but work connection does not support them, using stream")
		}
	}

	remote, _, err := pxy.wrapWorkConn(conn, pxy.encryptionKey)
	if err != nil {
		xl.Errorf("wrap work connection: %v", err)
		return
	}

	workConn := netpkg.WrapReadWriteCloserToConn(remote, conn)
	// Plain UDP payload follows the configured wire protocol for message framing.
	payloadRW, err := msg.NewUDPPacketReadWriter(workConn, pxy.clientCfg.Transport.WireProtocol, pxy.udpPacketCodec)
	if err != nil {
		xl.Errorf("create UDP packet read writer: %v", err)
		workConn.Close()
		return
	}

	// Install the new work connection and retire the previous one under a
	// single lock. Two work connections for the same proxy can be handled
	// at the same time (the client starts a goroutine per work
	// connection), and a replacement that is not atomic leaves the
	// replaced work connection behind: nobody closes it afterwards, so its
	// reader, sender, heartbeat and forwarding goroutines keep running and
	// its socket towards the local service stays open.
	//
	// The channels are captured in local variables: the goroutines below
	// must use the channels of their own work connection instead of the
	// ones the proxy holds now.
	pxy.mu.Lock()
	if pxy.closed {
		// The proxy was retired while this work connection was on its
		// way. Installing it would resurrect the proxy: nobody would
		// close the connection or its channels again, and the goroutines
		// of the connection would run until the peer goes away.
		pxy.mu.Unlock()
		xl.Warnf("udp proxy is closed, drop the new work connection")
		_ = workConn.Close()
		return
	}
	readCh := make(chan *msg.UDPPacket, 1024)
	sendCh := make(chan msg.Message, 1024)
	closeCh := make(chan struct{})
	pxy.retireWorkConnLocked()
	pxy.workConn = workConn
	pxy.dgMux = dgMux
	pxy.readCh = readCh
	pxy.sendCh = sendCh
	pxy.closeCh = closeCh
	if dgMux != nil {
		dgMux.Register(pxy.datagramName(), func(remoteAddr *net.UDPAddr, payload []byte) {
			// The bandwidth limiter can only police (drop) here, not shape:
			// this callback runs on the quic connection's shared receive
			// loop, which must never block.
			if !limit.AllowN(pxy.limiter, time.Now(), len(payload)) {
				return
			}
			m := udp.NewUDPPacket(payload, nil, remoteAddr)
			_ = liberrors.PanicToError(func() {
				select {
				case readCh <- m:
				case <-closeCh:
					// The work connection is retired: the channel is not
					// read anymore, so the packet is dropped.
				default:
				}
			})
		})
	}
	pxy.mu.Unlock()

	// Cancellable context tied to this work connection's lifecycle. The
	// datagram send path uses it for the limiter WaitN so a stall cannot
	// block the sole outbound pump forever; canceling it on teardown lets
	// WaitN return promptly instead of leaking the goroutine. Close() is
	// the sole owner of channel closing, so goroutines only cancel ctx.
	//
	// The context must outlive the call to InWorkConn: a canceled context
	// makes the limiter's WaitN return immediately, which would silently
	// disable the bandwidth limit for the whole life of the work
	// connection. It is therefore canceled when the reader or the sender
	// of this work connection exits, which is also what happens when the
	// proxy closes the work connection.
	ctx, cancel := context.WithCancel(context.Background())

	workConnReaderFn := func(rw msg.ReadWriter, readCh chan *msg.UDPPacket) {
		defer cancel()
		readUDPWorkConn(rw, readCh, xl)
	}
	workConnSenderFn := func(rw msg.ReadWriter, sendCh chan msg.Message) {
		defer cancel()
		var errRet error
		for {
			select {
			case rawMsg, ok := <-sendCh:
				if !ok {
					return
				}
				switch m := rawMsg.(type) {
				case *msg.UDPPacket:
					xl.Tracef("send udp package to workConn, len: %d", len(m.Content))
					// prefer the unreliable QUIC datagram lane; oversized
					// packets fall back to the stream
					if dgMux != nil {
						dgErr := dgMux.Send(pxy.datagramName(), m.RemoteAddr, m.Content)
						switch {
						case dgErr == nil:
							// Datagrams bypass the limiter-wrapped work
							// connection, so account for them here: charging
							// after the send (like limit.Reader does) delays
							// subsequent packets to hold the configured rate,
							// and never double-charges the stream fallback.
							_ = limit.WaitN(ctx, pxy.limiter, len(m.Content))
							continue
						case errors.Is(dgErr, udp.ErrDatagramTooLarge), errors.Is(dgErr, udp.ErrDatagramHeaderTooLong):
							if pxy.cfg.QUICDatagramStrict {
								xl.Tracef("quic datagram lane: dropping oversized packet (%d bytes) in strict mode", len(m.Content))
								continue
							}
							// The packet does not fit the datagram lane: the
							// payload is too large, or a header field (the
							// proxy name or the user address) is longer than
							// the frame can carry. The header case is a
							// property of the proxy, not of the packet, so it
							// must not be treated as a transport failure: the
							// packet goes over the reliable stream instead.
						default:
							xl.Errorf("udp datagram write error: %v", dgErr)
							return
						}
					}
				case *msg.Ping:
					xl.Tracef("send ping message to udp workConn")
				}
				if errRet = rw.WriteMsg(rawMsg); errRet != nil {
					xl.Errorf("udp work write error: %v", errRet)
					return
				}
			case <-closeCh:
				// The work connection is retired: the channel is not read
				// anymore.
				return
			}
		}
	}

	go workConnSenderFn(payloadRW, sendCh)
	go workConnReaderFn(payloadRW, readCh)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		udpHeartbeat(sendCh, closeCh, ticker.C, xl)
	}()

	// Call Forwarder with proxy protocol and transparent proxy configuration
	udp.Forwarder(
		pxy.localAddr,
		readCh,
		sendCh,
		closeCh,
		int(pxy.clientCfg.UDPPacketSize),
		pxy.cfg.Transport.ProxyProtocol,
		udp.ForwarderOptions{
			ProxyProtocol:     pxy.cfg.Transport.ProxyProtocol,
			ProxyProtocolMode: pxy.cfg.Transport.ProxyProtocolMode,
			SocketMark:        pxy.cfg.Transport.SocketMark,
			OnError: func(err error, raddr net.Addr, dstAddr net.Addr) {
				xl.Errorf("transparent udp dial to %s from %s failed: %v", dstAddr, raddr, err)
			},
		},
	)
}
