// Copyright 2019 fatedier, fatedier@gmail.com
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
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"time"

	liberrors "github.com/fatedier/golib/errors"
	libio "github.com/fatedier/golib/io"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/naming"
	"github.com/fatedier/frp/pkg/proto/udp"
	"github.com/fatedier/frp/pkg/util/limit"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/server/metrics"
)

func init() {
	RegisterProxyFactory(reflect.TypeFor[*v1.UDPProxyConfig](), NewUDPProxy)
}

type UDPProxy struct {
	*BaseProxy
	cfg *v1.UDPProxyConfig

	realBindPort int

	// udpConn is the listener of udp packages
	udpConn *net.UDPConn

	// there are always only one workConn at the same time
	// get another one if it closed
	workConn net.Conn

	// sendCh is used for sending packages to workConn
	sendCh chan *msg.UDPPacket

	// readCh is used for reading packages from workConn
	readCh chan *msg.UDPPacket

	// checkCloseCh is used for watching if workConn is closed
	checkCloseCh chan int

	// stopCh is closed when the proxy is retired. It replaces the closing
	// of the channels above: the proxy's own goroutines send on them, and a
	// send on a channel that is closed at the same time is a data race and
	// panics.
	stopCh chan struct{}

	isClosed bool

	// dgSizeReporter throttles the reports of packets that do not fit
	// into the datagram lane.
	dgSizeReporter *udp.SizeReporter
}

func NewUDPProxy(baseProxy *BaseProxy) Proxy {
	unwrapped, ok := baseProxy.GetConfigurer().(*v1.UDPProxyConfig)
	if !ok {
		return nil
	}
	baseProxy.usedPortsNum = 1
	// QUIC datagram relay bypasses the work-connection stream, so it can't
	// honor the per-proxy stream wrappers (encryption/compression).
	baseProxy.udpDatagramWanted = unwrapped.QUICDatagrams &&
		!unwrapped.Transport.UseEncryption && !unwrapped.Transport.UseCompression
	baseProxy.udpDatagramStrict = baseProxy.udpDatagramWanted && unwrapped.QUICDatagramStrict
	if baseProxy.udpDatagramWanted && !naming.DatagramIdentifierFits(baseProxy.name) {
		// The datagram lane addresses a proxy by its wire-level name, and
		// the identifier field of a datagram frame holds a single length
		// byte. A name that does not fit cannot be addressed at all, so
		// the proxy uses the reliable work-connection stream instead.
		baseProxy.xl.Warnf("udp proxy [%s] wire name is too long for the quic datagram lane (%d > %d bytes), using the reliable stream",
			baseProxy.name, len(baseProxy.name), naming.MaxDatagramIdentifierLength)
		baseProxy.udpDatagramWanted = false
	}
	return &UDPProxy{
		BaseProxy:      baseProxy,
		cfg:            unwrapped,
		dgSizeReporter: udp.NewSizeReporter(udp.DatagramSizeReportInterval),
	}
}

func (pxy *UDPProxy) Run() (remoteAddr string, err error) {
	xl := pxy.xl
	pxy.realBindPort, err = pxy.rc.UDPPortManager.Acquire(pxy.name, pxy.cfg.RemotePort)
	if err != nil {
		return "", fmt.Errorf("acquire port %d error: %v", pxy.cfg.RemotePort, err)
	}
	defer func() {
		if err != nil {
			pxy.rc.UDPPortManager.Release(pxy.realBindPort)
			// The port is released here. A later Close must not release it a
			// second time, because another proxy may have acquired it by then.
			pxy.realBindPort = 0
		}
	}()

	remoteAddr = fmt.Sprintf(":%d", pxy.realBindPort)
	pxy.cfg.RemotePort = pxy.realBindPort
	addr, errRet := net.ResolveUDPAddr("udp", net.JoinHostPort(pxy.serverCfg.ProxyBindAddr, strconv.Itoa(pxy.realBindPort)))
	if errRet != nil {
		err = errRet
		return
	}
	udpConn, errRet := net.ListenUDP("udp", addr)
	if errRet != nil {
		err = errRet
		xl.Warnf("listen udp port error: %v", err)
		return
	}
	xl.Infof("udp proxy listen port [%d]", pxy.cfg.RemotePort)

	pxy.udpConn = udpConn
	pxy.sendCh = make(chan *msg.UDPPacket, 1024)
	pxy.readCh = make(chan *msg.UDPPacket, 1024)
	pxy.checkCloseCh = make(chan int)
	pxy.stopCh = make(chan struct{})

	// Check if QUIC datagram relay is wanted for this proxy. When set, the
	// work-connection loop in runWithStreams creates a DatagramMux and routes
	// packets through it; otherwise the proxy behaves exactly as before.
	// runWithStreams is non-blocking (it spawns its own goroutines), so it
	// is always launched in the background for a consistent lifecycle.
	go pxy.runWithStreams(xl, udpConn)

	return remoteAddr, nil
}

// udpWorkConnIdleTimeout bounds how long a udp work connection may stay
// silent before the server tears it down and asks the client for a new one.
// The client sends a heartbeat every 30s, so a healthy connection never
// reaches it.
const udpWorkConnIdleTimeout = 60 * time.Second

// readUDPWorkConn reads udp packets from a work connection and hands them to
// onPacket. It returns when the connection fails, when the packet handler
// fails, or when the connection stays idle for longer than idleTimeout.
//
// The read deadline is an idle timeout: it is pushed forward while messages
// keep arriving, so a busy work connection is never torn down. Arming the
// deadline only once would expire it after idleTimeout even on a connection
// that is receiving messages all the time.
func readUDPWorkConn(
	payloadConn *msg.Conn,
	idleTimeout time.Duration,
	onPacket func(*msg.UDPPacket) error,
	onClosed func(),
	xl *xlog.Logger,
) {
	deadline := netpkg.NewIdleReadDeadline(payloadConn, idleTimeout)
	defer deadline.Clear()

	for {
		xl.Tracef("loop waiting message from udp workConn")
		// client will send heartbeat in workConn for keeping alive
		rawMsg, err := payloadConn.ReadMsg()
		if err != nil {
			xl.Warnf("read from workConn for udp error: %v", err)
			_ = payloadConn.Close()
			// notify proxy to start a new work connection
			// ignore error here, it means the proxy is closed
			_ = liberrors.PanicToError(onClosed)
			return
		}
		deadline.Refresh()
		switch m := rawMsg.(type) {
		case *msg.Ping:
			xl.Tracef("udp work conn get ping message")
			continue
		case *msg.UDPPacket:
			if err := onPacket(m); err != nil {
				_ = payloadConn.Close()
				xl.Infof("reader goroutine for udp work connection closed")
				return
			}
		}
	}
}

// runWithStreams is the stream-based implementation for non-QUIC or when datagrams are disabled.
func (pxy *UDPProxy) runWithStreams(xl *xlog.Logger, udpConn *net.UDPConn) {
	// read message from workConn, if it returns any error, notify proxy to start a new workConn
	workConnReaderFn := func(payloadConn *msg.Conn) {
		readUDPWorkConn(payloadConn, udpWorkConnIdleTimeout,
			func(m *msg.UDPPacket) error {
				return liberrors.PanicToError(func() {
					xl.Tracef("get udp message from workConn, len: %d", len(m.Content))
					// The send is interruptible: a full channel must not
					// keep the reader blocked while the proxy is
					// shutting down.
					select {
					case pxy.readCh <- m:
						metrics.Server.AddTrafficOut(
							pxy.GetName(),
							pxy.GetConfigurer().GetBaseConfig().Type,
							int64(len(m.Content)),
						)
					case <-pxy.stopCh:
					}
				})
			},
			func() {
				// notify proxy to start a new work connection
				// ignore error here, it means the proxy is closed
				select {
				case pxy.checkCloseCh <- 1:
				case <-pxy.stopCh:
				}
			},
			xl)
	}

	// send message to workConn; when dgMux is set, packets go out as
	// unreliable QUIC datagrams and only oversized ones use the stream
	workConnSenderFn := func(payloadConn *msg.Conn, dgMux *udp.DatagramMux, ctx context.Context) {
		var errRet error
		for {
			select {
			case udpMsg, ok := <-pxy.sendCh:
				if !ok {
					xl.Infof("sender goroutine for udp work connection closed")
					return
				}
				if dgMux != nil {
					dgErr := dgMux.Send(pxy.GetName(), udpMsg.RemoteAddr, udpMsg.Content)
					switch {
					case dgErr == nil:
						// Datagrams bypass the limiter-wrapped work
						// connection, so account for them here: charging
						// after the send (like limit.Reader does) delays
						// subsequent packets to hold the configured rate,
						// and never double-charges the stream fallback.
						_ = limit.WaitN(ctx, pxy.GetLimiter(), len(udpMsg.Content))
						metrics.Server.AddTrafficIn(
							pxy.GetName(),
							pxy.GetConfigurer().GetBaseConfig().Type,
							int64(len(udpMsg.Content)),
						)
						continue
					case errors.Is(dgErr, udp.ErrDatagramTooLarge), errors.Is(dgErr, udp.ErrDatagramHeaderTooLong):
						var tooLarge *udp.DatagramTooLargeError
						limit := 0
						if errors.As(dgErr, &tooLarge) {
							limit = tooLarge.Limit
						}
						if pxy.cfg.QUICDatagramStrict {
							if report, suffix := pxy.dgSizeReporter.ShouldReport(); report {
								xl.Warnf("udp datagram lane: dropping a packet of %d bytes that exceeds the %d byte limit of proxy %s in strict mode%s",
									len(udpMsg.Content), limit, pxy.GetName(), suffix)
							}
							continue
						}
						// The packet does not fit the datagram lane: the
						// payload is too large, or a header field (the
						// proxy name or the user address) is longer than
						// the frame can carry. The header case is a
						// property of the proxy, not of the packet, so it
						// must not be treated as a transport failure: the
						// packet goes over the reliable stream instead.
						// The fallback is reported, because a stream lane
						// reintroduces the head-of-line blocking that the
						// datagram lane avoids, and the operator can only
						// see it in the log.
						if report, suffix := pxy.dgSizeReporter.ShouldReport(); report {
							xl.Warnf("udp datagram lane: a packet of %d bytes exceeds the %d byte limit of proxy %s, sending it over the reliable stream instead%s",
								len(udpMsg.Content), limit, pxy.GetName(), suffix)
						}
					default:
						// A terminal datagram send error tears down this
						// work connection: the stale dgMux registration
						// would otherwise keep pointing at a dead sendCh.
						xl.Infof("sender goroutine for udp work connection closed: %v", dgErr)
						_ = payloadConn.Close()
						return
					}
				}
				if errRet = payloadConn.WriteMsg(udpMsg); errRet != nil {
					xl.Infof("sender goroutine for udp work connection closed: %v", errRet)
					_ = payloadConn.Close()
					return
				}
				xl.Tracef("send message to udp workConn, len: %d", len(udpMsg.Content))
				metrics.Server.AddTrafficIn(
					pxy.GetName(),
					pxy.GetConfigurer().GetBaseConfig().Type,
					int64(len(udpMsg.Content)),
				)
				continue
			case <-ctx.Done():
				xl.Infof("sender goroutine for udp work connection closed")
				return
			}
		}
	}

	go func() {
		// Use exponential backoff to avoid fixed sleeps that delay
		// first packet and hinder graceful shutdown.
		backoff := time.Duration(0)
		for {
			select {
			case <-pxy.stopCh:
				return
			case <-time.After(backoff):
			}

			workConn, err := pxy.GetWorkConnFromPool(nil, nil)
			if err != nil {
				if backoff == 0 {
					backoff = 100 * time.Millisecond
				} else {
					backoff *= 2
					if backoff > time.Second {
						backoff = time.Second
					}
				}
				continue
			}
			backoff = 0

			// QUIC datagram lane: mirror of the udpDatagram decision made
			// in GetWorkConnFromPool for this same work connection
			var dgMux *udp.DatagramMux
			if pxy.udpDatagramWanted {
				if qc := netpkg.GetQuicConnFromNetConn(workConn); qc != nil && qc.ConnectionState().SupportsDatagrams.Remote {
					dgMux = udp.DatagramMuxFor(qc)
					dgMux.Register(pxy.GetName(), func(remoteAddr *net.UDPAddr, payload []byte) {
						// The bandwidth limiter can only police (drop)
						// here, not shape: this callback runs on the quic
						// connection's shared receive loop, which must
						// never block.
						if !limit.AllowN(pxy.GetLimiter(), time.Now(), len(payload)) {
							return
						}
						m := udp.NewUDPPacket(payload, nil, remoteAddr)
						_ = liberrors.PanicToError(func() {
							select {
							// The datagram came from frpc, so it belongs
							// on readCh, which ForwardUserConn drains into
							// the user connection. sendCh carries the
							// opposite direction (user -> frpc).
							case pxy.readCh <- m:
								metrics.Server.AddTrafficOut(
									pxy.GetName(),
									pxy.GetConfigurer().GetBaseConfig().Type,
									int64(len(m.Content)),
								)
							case <-pxy.stopCh:
							default:
							}
						})
					})
					xl.Infof("udp proxy relaying via quic datagrams (max frame %d bytes, %d bytes of frame overhead for the proxy name)",
						udp.DatagramFrameBudget, udp.DatagramFrameHeaderOverhead+len(pxy.GetName()))
				}
			}

			var rwc io.ReadWriteCloser = workConn
			if pxy.cfg.Transport.UseEncryption {
				rwc, err = libio.WithEncryption(rwc, pxy.encryptionKey)
				if err != nil {
					xl.Errorf("create encryption stream error: %v", err)
					// Unregister the datagram lane before retrying: a stale
					// callback would otherwise keep pointing at the old sendCh.
					if dgMux != nil {
						dgMux.Unregister(pxy.GetName())
						dgMux = nil
					}
					workConn.Close()
					continue
				}
			}
			if pxy.cfg.Transport.UseCompression {
				rwc = libio.WithCompression(rwc)
			}

			if pxy.GetLimiter() != nil {
				rwc = libio.WrapReadWriteCloser(limit.NewReader(rwc, pxy.GetLimiter()), limit.NewWriter(rwc, pxy.GetLimiter()), rwc.Close)
			}

			// Install the new work connection under the proxy lock. The
			// proxy may have been closed while the connection was being
			// acquired: such a connection belongs to nobody, and keeping
			// it would leak the connection and its goroutines for as long
			// as the client keeps the connection alive. The lock also
			// keeps the connection and the close path from racing.
			pxy.mu.Lock()
			if pxy.isClosed {
				pxy.mu.Unlock()
				if dgMux != nil {
					dgMux.Unregister(pxy.GetName())
				}
				_ = workConn.Close()
				return
			}
			oldWorkConn := pxy.workConn
			pxy.workConn = netpkg.WrapReadWriteCloserToConn(rwc, workConn)
			newWorkConn := pxy.workConn
			pxy.mu.Unlock()
			// Close the old workConn and replace it with the new one.
			if oldWorkConn != nil {
				_ = oldWorkConn.Close()
			}

			// Plain UDP payload follows the negotiated wire protocol for message framing.
			payloadRW, err := msg.NewUDPPacketReadWriter(newWorkConn, pxy.wireProtocol, pxy.udpPacketCodec)
			if err != nil {
				xl.Errorf("create UDP packet read writer: %v", err)
				if dgMux != nil {
					dgMux.Unregister(pxy.GetName())
					dgMux = nil
				}
				newWorkConn.Close()
				continue
			}
			payloadConn := msg.NewConn(newWorkConn, payloadRW)
			ctx, cancel := context.WithCancel(context.Background())
			go workConnReaderFn(payloadConn)
			go workConnSenderFn(payloadConn, dgMux, ctx)
			select {
			case <-pxy.stopCh:
				cancel()
				if dgMux != nil {
					dgMux.Unregister(pxy.GetName())
				}
				return
			case <-pxy.checkCloseCh:
			}
			cancel()
			if dgMux != nil {
				dgMux.Unregister(pxy.GetName())
			}
		}
	}()

	// Read from user connections and send wrapped udp message to sendCh (forwarded by workConn).
	// Client will transfor udp message to local udp service and waiting for response for a while.
	// Response will be wrapped to be forwarded by work connection to server.
	// The channels are not closed here: the proxy's own goroutines send on
	// them, and the stop channel ends them instead.
	go func() {
		udp.ForwardUserConn(udpConn, pxy.readCh, pxy.sendCh, pxy.stopCh, int(pxy.serverCfg.UDPPacketSize))
		pxy.Close()
	}()
}

func (pxy *UDPProxy) Close() {
	pxy.mu.Lock()
	defer pxy.mu.Unlock()
	if pxy.isClosed {
		return
	}
	pxy.isClosed = true

	pxy.BaseProxy.Close()
	if pxy.workConn != nil {
		pxy.workConn.Close()
	}
	if pxy.udpConn != nil {
		pxy.udpConn.Close()
	}

	// The channels are not closed here. The proxy's own goroutines send on
	// readCh and on checkCloseCh, and a send on a channel that is closed at
	// the same time is a data race and panics even when the send is in a
	// select with a default branch. The stop channel ends those goroutines
	// instead, and the channels are collected with the proxy.
	if pxy.stopCh != nil {
		close(pxy.stopCh)
	}

	// Release the port exactly once. The proxy's own teardown goroutine
	// calls Close again after the control retired the proxy, and a second
	// release would free a port that another proxy may have acquired in the
	// meantime.
	if pxy.realBindPort > 0 {
		pxy.rc.UDPPortManager.Release(pxy.realBindPort)
	}
}
