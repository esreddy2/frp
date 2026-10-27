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
	"net"
	"reflect"
	"strconv"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/proto/udp"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
)

func init() {
	RegisterProxyFactory(reflect.TypeFor[*v1.SUDPProxyConfig](), NewSUDPProxy)
}

type SUDPProxy struct {
	*BaseProxy

	cfg *v1.SUDPProxyConfig

	localAddr *net.UDPAddr

	workConn net.Conn
	readCh   chan *msg.UDPPacket
	sendCh   chan msg.Message
	closeCh  chan struct{}
	closed   bool
}

func NewSUDPProxy(baseProxy *BaseProxy, cfg v1.ProxyConfigurer) Proxy {
	unwrapped, ok := cfg.(*v1.SUDPProxyConfig)
	if !ok {
		return nil
	}
	return &SUDPProxy{
		BaseProxy: baseProxy,
		cfg:       unwrapped,
	}
}

func (pxy *SUDPProxy) Run() (err error) {
	pxy.localAddr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(pxy.cfg.LocalIP, strconv.Itoa(pxy.cfg.LocalPort)))
	if err != nil {
		return
	}
	return
}

// Close retires the proxy. It is called by the owner of the proxy when the
// proxy is stopped, and it is the only place that stops the work connection
// that is currently installed.
func (pxy *SUDPProxy) Close() {
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
func (pxy *SUDPProxy) retireWorkConnLocked() {
	if pxy.workConn != nil {
		_ = pxy.workConn.Close()
		pxy.workConn = nil
	}
	// The channels of the work connection are not closed. The goroutines of
	// the work connection send on them, and a send on a closed channel
	// panics even when it is in a select with a default branch. Closing a
	// channel while another goroutine sends on it is a data race as well:
	// the runtime records a send as a read of the channel and a close as a
	// write of it, so the race detector reports the retirement of the work
	// connection against the reader, the heartbeat and the forwarding
	// goroutine. The stop channel ends those goroutines instead, and the
	// channels are collected with the work connection.
	if pxy.closeCh != nil {
		close(pxy.closeCh)
		pxy.closeCh = nil
	}
	pxy.readCh = nil
	pxy.sendCh = nil
}

// readSUDPWorkConn reads udp packets from the work connection and forwards
// them to readCh until the connection fails or the proxy is closed.
//
// A packet is dropped when readCh is full, because udp is lossy anyway and
// the consumer is stalled. The reader keeps running in that case: stopping
// it retires the whole work connection, so a temporarily stalled local
// service would turn into a broken proxy until the server re-establishes
// the work connection. This mirrors the udp proxy of the same package.
func readSUDPWorkConn(rw msg.ReadWriter, readCh chan<- *msg.UDPPacket, closeCh <-chan struct{}, xl *xlog.Logger) {
	for {
		select {
		case <-closeCh:
			xl.Tracef("frpc sudp proxy is closed")
			return
		default:
		}

		var udpMsg msg.UDPPacket
		if errRet := rw.ReadMsgInto(&udpMsg); errRet != nil {
			xl.Warnf("read from workConn for sudp error: %v", errRet)
			return
		}

		select {
		case readCh <- &udpMsg:
		case <-closeCh:
			return
		default:
			// The channel is full: the consumer is stalled. The packet is
			// dropped and the reader keeps running, like the reader of the
			// udp proxy does.
			xl.Tracef("sudp readCh is full, drop a udp package")
		}
	}
}

func (pxy *SUDPProxy) InWorkConn(conn net.Conn, _ *msg.StartWorkConn) {
	xl := pxy.xl
	xl.Infof("incoming a new work connection for sudp proxy, %s", conn.RemoteAddr().String())

	remote, _, err := pxy.wrapWorkConn(conn, pxy.encryptionKey)
	if err != nil {
		xl.Errorf("wrap work connection: %v", err)
		return
	}

	workConn := netpkg.WrapReadWriteCloserToConn(remote, conn)
	payloadRW, err := msg.NewUDPPacketReadWriter(workConn, pxy.clientCfg.Transport.WireProtocol, pxy.udpPacketCodec)
	if err != nil {
		xl.Errorf("create SUDP packet read writer: %v", err)
		_ = workConn.Close()
		return
	}
	payloadConn := msg.NewConn(workConn, payloadRW)

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
		xl.Warnf("sudp proxy is closed, drop the new work connection")
		_ = workConn.Close()
		return
	}
	readCh := make(chan *msg.UDPPacket, 1024)
	sendCh := make(chan msg.Message, 1024)
	closeCh := make(chan struct{})
	pxy.retireWorkConnLocked()
	pxy.workConn = workConn
	pxy.readCh = readCh
	pxy.sendCh = sendCh
	pxy.closeCh = closeCh
	pxy.mu.Unlock()

	// closeFn is called when one of the goroutines of this work connection
	// stops. It retires the work connection, but only when it is still the
	// one installed on the proxy: a replaced work connection was retired
	// already, and nobody has to close it a second time.
	closeFn := func() {
		pxy.mu.Lock()
		defer pxy.mu.Unlock()
		if pxy.workConn != workConn {
			return
		}
		pxy.retireWorkConnLocked()
	}

	workConnReaderFn := func(payloadConn *msg.Conn, readCh chan *msg.UDPPacket) {
		defer func() {
			if r := recover(); r != nil {
				xl.Infof("reader goroutine for sudp work connection closed: %v", r)
			}
		}()
		defer closeFn()

		readSUDPWorkConn(payloadConn, readCh, closeCh, xl)
	}

	workConnSenderFn := func(payloadConn *msg.Conn, sendCh chan msg.Message) {
		defer func() {
			if r := recover(); r != nil {
				xl.Infof("writer goroutine for sudp work connection closed: %v", r)
			}
		}()
		defer closeFn()

		var errRet error
		for {
			select {
			case rawMsg, ok := <-sendCh:
				if !ok {
					return
				}
				switch m := rawMsg.(type) {
				case *msg.UDPPacket:
					xl.Tracef("frpc send udp package to frpc visitor, [udp local: %v, remote: %v], [tcp work conn local: %v, remote: %v]",
						m.LocalAddr.String(), m.RemoteAddr.String(), payloadConn.LocalAddr().String(), payloadConn.RemoteAddr().String())
				case *msg.Ping:
					xl.Tracef("frpc send ping message to frpc visitor")
				}

				if errRet = payloadConn.WriteMsg(rawMsg); errRet != nil {
					xl.Errorf("sudp work write error: %v", errRet)
					return
				}
			case <-closeCh:
				// The work connection is retired: the channel is not read
				// anymore.
				return
			}
		}
	}

	heartbeatFn := func(sendCh chan msg.Message, closeCh <-chan struct{}) {
		defer func() {
			if r := recover(); r != nil {
				xl.Infof("heartbeat goroutine for sudp work connection closed: %v", r)
			}
		}()
		defer closeFn()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				select {
				case sendCh <- &msg.Ping{}:
				case <-closeCh:
					return
				default:
				}
			case <-closeCh:
				xl.Tracef("frpc sudp proxy is closed")
				return
			}
		}
	}

	go workConnSenderFn(payloadConn, sendCh)
	go workConnReaderFn(payloadConn, readCh)
	go heartbeatFn(sendCh, closeCh)

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
