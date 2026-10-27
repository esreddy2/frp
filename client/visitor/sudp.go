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

package visitor

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/proto/udp"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
)

type SUDPVisitor struct {
	*BaseVisitor

	checkCloseCh chan struct{}
	// udpConn is the listener of udp packet
	udpConn *net.UDPConn
	readCh  chan *msg.UDPPacket
	sendCh  chan *msg.UDPPacket

	cfg *v1.SUDPVisitorConfig
}

// SUDP Run start listen a udp port
func (sv *SUDPVisitor) Run() (err error) {
	xl := xlog.FromContextSafe(sv.ctx)

	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(sv.cfg.BindAddr, strconv.Itoa(sv.cfg.BindPort)))
	if err != nil {
		return fmt.Errorf("sudp ResolveUDPAddr error: %v", err)
	}

	sv.udpConn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp port %s error: %v", addr.String(), err)
	}

	sv.sendCh = make(chan *msg.UDPPacket, 1024)
	sv.readCh = make(chan *msg.UDPPacket, 1024)

	xl.Infof("sudp start to work, listen on %s", addr)

	go sv.dispatcher()
	go udp.ForwardUserConn(sv.udpConn, sv.readCh, sv.sendCh, sv.checkCloseCh, int(sv.clientCfg.UDPPacketSize))

	return
}

func (sv *SUDPVisitor) dispatcher() {
	xl := xlog.FromContextSafe(sv.ctx)

	var (
		visitorConn net.Conn
		recycleFn   func()
		err         error

		firstPacket *msg.UDPPacket
	)

	for {
		select {
		case firstPacket = <-sv.sendCh:
			if firstPacket == nil {
				xl.Infof("frpc sudp visitor proxy is closed")
				return
			}
		case <-sv.checkCloseCh:
			xl.Infof("frpc sudp visitor proxy is closed")
			return
		}

		visitorConn, recycleFn, err = sv.getNewVisitorConn()
		if err != nil {
			xl.Warnf("newVisitorConn to frps error: %v, try to reconnect", err)
			continue
		}

		// visitorConn always be closed when worker done.
		func() {
			defer recycleFn()
			sv.worker(visitorConn, firstPacket)
		}()

		select {
		case <-sv.checkCloseCh:
			return
		default:
		}
	}
}

func (sv *SUDPVisitor) worker(workConn net.Conn, firstPacket *msg.UDPPacket) {
	xl := xlog.FromContextSafe(sv.ctx)
	xl.Debugf("starting sudp proxy worker")
	payloadRW, err := msg.NewUDPPacketReadWriter(workConn, sv.clientCfg.Transport.WireProtocol, udpPacketCodecFromHelper(sv.helper))
	if err != nil {
		xl.Errorf("create SUDP packet read writer: %v", err)
		_ = workConn.Close()
		return
	}
	payloadConn := msg.NewConn(workConn, payloadRW)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	closeCh := make(chan struct{})

	// udp service -> frpc -> frps -> frpc visitor -> user
	workConnReaderFn := func(payloadConn *msg.Conn) {
		defer func() {
			payloadConn.Close()
			close(closeCh)
			wg.Done()
		}()

		for {
			var (
				rawMsg msg.Message
				errRet error
			)

			// frpc will send heartbeat in workConn to frpc visitor for keeping alive
			_ = payloadConn.SetReadDeadline(time.Now().Add(60 * time.Second))
			if rawMsg, errRet = payloadConn.ReadMsg(); errRet != nil {
				xl.Warnf("read from workconn for user udp conn error: %v", errRet)
				return
			}

			_ = payloadConn.SetReadDeadline(time.Time{})
			switch m := rawMsg.(type) {
			case *msg.Ping:
				xl.Debugf("frpc visitor get ping message from frpc")
				continue
			case *msg.UDPPacket:
				select {
				case sv.readCh <- m:
				case <-sv.checkCloseCh:
					// The visitor is closed: the channel is not read
					// anymore, so the packet is dropped.
					return
				default:
					// The channel is full: the consumer is stalled. The
					// packet is dropped and the reader keeps running, so
					// a user that stops reading for a moment does not
					// break the work connection of the visitor.
					xl.Tracef("visitor readCh is full, drop a udp package")
				}
			}
		}
	}

	// udp service <- frpc <- frps <- frpc visitor <- user
	workConnSenderFn := func(payloadConn *msg.Conn) {
		defer func() {
			payloadConn.Close()
			wg.Done()
		}()

		var errRet error
		if firstPacket != nil {
			if errRet = payloadConn.WriteMsg(firstPacket); errRet != nil {
				xl.Warnf("sender goroutine for udp work connection closed: %v", errRet)
				return
			}
			xl.Tracef("send udp package to workConn, len: %d", len(firstPacket.Content))
		}

		for {
			select {
			case udpMsg, ok := <-sv.sendCh:
				if !ok {
					xl.Infof("sender goroutine for udp work connection closed")
					return
				}

				if errRet = payloadConn.WriteMsg(udpMsg); errRet != nil {
					xl.Warnf("sender goroutine for udp work connection closed: %v", errRet)
					return
				}
				xl.Tracef("send udp package to workConn, len: %d", len(udpMsg.Content))
			case <-closeCh:
				return
			}
		}
	}

	go workConnReaderFn(payloadConn)
	go workConnSenderFn(payloadConn)

	wg.Wait()
	xl.Infof("sudp worker is closed")
}

func (sv *SUDPVisitor) getNewVisitorConn() (net.Conn, func(), error) {
	rawConn, err := sv.dialRawVisitorConn(sv.cfg.GetBaseConfig())
	if err != nil {
		return nil, func() {}, err
	}
	rwc, recycleFn, err := wrapVisitorConn(rawConn, sv.cfg.GetBaseConfig())
	if err != nil {
		rawConn.Close()
		return nil, func() {}, err
	}
	return netpkg.WrapReadWriteCloserToConn(rwc, rawConn), recycleFn, nil
}

func (sv *SUDPVisitor) Close() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	select {
	case <-sv.checkCloseCh:
		return
	default:
		close(sv.checkCloseCh)
	}
	sv.BaseVisitor.Close()
	if sv.udpConn != nil {
		sv.udpConn.Close()
	}
	// The channels are not closed here. The worker of the visitor sends on
	// readCh, and a send on a closed channel panics even when it is in a
	// select with a default branch. That panic happens in a goroutine that
	// does not recover it, so it would take the whole client process down.
	// The stop channel ends the worker instead.
}
