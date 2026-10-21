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

package visitor

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
)

// A full read channel means the consumer is stalled, not that the reader of
// the work connection must stop. The reader has to survive the stall: once
// the consumer catches up the flow must continue.
//
// A reader that stops instead tears the whole worker down: it closes the
// work connection, so the visitor has to reconnect and the flow of the user
// is broken until it does. Upstream blocks in the send instead of
// returning, and the udp proxy of the client drops the packet and keeps
// reading.
func TestSUDPVisitorWorkerKeepsReadingAfterAFullChannel(t *testing.T) {
	require := require.New(t)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)
	t.Cleanup(func() { _ = udpConn.Close() })

	readCh := make(chan *msg.UDPPacket, 1)
	sv := &SUDPVisitor{
		BaseVisitor: &BaseVisitor{
			clientCfg: &v1.ClientCommonConfig{},
			ctx:       context.Background(),
		},
		cfg:          &v1.SUDPVisitorConfig{},
		checkCloseCh: make(chan struct{}),
		udpConn:      udpConn,
		readCh:       readCh,
		sendCh:       make(chan *msg.UDPPacket, 1024),
	}

	// The consumer is stalled: the channel of the flow is full.
	readCh <- &msg.UDPPacket{Content: []byte("stalled")}

	// The work connection of the visitor and the peer that plays the
	// server: it delivers the packets of the user.
	peer, workConn := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })

	// The reader of the worker arms the read deadline of the work
	// connection before every message, so the deadline is the signal that
	// the reader processed the previous message.
	signals := make(chan struct{}, 16)
	workConn = &deadlineSignalConn{Conn: workConn, signals: signals}

	peerRW, err := msg.NewUDPPacketReadWriter(peer, "", "")
	require.NoError(err)
	peerConn := msg.NewConn(peer, peerRW)

	go sv.worker(workConn, nil)

	waitForRead := func(what string) {
		t.Helper()
		select {
		case <-signals:
		case <-time.After(5 * time.Second):
			t.Fatalf("the reader of the visitor %s", what)
		}
	}

	// writePacket delivers a packet of the server to the worker. The pipe
	// is synchronous, so the write only returns when the worker read the
	// packet.
	writePacket := func(payload string) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			done <- peerConn.WriteMsg(&msg.UDPPacket{Content: []byte(payload)})
		}()
		select {
		case err := <-done:
			require.NoError(err)
		case <-time.After(5 * time.Second):
			t.Fatal("the work connection of the visitor was not read")
		}
	}

	waitForRead("did not start")

	// The packet of the stalled consumer is dropped, and the reader keeps
	// reading: it asks for the next message.
	writePacket("first")
	waitForRead("stopped after the channel was full")

	// The consumer catches up, so the flow continues.
	var pkt *msg.UDPPacket
	select {
	case pkt = <-readCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the packet of the stalled consumer was not delivered")
	}
	require.Equal("stalled", string(pkt.Content))

	writePacket("second")

	select {
	case pkt = <-readCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the packet after the dropped one was not delivered")
	}
	require.Equal("second", string(pkt.Content))
}

// deadlineSignalConn reports every read deadline that is armed on a work
// connection. The reader of the visitor arms the deadline before it reads
// the next message, so the signal tells the test that the reader processed
// the previous message.
type deadlineSignalConn struct {
	net.Conn

	signals chan struct{}
}

func (c *deadlineSignalConn) SetReadDeadline(t time.Time) error {
	if !t.IsZero() {
		select {
		case c.signals <- struct{}{}:
		default:
		}
	}
	return c.Conn.SetReadDeadline(t)
}

// The worker of a sudp visitor sends the packets of the work connection on
// the channel of the flow. A send on a closed channel panics even when it is
// in a select with a default branch, and the panic happens in a goroutine
// that does not recover it: it would take the whole client process down.
// The visitor therefore ends its worker with the stop channel instead of
// closing the channel.
func TestSUDPVisitorCloseLeavesTheFlowChannelsOpen(t *testing.T) {
	require := require.New(t)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(err)

	sv := &SUDPVisitor{
		BaseVisitor: &BaseVisitor{
			clientCfg: &v1.ClientCommonConfig{},
			ctx:       context.Background(),
		},
		cfg:          &v1.SUDPVisitorConfig{},
		checkCloseCh: make(chan struct{}),
		udpConn:      udpConn,
		readCh:       make(chan *msg.UDPPacket, 1024),
		sendCh:       make(chan *msg.UDPPacket, 1024),
	}

	// A sender that fills the channel of the flow and blocks on the next
	// send, like the worker of a busy visitor does.
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
			case sv.readCh <- &msg.UDPPacket{Content: []byte("payload")}:
			case <-senderStopped:
				return
			}
		}
	}()
	// Wait until the channel of the flow is full, so that the sender is
	// blocked in a send when the visitor is closed.
	for len(sv.readCh) < cap(sv.readCh) {
		time.Sleep(time.Millisecond)
	}

	sv.Close()
	close(senderStopped)

	select {
	case r := <-senderPanic:
		t.Fatalf("the send on the channel of a retired visitor panicked: %v", r)
	case <-time.After(time.Second):
	}

	// The channel of a retired visitor is not closed, so a late packet of
	// the work connection does not panic.
	require.NotPanics(func() {
		select {
		case sv.readCh <- &msg.UDPPacket{Content: []byte("late")}:
		default:
		}
	})
}
