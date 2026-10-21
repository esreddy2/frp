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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/util/xlog"
)

// newSUDPProxyForLifecycleTest returns a sudp proxy that is ready to receive
// work connections.
func newSUDPProxyForLifecycleTest(t *testing.T, name string) *SUDPProxy {
	t.Helper()
	require := require.New(t)

	cfg := &v1.SUDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: string(v1.ProxyTypeSUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   "127.0.0.1",
				LocalPort: 9,
			},
		},
	}
	pxy := &SUDPProxy{
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

// A full read channel means the consumer is stalled, not that the reader
// must stop. The reader has to survive the stall: once the consumer catches
// up the flow must continue. A reader that stops instead retires the whole
// work connection, so a temporarily stalled local service turns into a
// broken proxy until the server re-establishes the work connection.
//
// This is the same defect that the udp proxy of this package already
// avoids, see TestReadUDPWorkConnKeepsReadingAfterAFullChannel.
func TestReadSUDPWorkConnKeepsReadingAfterAFullChannel(t *testing.T) {
	rw := newScriptedReadWriter()
	// The channel has one slot, like the read channel of a proxy it has
	// more than one, and the reader sends without waiting. A send
	// therefore succeeds while the slot is free and is dropped while the
	// slot is taken: the state of the channel is decided by the test, not
	// by a race between the reader and the consumer.
	readCh := make(chan *msg.UDPPacket, 1)
	closeCh := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		readSUDPWorkConn(rw, readCh, closeCh, xlog.New())
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
	// instead of stopping the reader.
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
	require.Equal(t, "first", string(pkt.Content))

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
	require.Equal(t, "third", string(pkt.Content))

	select {
	case <-done:
		t.Fatal("the reader returned while the work connection was still usable")
	default:
	}
}

// The channels of a retired work connection are not closed: the goroutines
// of the work connection send on them, and a send on a closed channel
// panics even when it is in a select with a default branch. The panic
// happens in a goroutine that does not always recover it, and closing a
// channel while another goroutine sends on it is a data race that the race
// detector reports.
//
// The stop channel ends the goroutines of the work connection instead.
func TestSUDPProxyCloseLeavesTheFlowChannelsOpen(t *testing.T) {
	require := require.New(t)

	pxy := newSUDPProxyForLifecycleTest(t, "sudp-flow-channels")

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
func TestSUDPProxyRetirementDoesNotRaceWithTheReader(t *testing.T) {
	require := require.New(t)

	localAddr, received := startRecordingService(t)
	pxy := newSUDPProxyForLocalService(t, "sudp-retire-race", localAddr)

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

// newSUDPProxyForLocalService returns a sudp proxy that forwards to the
// given local service and that is ready to receive work connections.
func newSUDPProxyForLocalService(t *testing.T, name string, localAddr *net.UDPAddr) *SUDPProxy {
	t.Helper()
	require := require.New(t)

	cfg := &v1.SUDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: string(v1.ProxyTypeSUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   localAddr.IP.String(),
				LocalPort: localAddr.Port,
			},
		},
	}
	pxy := &SUDPProxy{
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
// and the forwarding goroutines of the connection keep running, and the
// socket towards the local service stays open until the peer goes away.
//
// The window is real: the wrapper checks the proxy phase and only then starts
// the goroutine that handles the work connection, and the wrapper can stop
// the proxy in between.
func TestSUDPProxyDropsAWorkConnThatArrivesAfterClose(t *testing.T) {
	require := require.New(t)

	pxy := newSUDPProxyForLifecycleTest(t, "sudp-closed")

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
// Two work connections for the same proxy can be handled at the same time
// (the client starts a goroutine per work connection), so the replacement
// has to be atomic. A replacement that closes the previous work connection
// and installs the new one in two separate steps leaves a work connection
// behind when the two steps interleave: it is installed after the last
// close, so nobody closes it and its goroutines keep running.
//
// The proxy under test compresses the work connection, which makes the
// setup of a work connection slow enough for the two steps to interleave
// regularly.
func TestSUDPProxyClosesEveryReplacedWorkConn(t *testing.T) {
	require := require.New(t)

	const workConns = 8

	pxy := newSUDPProxyForLifecycleTest(t, "sudp-replaced")
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

// Two work connections for the same proxy can be handled at the same time:
// the client starts a goroutine per work connection. The goroutines of a
// work connection have to use the channels of that work connection, not the
// ones the proxy holds after another work connection replaced them.
//
// The test is meaningful under the race detector: the unsynchronized access
// to the proxy's channel fields is reported as a data race.
func TestSUDPProxyConcurrentWorkConnsUseTheirOwnChannels(t *testing.T) {
	pxy := newSUDPProxyForLifecycleTest(t, "sudp-concurrent")

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
