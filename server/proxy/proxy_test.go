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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/proto/wire"
)

func TestWorkConnStartWritesStartWorkConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverMsgConn := msg.NewConn(server, msg.NewV2ReadWriter(server))
	clientMsgConn := msg.NewConn(client, msg.NewV2ReadWriter(client))
	workConn := NewWorkConn(serverMsgConn)

	in := &msg.StartWorkConn{ProxyName: "tcp", SrcAddr: "127.0.0.1", SrcPort: 1234}
	type startResult struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan startResult, 1)
	go func() {
		conn, err := workConn.Start(in)
		resultCh <- startResult{conn: conn, err: err}
	}()

	out, err := clientMsgConn.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, in, out)

	result := <-resultCh
	require.NoError(t, result.err)
	// The started stream wraps the same underlying connection so that the
	// owning control keeps observing the stream's lifetime.
	stream, ok := result.conn.(*workConnStream)
	require.True(t, ok)
	require.Same(t, serverMsgConn, stream.Conn)
}

// Closing the started stream must close the owning work connection and run
// its close callback exactly once, so that the control can stop tracking it.
func TestWorkConnStartStreamCloseClosesOwnerAndFiresCallback(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverMsgConn := msg.NewConn(server, msg.NewV2ReadWriter(server))
	clientMsgConn := msg.NewConn(client, msg.NewV2ReadWriter(client))
	workConn := NewWorkConn(serverMsgConn)

	var callbacks int
	var callbackMu sync.Mutex
	workConn.SetCloseCallback(func() {
		callbackMu.Lock()
		callbacks++
		callbackMu.Unlock()
	})

	// Drain the start message so that the write inside Start can complete.
	go func() {
		_, _ = clientMsgConn.ReadMsg()
	}()

	conn, err := workConn.Start(&msg.StartWorkConn{ProxyName: "tcp"})
	require.NoError(t, err)

	require.NoError(t, conn.Close())
	callbackMu.Lock()
	require.Equal(t, 1, callbacks)
	callbackMu.Unlock()

	// The underlying connection is closed, so the stream is no longer usable.
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)

	// Repeated closes keep the callback count at one.
	require.NoError(t, conn.Close())
	callbackMu.Lock()
	require.Equal(t, 1, callbacks)
	callbackMu.Unlock()
}

// Interrupt must close the connection even when the transport close handshake
// would otherwise block.
func TestWorkConnInterruptClosesConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverMsgConn := msg.NewConn(server, msg.NewV2ReadWriter(server))
	workConn := NewWorkConn(serverMsgConn)

	closed := make(chan struct{})
	workConn.SetCloseCallback(func() { close(closed) })

	require.NoError(t, workConn.Interrupt())

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("interrupt did not close the work connection")
	}
}

func TestGetWorkConnFromPoolStartWorkConnUnchangedForUDPWireV2(t *testing.T) {
	startMsg := getStartWorkConnFromPool(t, &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "udp", Type: string(v1.ProxyTypeUDP)},
	}, wire.ProtocolV2)

	require.Equal(t, msg.StartWorkConn{ProxyName: "udp"}, startMsg)
}

func TestGetWorkConnFromPoolLeavesRawTCPPayloadUnframed(t *testing.T) {
	startMsg := getStartWorkConnFromPool(t, &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: "tcp", Type: string(v1.ProxyTypeTCP)},
	}, wire.ProtocolV2)

	require.Equal(t, msg.StartWorkConn{ProxyName: "tcp"}, startMsg)
}

func getStartWorkConnFromPool(t *testing.T, cfg v1.ProxyConfigurer, wireProtocol string) msg.StartWorkConn {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})

	serverMsgConn := msg.NewConn(server, msg.NewV2ReadWriter(server))
	clientMsgConn := msg.NewConn(client, msg.NewV2ReadWriter(client))
	pxy := &BaseProxy{
		name:         cfg.GetBaseConfig().Name,
		configurer:   cfg,
		poolCount:    0,
		ctx:          context.Background(),
		wireProtocol: wireProtocol,
		getWorkConnFn: func() (*WorkConn, error) {
			return NewWorkConn(serverMsgConn), nil
		},
	}

	errCh := make(chan error, 1)
	go func() {
		conn, err := pxy.GetWorkConnFromPool(nil, nil)
		if conn != nil {
			conn.Close()
		}
		errCh <- err
	}()

	var startMsg msg.StartWorkConn
	require.NoError(t, clientMsgConn.ReadMsgInto(&startMsg))
	require.NoError(t, <-errCh)
	return startMsg
}
