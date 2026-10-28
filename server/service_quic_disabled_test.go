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

package server

import (
	"bytes"
	"testing"

	goliblog "github.com/fatedier/golib/log"
	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/log"
)

// A server without quicBindPort does not listen for quic at all. A client
// that is configured for the quic transport then fails with a handshake
// timeout, which looks like a broken path, so the server has to say that its
// quic listener is off.
func TestNewServiceReportsThatQUICIsDisabled(t *testing.T) {
	var buf bytes.Buffer
	previous := log.Logger
	log.Logger = previous.WithOptions(goliblog.WithOutput(&buf))
	t.Cleanup(func() { log.Logger = previous })

	cfg := &v1.ServerConfig{
		BindAddr:      "127.0.0.1",
		ProxyBindAddr: "127.0.0.1",
	}
	require.NoError(t, cfg.Complete())
	cfg.BindPort = freeLifecycleTCPPort(t)

	svr, err := NewService(cfg)
	require.NoError(t, err)
	require.NotNil(t, svr)
	defer svr.Close()

	require.Contains(t, buf.String(), "quic is disabled")
	require.Contains(t, buf.String(), "quicBindPort")
}

// A server with quicBindPort set listens for quic, and the log has to say
// where it listens.
func TestNewServiceReportsTheQUICListener(t *testing.T) {
	var buf bytes.Buffer
	previous := log.Logger
	log.Logger = previous.WithOptions(goliblog.WithOutput(&buf))
	t.Cleanup(func() { log.Logger = previous })

	cfg := &v1.ServerConfig{
		BindAddr:      "127.0.0.1",
		ProxyBindAddr: "127.0.0.1",
	}
	require.NoError(t, cfg.Complete())
	cfg.BindPort = freeLifecycleTCPPort(t)
	cfg.QUICBindPort = freeLifecycleUDPPort(t)

	svr, err := NewService(cfg)
	require.NoError(t, err)
	require.NotNil(t, svr)
	defer svr.Close()

	require.Contains(t, buf.String(), "frps quic listen on")
	require.NotContains(t, buf.String(), "quic is disabled")
}
