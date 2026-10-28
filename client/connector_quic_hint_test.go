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

package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// The server only listens for quic when quicBindPort is set, and the client
// always dials quic over udp on the port it is configured with. A client
// that cannot reach a quic server therefore has to name the setting that is
// missing on the server side, because the failure otherwise looks like a
// network problem.
func TestQUICConnectorErrorNamesTheServerSideSetting(t *testing.T) {
	cfg := &v1.ClientCommonConfig{
		// A name that cannot be resolved, so the dial fails without
		// waiting for a handshake timeout.
		ServerAddr: "frp-test-host-that-does-not-exist.invalid",
		ServerPort: 7000,
	}
	cfg.Transport.Protocol = "quic"
	require.NoError(t, cfg.Complete())

	connector := NewConnector(context.Background(), cfg)
	defer connector.Close()

	err := connector.Open()
	require.Error(t, err)
	require.ErrorContains(t, err, "quicBindPort")
	require.ErrorContains(t, err, "7000")
}
