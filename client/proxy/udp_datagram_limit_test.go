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
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/proto/udp"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/xlog"
)

// A packet that does not fit into the datagram lane has to travel over the
// reliable work-connection stream instead: a stream lane reintroduces the
// head-of-line blocking that the datagram lane avoids.
func TestUDPProxyDatagramFallsBackToStreamWhenThePacketIsTooLarge(t *testing.T) {
	require := require.New(t)

	serverConn, clientConn := quicConnPair(t)

	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "dg-oversize",
			Type: string(v1.ProxyTypeUDP),
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   "127.0.0.1",
				LocalPort: 9,
			},
		},
	}
	pxy := &UDPProxy{
		BaseProxy: &BaseProxy{
			baseCfg:   &cfg.ProxyBaseConfig,
			clientCfg: &v1.ClientCommonConfig{UDPPacketSize: 1500},
			xl:        xlog.New(),
			ctx:       context.Background(),
		},
		cfg:            cfg,
		dgSizeReporter: udp.NewSizeReporter(udp.DatagramSizeReportInterval),
	}
	require.NoError(pxy.Run())
	t.Cleanup(pxy.Close)

	stream, err := clientConn.OpenStreamSync(context.Background())
	require.NoError(err)
	pxy.InWorkConn(netpkg.QuicStreamToNetConn(stream, clientConn), &msg.StartWorkConn{UDPDatagram: true})

	remoteAddr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4242}
	limit := udp.MaxDatagramPayloadSize(pxy.datagramName(), remoteAddr)
	payload := make([]byte, limit+1)

	sendCh := waitForProxySendChannel(t, pxy)
	sendCh <- &msg.UDPPacket{Content: payload, RemoteAddr: remoteAddr}

	require.Equal(payload, readWorkConnPacket(t, serverConn),
		"the packet did not fall back to the work-connection stream")
}
