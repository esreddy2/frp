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

package validation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/config/types"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/naming"
)

func TestValidateDomainConfigForServerRejectsSubdomainHostCaseInsensitively(t *testing.T) {
	tests := []struct {
		name          string
		subDomainHost string
		customDomain  string
		wantErr       bool
	}{
		{
			name:          "lowercase subdomain",
			subDomainHost: "frp.example.com",
			customDomain:  "victim.frp.example.com",
			wantErr:       true,
		},
		{
			name:          "mixed case custom domain",
			subDomainHost: "frp.example.com",
			customDomain:  "victim.FRP.example.com",
			wantErr:       true,
		},
		{
			name:          "mixed case wildcard domain",
			subDomainHost: "frp.example.com",
			customDomain:  "*.FRP.example.com",
			wantErr:       true,
		},
		{
			name:          "mixed case subdomain host",
			subDomainHost: "FRP.Example.Com",
			customDomain:  "victim.frp.example.com",
			wantErr:       true,
		},
		{
			name:          "external domain",
			subDomainHost: "frp.example.com",
			customDomain:  "victim.example.net",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDomainConfigForServer(
				&v1.DomainConfig{CustomDomains: []string{tt.customDomain}},
				&v1.ServerConfig{SubDomainHost: tt.subDomainHost},
			)
			if tt.wantErr {
				require.ErrorContains(t, err, "should not belong to subdomain host")
				return
			}
			require.NoError(t, err)
		})
	}
}

// The quic datagram lane identifies a proxy by its wire-level name, and the
// identifier field of a datagram frame holds a single length byte. The
// wire-level name is the proxy name with the user prefix, so the limit
// applies to the effective name on the wire, not to the local
// configuration name.
func TestValidateUDPProxyConfigForClientChecksTheWireLevelName(t *testing.T) {
	const user = "dg-user"

	tests := []struct {
		name          string
		user          string
		proxyName     string
		quicDatagrams bool
		wantErr       bool
	}{
		{
			name:          "the wire name fits",
			user:          user,
			proxyName:     strings.Repeat("n", naming.MaxDatagramIdentifierLength-len(user)-1),
			quicDatagrams: true,
		},
		{
			name:          "the wire name is one byte too long",
			user:          user,
			proxyName:     strings.Repeat("n", naming.MaxDatagramIdentifierLength-len(user)),
			quicDatagrams: true,
			wantErr:       true,
		},
		{
			name:          "the proxy name alone is too long",
			proxyName:     strings.Repeat("n", naming.MaxDatagramIdentifierLength+1),
			quicDatagrams: true,
			wantErr:       true,
		},
		{
			name:      "the lane is not requested, so the name does not matter",
			proxyName: strings.Repeat("n", naming.MaxDatagramIdentifierLength+1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &v1.UDPProxyConfig{
				ProxyBaseConfig: v1.ProxyBaseConfig{
					Name: tt.proxyName,
					Type: string(v1.ProxyTypeUDP),
				},
				QUICDatagrams: tt.quicDatagrams,
			}
			err := validateUDPProxyConfigForClient(cfg, tt.user)
			if tt.wantErr {
				require.ErrorContains(t, err, "the quic datagram lane needs a wire-level proxy name")
				return
			}
			require.NoError(t, err)
		})
	}
}

// The client validation reports the rule for the whole proxy config, so the
// datagram rule is reachable through the exported entry point as well.
func TestValidateProxyConfigurerForClientChecksTheDatagramName(t *testing.T) {
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: strings.Repeat("n", naming.MaxDatagramIdentifierLength+1),
			Type: string(v1.ProxyTypeUDP),
			Transport: v1.ProxyTransport{
				BandwidthLimitMode: types.BandwidthLimitModeClient,
			},
		},
		QUICDatagrams: true,
	}
	require.ErrorContains(t, ValidateProxyConfigurerForClient(cfg, ""),
		"the quic datagram lane needs a wire-level proxy name")

	// Without the datagram lane the name is not constrained.
	cfg.QUICDatagrams = false
	require.NoError(t, ValidateProxyConfigurerForClient(cfg, ""))
}

func TestValidateUDPProxyConfigQUICDatagramStrict(t *testing.T) {
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "test-udp",
			Type: string(v1.ProxyTypeUDP),
			Transport: v1.ProxyTransport{
				BandwidthLimitMode: types.BandwidthLimitModeClient,
			},
		},
		QUICDatagrams:      false,
		QUICDatagramStrict: true,
	}
	require.ErrorContains(t, ValidateProxyConfigurerForClient(cfg, ""),
		"quicDatagramStrict requires quicDatagrams to be true")

	cfg.QUICDatagrams = true
	require.NoError(t, ValidateProxyConfigurerForClient(cfg, ""))
}

func TestValidateProxyTransportProxyProtocolMode(t *testing.T) {
	tests := []struct {
		name                 string
		proxyProtocolVersion string
		proxyProtocolMode    string
		wantErr              string
	}{
		{
			name:                 "valid default empty mode with version",
			proxyProtocolVersion: "v2",
			proxyProtocolMode:    "",
		},
		{
			name:                 "valid first-packet mode",
			proxyProtocolVersion: "v2",
			proxyProtocolMode:    "first-packet",
		},
		{
			name:                 "valid per-packet mode",
			proxyProtocolVersion: "v2",
			proxyProtocolMode:    "per-packet",
		},
		{
			name:                 "invalid mode",
			proxyProtocolVersion: "v2",
			proxyProtocolMode:    "every-packet",
			wantErr:              "not support proxy protocol mode",
		},
		{
			name:                 "per-packet mode without version",
			proxyProtocolVersion: "",
			proxyProtocolMode:    "per-packet",
			wantErr:              "transport.proxyProtocolMode requires transport.proxyProtocol to be set to v1 or v2",
		},
		{
			name:                 "first-packet mode without version",
			proxyProtocolVersion: "",
			proxyProtocolMode:    "first-packet",
			wantErr:              "transport.proxyProtocolMode requires transport.proxyProtocol to be set to v1 or v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &v1.ProxyBaseConfig{
				Name: "test-proxy",
				Type: "tcp",
				ProxyBackend: v1.ProxyBackend{
					LocalPort: 80,
				},
				Transport: v1.ProxyTransport{
					BandwidthLimitMode:   types.BandwidthLimitModeClient,
					ProxyProtocolVersion: tt.proxyProtocolVersion,
					ProxyProtocolMode:    tt.proxyProtocolMode,
				},
			}
			err := validateProxyBaseConfigForClient(base)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateProxyTransportProxyProtocolAndSocketMark(t *testing.T) {
	tests := []struct {
		name          string
		proxyProtocol string
		socketMark    int
		wantErr       string
	}{
		{
			name:          "valid tproxy with zero mark",
			proxyProtocol: "tproxy",
			socketMark:    0,
		},
		{
			name:          "valid tproxy with positive mark",
			proxyProtocol: "tproxy",
			socketMark:    100,
		},
		{
			name:          "valid v2 protocol",
			proxyProtocol: "v2",
			socketMark:    0,
		},
		{
			name:          "invalid protocol",
			proxyProtocol: "invalid",
			wantErr:       "not support proxy protocol",
		},
		{
			name:          "invalid negative socket mark",
			proxyProtocol: "tproxy",
			socketMark:    -1,
			wantErr:       "transport.socketMark should be greater than or equal to 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &v1.ProxyBaseConfig{
				Name: "test-proxy",
				Type: "tcp",
				ProxyBackend: v1.ProxyBackend{
					LocalPort: 80,
				},
				Transport: v1.ProxyTransport{
					BandwidthLimitMode: types.BandwidthLimitModeClient,
					ProxyProtocol:      tt.proxyProtocol,
					SocketMark:         tt.socketMark,
				},
			}
			err := validateProxyBaseConfigForClient(base)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestProxyBaseConfigCompleteSocketMarkDefaulting(t *testing.T) {
	// tproxy with no socketMark defaults to 1
	base1 := &v1.ProxyBaseConfig{
		Transport: v1.ProxyTransport{
			ProxyProtocol: "tproxy",
		},
	}
	base1.Complete()
	require.Equal(t, 1, base1.Transport.SocketMark)

	// tproxy with explicit socketMark retains it
	base2 := &v1.ProxyBaseConfig{
		Transport: v1.ProxyTransport{
			ProxyProtocol: "tproxy",
			SocketMark:    42,
		},
	}
	base2.Complete()
	require.Equal(t, 42, base2.Transport.SocketMark)

	// non-tproxy ignores/clears socketMark
	base3 := &v1.ProxyBaseConfig{
		Transport: v1.ProxyTransport{
			ProxyProtocol: "v2",
			SocketMark:    42,
		},
	}
	base3.Complete()
	require.Equal(t, 0, base3.Transport.SocketMark)
	require.Equal(t, "first-packet", base3.Transport.ProxyProtocolMode)

	// proxyProtocolVersion alias is mapped to proxyProtocol
	base4 := &v1.ProxyBaseConfig{
		Transport: v1.ProxyTransport{
			ProxyProtocolVersion: "tproxy",
		},
	}
	base4.Complete()
	require.Equal(t, "tproxy", base4.Transport.ProxyProtocol)
	require.Equal(t, 1, base4.Transport.SocketMark)
}
