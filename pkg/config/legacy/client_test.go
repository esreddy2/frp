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

package legacy

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	fn()
	require.NoError(t, writer.Close())
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(output)
}

func TestValidateTLSMaterialWarnings(t *testing.T) {
	for _, test := range []struct {
		protocol    string
		wantWarning bool
	}{
		{protocol: "tcp", wantWarning: true},
		{protocol: "kcp", wantWarning: true},
		{protocol: "websocket", wantWarning: true},
		// The wss and quic transports always run over TLS, so their material
		// is applied even while tls_enable is false.
		{protocol: "wss", wantWarning: false},
		{protocol: "quic", wantWarning: false},
	} {
		t.Run(test.protocol, func(t *testing.T) {
			cfg := &ClientCommonConf{
				Protocol:         test.protocol,
				TLSEnable:        false,
				TLSCertFile:      "client.crt",
				TLSKeyFile:       "client.key",
				TLSTrustedCaFile: "ca.crt",
			}
			output := captureStdout(t, func() {
				require.NoError(t, cfg.Validate())
			})
			if test.wantWarning {
				require.Contains(t, output, "tls_cert_file is invalid when tls_enable is false")
				require.Contains(t, output, "tls_key_file is invalid when tls_enable is false")
				require.Contains(t, output, "tls_trusted_ca_file is invalid when tls_enable is false")
			} else {
				require.NotContains(t, output, "invalid when tls_enable is false")
			}
		})
	}
}

func TestIsImplicitTLSProtocol(t *testing.T) {
	require.True(t, isImplicitTLSProtocol("wss"))
	require.True(t, isImplicitTLSProtocol("quic"))
	require.False(t, isImplicitTLSProtocol("tcp"))
	require.False(t, isImplicitTLSProtocol("kcp"))
	require.False(t, isImplicitTLSProtocol("websocket"))
	require.False(t, isImplicitTLSProtocol(""))
}
