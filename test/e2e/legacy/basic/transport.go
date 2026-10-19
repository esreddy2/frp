package basic

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: Transport]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("KCP", func() {
		ginkgo.It("kcp with custom settings", func() {
			serverConf := consts.LegacyDefaultServerConfig
			serverConf += fmt.Sprintf(`
			kcp_bind_port = {{ .%s }}
			`, consts.PortServerName)

			clientConf := consts.LegacyDefaultClientConfig
			clientConf += fmt.Sprintf(`
			protocol = kcp
			[kcp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, framework.TCPEchoServerPort, port.GenName("KCPRemotePort"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("KCPRemotePort")).Ensure()
		})

		ginkgo.It("kcp with encryption", func() {
			serverConf := consts.LegacyDefaultServerConfig
			serverConf += fmt.Sprintf(`
			kcp_bind_port = {{ .%s }}
			`, consts.PortServerName)

			clientConf := consts.LegacyDefaultClientConfig
			clientConf += fmt.Sprintf(`
			protocol = kcp
			use_encryption = true
			[kcp-encrypted]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, framework.TCPEchoServerPort, port.GenName("KCPEncryptedRemotePort"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("KCPEncryptedRemotePort")).Ensure()
		})
	})

	ginkgo.Describe("QUIC", func() {
		ginkgo.It("quic with custom settings", func() {
			serverConf := consts.LegacyDefaultServerConfig
			serverConf += fmt.Sprintf(`
			quic_bind_port = {{ .%s }}
			`, consts.PortServerName)

			clientConf := consts.LegacyDefaultClientConfig
			clientConf += fmt.Sprintf(`
			protocol = quic
			[quic]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, framework.TCPEchoServerPort, port.GenName("QUICRemotePort"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("QUICRemotePort")).Ensure()
		})
	})

	ginkgo.Describe("WebSocket", func() {
		ginkgo.It("websocket with custom path", func() {
			serverConf := consts.LegacyDefaultServerConfig
			serverConf += fmt.Sprintf(`
			websocket_server = {{ .%s }}
			`, port.GenName("WSPort"))

			clientConf := consts.LegacyDefaultClientConfig
			clientConf += fmt.Sprintf(`
			protocol = websocket
			websocket_server = {{ .%s }}
			[ws]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, port.GenName("WSPort"), framework.TCPEchoServerPort, port.GenName("WSRemotePort"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("WSRemotePort")).Ensure()
		})
	})
})
