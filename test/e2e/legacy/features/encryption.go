package features

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: Encryption]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("TCP with encryption", func() {
		// frp's link encryption is a fixed XOR cipher; there is no
		// user-selectable cipher. These cases verify that encryption can be
		// toggled per proxy and that traffic still round-trips.
		ginkgo.It("encryption enabled", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			remotePortName := port.GenName("EncTCP")
			clientConf += fmt.Sprintf(`
			[enc]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			use_encryption = true
			`, framework.TCPEchoServerPort, remotePortName)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(remotePortName).Ensure()
		})

		ginkgo.It("encryption with compression combined", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			remotePortName := port.GenName("EncComp")
			clientConf += fmt.Sprintf(`
			[enc-comp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			use_encryption = true
			use_compression = true
			`, framework.TCPEchoServerPort, remotePortName)

			f.RunProcesses(serverConf, []string{clientConf})
			framework.NewRequestExpect(f).PortName(remotePortName).Ensure()
		})
	})

	ginkgo.Describe("UDP with encryption", func() {
		ginkgo.It("udp with encryption", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			remotePortName := port.GenName("UDPEnc")
			clientConf += fmt.Sprintf(`
			[udp-enc]
			type = udp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			use_encryption = true
			`, framework.UDPEchoServerPort, remotePortName)

			f.RunProcesses(serverConf, []string{clientConf})
			framework.NewRequestExpect(f).Protocol("udp").PortName(remotePortName).Ensure()
		})
	})
})
