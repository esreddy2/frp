package basic

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/cert"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: TLS]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("TLS on frpc", func() {
		ginkgo.It("tls_enable with custom certificate", func() {
			generator := &cert.SelfSignedCertGenerator{}
			artifacts, err := generator.Generate("127.0.0.1")
			framework.ExpectNoError(err)
			serverCrtPath := f.WriteTempFile("server.crt", string(artifacts.Cert))
			serverKeyPath := f.WriteTempFile("server.key", string(artifacts.Key))
			caCrtPath := f.WriteTempFile("ca.crt", string(artifacts.CACert))

			serverConf := consts.LegacyDefaultServerConfig + fmt.Sprintf(`
			tls_cert_file = %s
			tls_key_file = %s
			tls_trusted_ca_file = %s
			`, serverCrtPath, serverKeyPath, caCrtPath)

			clientConf := consts.LegacyDefaultClientConfig + fmt.Sprintf(`
			tls_enable = true
			tls_cert_file = %s
			tls_key_file = %s
			tls_trusted_ca_file = %s
			[tcp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, serverCrtPath, serverKeyPath, caCrtPath,
				framework.TCPEchoServerPort, port.GenName("TLSTCP"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("TLSTCP")).Ensure()
		})
	})

	ginkgo.Describe("TLS on frps", func() {
		ginkgo.It("tls_only with custom certificate", func() {
			generator := &cert.SelfSignedCertGenerator{}
			artifacts, err := generator.Generate("127.0.0.1")
			framework.ExpectNoError(err)
			serverCrtPath := f.WriteTempFile("server.crt", string(artifacts.Cert))
			serverKeyPath := f.WriteTempFile("server.key", string(artifacts.Key))
			caCrtPath := f.WriteTempFile("ca.crt", string(artifacts.CACert))

			serverConf := consts.LegacyDefaultServerConfig + fmt.Sprintf(`
			tls_only = true
			tls_cert_file = %s
			tls_key_file = %s
			tls_trusted_ca_file = %s
			`, serverCrtPath, serverKeyPath, caCrtPath)

			clientConf := consts.LegacyDefaultClientConfig + fmt.Sprintf(`
			tls_enable = true
			tls_cert_file = %s
			tls_key_file = %s
			tls_trusted_ca_file = %s
			[tcp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, serverCrtPath, serverKeyPath, caCrtPath,
				framework.TCPEchoServerPort, port.GenName("TLSOnlyTCP"))

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(port.GenName("TLSOnlyTCP")).Ensure()
		})
	})

	ginkgo.Describe("TLS with disable_custom_tls_first_byte", func() {
		supportProtocols := []string{"tcp", "websocket"}
		for _, protocol := range supportProtocols {
			ginkgo.It(fmt.Sprintf("TLS over %s with disable_custom_tls_first_byte=false", strings.ToUpper(protocol)), func() {
				serverConf := consts.LegacyDefaultServerConfig
				if protocol == "websocket" {
					serverConf += fmt.Sprintf(`
					websocket_server = {{ .%s }}
					`, port.GenName("WSPort"))
				}

				clientConf := consts.LegacyDefaultClientConfig + fmt.Sprintf(`
				protocol = %s
				disable_custom_tls_first_byte = false
				`, protocol)
				if protocol == "websocket" {
					clientConf += fmt.Sprintf(`
					websocket_server = {{ .%s }}
					`, port.GenName("WSPort"))
				}
				clientConf += fmt.Sprintf(`
				[%s-tls]
				type = tcp
				local_port = {{ .%s }}
				remote_port = {{ .%s }}
				`, protocol, framework.TCPEchoServerPort, port.GenName(protocol+"TLS"))

				f.RunProcesses(serverConf, []string{clientConf})

				framework.NewRequestExpect(f).PortName(port.GenName(protocol + "TLS")).Ensure()
			})
		}
	})
})
