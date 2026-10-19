package basic

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: Config Advanced]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("Template", func() {
		ginkgo.It("render by env with multiple values", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			portName := port.GenName("TCP")
			serverConf += fmt.Sprintf(`
			token = {{ %s{{ .Envs.FRP_TOKEN }}%s }}
			`, "`", "`")

			clientConf += fmt.Sprintf(`
			token = {{ %s{{ .Envs.FRP_TOKEN }}%s }}

			[tcp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, "`", "`", framework.TCPEchoServerPort, portName)

			f.SetEnvs([]string{"FRP_TOKEN=123"})
			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(portName).Ensure()
		})

		ginkgo.It("render by env with multiple env vars", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			portName := port.GenName("TCP2")
			serverConf += fmt.Sprintf(`
			token = {{ %s{{ .Envs.FRP_TOKEN }}%s }}
			`, "`", "`")

			clientConf += fmt.Sprintf(`
			token = {{ %s{{ .Envs.FRP_TOKEN }}%s }}

			[tcp]
			type = tcp
			local_port = {{ .%s }}
			remote_port = {{ .%s }}
			`, "`", "`", framework.TCPEchoServerPort, portName)

			f.SetEnvs([]string{"FRP_TOKEN=abc123"})
			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).PortName(portName).Ensure()
		})
	})

	ginkgo.Describe("Includes", func() {
		ginkgo.It("split multiple proxies into different files", func() {
			serverPort := f.AllocPort()
			serverConfigPath := f.GenerateConfigFile(fmt.Sprintf(`
			[common]
			bind_addr = 0.0.0.0
			bind_port = %d
			`, serverPort))

			remotePort := f.AllocPort()
			proxyConfigPath := f.GenerateConfigFile(fmt.Sprintf(`
			[tcp]
			type = tcp
			local_port = %d
			remote_port = %d
			`, f.PortByName(framework.TCPEchoServerPort), remotePort))

			remotePort2 := f.AllocPort()
			proxyConfigPath2 := f.GenerateConfigFile(fmt.Sprintf(`
			[tcp2]
			type = tcp
			local_port = %d
			remote_port = %d
			`, f.PortByName(framework.TCPEchoServerPort), remotePort2))

			remotePort3 := f.AllocPort()
			proxyConfigPath3 := f.GenerateConfigFile(fmt.Sprintf(`
			[tcp3]
			type = tcp
			local_port = %d
			remote_port = %d
			`, f.PortByName(framework.TCPEchoServerPort), remotePort3))

			clientConfigPath := f.GenerateConfigFile(fmt.Sprintf(`
			[common]
			server_port = %d
			includes = %s,%s,%s
			`, serverPort, proxyConfigPath, proxyConfigPath2, proxyConfigPath3))

			_, _, err := f.RunFrps("-c", serverConfigPath)
			framework.ExpectNoError(err)

			_, _, err = f.RunFrpc("-c", clientConfigPath)
			framework.ExpectNoError(err)

			framework.NewRequestExpect(f).Port(remotePort).Ensure()
			framework.NewRequestExpect(f).Port(remotePort2).Ensure()
			framework.NewRequestExpect(f).Port(remotePort3).Ensure()
		})
	})

	ginkgo.Describe("Env Substitution", func() {
		ginkgo.It("use env vars in config", func() {
			serverPort := f.AllocPort()
			serverConfigPath := f.GenerateConfigFile(fmt.Sprintf(`
			[common]
			bind_addr = 0.0.0.0
			bind_port = %d
			token = ${FRP_TOKEN}
			`, serverPort))

			remotePort := f.AllocPort()
			clientConfigPath := f.GenerateConfigFile(fmt.Sprintf(`
			[common]
			server_port = %d
			token = ${FRP_TOKEN}

			[tcp]
			type = tcp
			local_port = %d
			remote_port = %d
			`, serverPort, f.PortByName(framework.TCPEchoServerPort), remotePort))

			f.SetEnvs([]string{"FRP_TOKEN=envtoken"})
			_, _, err := f.RunFrps("-c", serverConfigPath)
			framework.ExpectNoError(err)

			_, _, err = f.RunFrpc("-c", clientConfigPath)
			framework.ExpectNoError(err)

			framework.NewRequestExpect(f).Port(remotePort).Ensure()
		})
	})

	ginkgo.Describe("Config Validation", func() {
		ginkgo.It("frpc config with invalid proxy type", func() {
			path := f.GenerateConfigFile(`
			[common]
			server_addr = 0.0.0.0
			server_port = 7000

			[invalid]
			type = invalidtype
			local_port = 80
			remote_port = 8080
			`)
			_, output, err := f.RunFrpc("verify", "-c", path)
			framework.ExpectNoError(err)
			framework.ExpectTrue(!strings.Contains(output, "syntax is ok"), "output: %s", output)
		})

		ginkgo.It("frpc config with missing required fields", func() {
			path := f.GenerateConfigFile(`
			[common]
			server_addr = 0.0.0.0
			server_port = 7000

			[tcp]
			type = tcp
			`)
			_, output, err := f.RunFrpc("verify", "-c", path)
			framework.ExpectNoError(err)
			framework.ExpectTrue(strings.Contains(output, "syntax is ok"), "output: %s", output)
		})
	})
})
