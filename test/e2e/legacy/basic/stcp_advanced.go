package basic

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: STCP Advanced]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.It("STCP with multiple proxies and different SKs", func() {
		serverConf := consts.LegacyDefaultServerConfig

		clientServerConf := consts.LegacyDefaultClientConfig + "\nuser = user1"
		clientVisitorConf := consts.LegacyDefaultClientConfig + "\nuser = user1"

		localPortName := framework.TCPEchoServerPort

		getProxyServerConf := func(proxyName string, sk string, extra string) string {
			return fmt.Sprintf(`
			[%s]
			type = stcp
			role = server
			sk = %s
			local_port = {{ .%s }}
			`+extra, proxyName, sk, localPortName)
		}

		getProxyVisitorConf := func(proxyName string, portName string, sk string, extra string) string {
			return fmt.Sprintf(`
			[%s-visitor]
			type = stcp
			role = visitor
			server_name = %s
			sk = %s
			bind_port = {{ .%s }}
			`+extra, proxyName, proxyName, sk, portName)
		}

		// Proxy foo with SK "abc"
		fooVisitorPort := port.GenName("FooVisitor")
		clientServerConf += getProxyServerConf("foo", "abc", "") + "\n"
		clientVisitorConf += getProxyVisitorConf("foo", fooVisitorPort, "abc", "") + "\n"

		// Proxy bar with SK "def"
		barVisitorPort := port.GenName("BarVisitor")
		clientServerConf += getProxyServerConf("bar", "def", "") + "\n"
		clientVisitorConf += getProxyVisitorConf("bar", barVisitorPort, "def", "") + "\n"

		f.RunProcesses(serverConf, []string{clientServerConf, clientVisitorConf})

		framework.NewRequestExpect(f).PortName(fooVisitorPort).Explain("foo visitor").Ensure()
		framework.NewRequestExpect(f).PortName(barVisitorPort).Explain("bar visitor").Ensure()
	})

	ginkgo.It("STCP with wrong SK should fail", func() {
		serverConf := consts.LegacyDefaultServerConfig

		clientServerConf := consts.LegacyDefaultClientConfig + "\nuser = user1"
		clientVisitorConf := consts.LegacyDefaultClientConfig + "\nuser = user1"

		localPortName := framework.TCPEchoServerPort

		clientServerConf += fmt.Sprintf(`
		[foo]
		type = stcp
		role = server
		sk = abc
		local_port = {{ .%s }}
		`, localPortName)

		wrongVisitorPort := port.GenName("WrongVisitor")
		clientVisitorConf += fmt.Sprintf(`
		[foo-visitor]
		type = stcp
		role = visitor
		server_name = foo
		sk = wrong
		bind_port = {{ .%s }}
		`, wrongVisitorPort)

		f.RunProcesses(serverConf, []string{clientServerConf, clientVisitorConf})

		framework.NewRequestExpect(f).PortName(wrongVisitorPort).Explain("wrong SK").ExpectError(true).Ensure()
	})

	ginkgo.It("STCP with encryption", func() {
		serverConf := consts.LegacyDefaultServerConfig

		clientServerConf := consts.LegacyDefaultClientConfig + "\nuser = user1"
		clientVisitorConf := consts.LegacyDefaultClientConfig + "\nuser = user1"

		localPortName := framework.TCPEchoServerPort

		clientServerConf += fmt.Sprintf(`
		[foo]
		type = stcp
		role = server
		sk = abc
		use_encryption = true
		local_port = {{ .%s }}
		`, localPortName)

		visitorPort := port.GenName("EncVisitor")
		clientVisitorConf += fmt.Sprintf(`
		[foo-visitor]
		type = stcp
		role = visitor
		server_name = foo
		sk = abc
		use_encryption = true
		bind_port = {{ .%s }}
		`, visitorPort)

		f.RunProcesses(serverConf, []string{clientServerConf, clientVisitorConf})

		framework.NewRequestExpect(f).PortName(visitorPort).Explain("encrypted stcp").Ensure()
	})
})
