package features

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/pkg/naming"
	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/port"
)

var _ = ginkgo.Describe("[Feature: QUIC Datagram]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.It("relays a udp proxy over quic datagrams", func() {
		serverConf := consts.DefaultServerConfig
		serverConf += fmt.Sprintf(`
		quicBindPort = {{ .%s }}
		transport.quic.enableDatagrams = true
		`, consts.PortServerName)

		clientConf := consts.DefaultClientConfig
		clientConf += fmt.Sprintf(`
		transport.protocol = "quic"
		transport.quic.enableDatagrams = true

		[[proxies]]
		name = "udp-datagram"
		type = "udp"
		localPort = {{ .%s }}
		remotePort = {{ .%s }}
		quicDatagrams = true
		`, framework.UDPEchoServerPort, port.GenName("QUICDatagramRemotePort"))

		serverProcess, clientProcesses := f.RunProcesses(serverConf, []string{clientConf})

		// A silent fallback to the reliable work-connection stream would
		// still pass the echo check below, so require both ends to report
		// that the datagram lane is actually in use.
		framework.ExpectNoError(serverProcess.WaitForOutput("udp proxy relaying via quic datagrams", 1, 10*time.Second))
		framework.ExpectNoError(clientProcesses[0].WaitForOutput("udp proxy relaying via quic datagrams", 1, 10*time.Second))

		// QUIC datagrams are unreliable and unordered, so retry the echo
		// instead of expecting the first packet to arrive.
		remotePortName := port.GenName("QUICDatagramRemotePort")
		req := framework.NewRequest().UDP().Port(f.PortByName(remotePortName))

		var lastErr error
		received := false
		for range 20 {
			resp, err := req.Do()
			if err == nil && string(resp.Content) == consts.TestString {
				received = true
				break
			}
			lastErr = err
			time.Sleep(200 * time.Millisecond)
		}
		framework.ExpectTrue(received, "udp echo did not arrive over the quic datagram path", lastErr)
	})

	// The datagram lane identifies a proxy by its wire-level name, and the
	// identifier field of a datagram frame holds a single length byte. The
	// wire-level name is the proxy name with the user prefix, so the limit
	// applies to the effective name on the wire.
	ginkgo.It("rejects a udp proxy whose wire name does not fit the datagram identifier", func() {
		serverConf := consts.DefaultServerConfig
		serverConf += fmt.Sprintf(`
		quicBindPort = {{ .%s }}
		transport.quic.enableDatagrams = true
		`, consts.PortServerName)

		user := "dg-user"
		// The wire-level name is the user, a dot and the proxy name.
		proxyName := strings.Repeat("n", naming.MaxDatagramIdentifierLength-len(user))

		clientConf := consts.DefaultClientConfig
		clientConf += fmt.Sprintf(`
		transport.protocol = "quic"
		transport.quic.enableDatagrams = true
		user = "%s"

		[[proxies]]
		name = "%s"
		type = "udp"
		localPort = {{ .%s }}
		remotePort = {{ .%s }}
		quicDatagrams = true
		`, user, proxyName, framework.UDPEchoServerPort, port.GenName("QUICDatagramLongNameRemotePort"))

		_, clientProcesses := f.RunProcesses(serverConf, []string{clientConf})

		// The client has to report the config error instead of silently
		// relaying the proxy over the reliable work-connection stream.
		framework.ExpectNoError(clientProcesses[0].WaitForOutput(
			"the quic datagram lane needs a wire-level proxy name", 1, 10*time.Second))
	})
})
