package features

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/streamserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

var _ = ginkgo.Describe("[Feature: TCPMux Advanced]", func() {
	f := framework.NewDefaultFramework()

	getDefaultServerConf := func(httpconnectPort int) string {
		conf := consts.LegacyDefaultServerConfig + `
		tcpmux_httpconnect_port = %d
		`
		return fmt.Sprintf(conf, httpconnectPort)
	}

	newServer := func(port int, respContent string) *streamserver.Server {
		return streamserver.New(
			streamserver.TCP,
			streamserver.WithBindPort(port),
			streamserver.WithRespContent([]byte(respContent)),
		)
	}

	ginkgo.It("TCPMux with multiple backends and different domains", func() {
		vhostPort := f.AllocPort()
		serverConf := getDefaultServerConf(vhostPort)

		fooPort := f.AllocPort()
		f.RunServer("", newServer(fooPort, "foo"))

		barPort := f.AllocPort()
		f.RunServer("", newServer(barPort, "bar"))

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
			[foo]
			type = tcpmux
			multiplexer = httpconnect
			local_port = %d
			custom_domains = foo.example.com

			[bar]
			type = tcpmux
			multiplexer = httpconnect
			local_port = %d
			custom_domains = bar.example.com
			`, fooPort, barPort)

		f.RunProcesses(serverConf, []string{clientConf})

		// foo
		framework.NewRequestExpect(f).Explain("foo").
			RequestModify(func(r *request.Request) {
				r.Addr("foo.example.com").Proxy(fmt.Sprintf("http://127.0.0.1:%d", vhostPort))
			}).
			ExpectResp([]byte("foo")).
			Ensure()

		// bar
		framework.NewRequestExpect(f).Explain("bar").
			RequestModify(func(r *request.Request) {
				r.Addr("bar.example.com").Proxy(fmt.Sprintf("http://127.0.0.1:%d", vhostPort))
			}).
			ExpectResp([]byte("bar")).
			Ensure()
	})

	ginkgo.It("TCPMux with encryption and compression", func() {
		vhostPort := f.AllocPort()
		serverConf := getDefaultServerConf(vhostPort)

		fooPort := f.AllocPort()
		f.RunServer("", newServer(fooPort, "foo"))

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
			[foo-enc]
			type = tcpmux
			multiplexer = httpconnect
			local_port = %d
			custom_domains = foo.example.com
			use_encryption = true
			use_compression = true
			`, fooPort)

		f.RunProcesses(serverConf, []string{clientConf})

		framework.NewRequestExpect(f).Explain("foo-enc").
			RequestModify(func(r *request.Request) {
				r.Addr("foo.example.com").Proxy(fmt.Sprintf("http://127.0.0.1:%d", vhostPort))
			}).
			ExpectResp([]byte("foo")).
			Ensure()
	})
})
