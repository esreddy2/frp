package features

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/httpserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

var _ = ginkgo.Describe("[Feature: Domain More]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.It("Subdomain", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPPort = %d
		subdomainHost = "example.com"
		`, vhostHTTPPort)

		localPort := f.AllocPort()
		localServer := httpserver.New(
			httpserver.WithBindPort(localPort),
			httpserver.WithHandler(framework.SpecifiedHTTPBodyHandler([]byte("ok"))),
		)
		f.RunServer("", localServer)

		clientConf := consts.DefaultClientConfig
		clientConf += fmt.Sprintf(`
		[[proxies]]
		name = "test"
		type = "http"
		localPort = %d
		subdomain = "foo"
		`, localPort)

		f.RunProcesses(serverConf, []string{clientConf})

		framework.NewRequestExpect(f).Port(vhostHTTPPort).
			RequestModify(func(r *request.Request) {
				r.HTTP().HTTPHost("foo.example.com")
			}).
			ExpectResp([]byte("ok")).
			Ensure()
	})
})
