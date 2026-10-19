package features

import (
	"fmt"
	"net/http"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/httpserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

var _ = ginkgo.Describe("[Feature: HTTP Advanced]", func() {
	f := framework.NewDefaultFramework()

	getDefaultServerConf := func(vhostHTTPPort int) string {
		conf := consts.LegacyDefaultServerConfig + `
		vhost_http_port = %d
		`
		return fmt.Sprintf(conf, vhostHTTPPort)
	}

	newHTTPServer := func(port int, respContent string) *httpserver.Server {
		return httpserver.New(
			httpserver.WithBindPort(port),
			httpserver.WithHandler(framework.SpecifiedHTTPBodyHandler([]byte(respContent))),
		)
	}

	ginkgo.It("HTTP route by locations with multiple paths", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := getDefaultServerConf(vhostHTTPPort)

		fooPort := f.AllocPort()
		f.RunServer("", newHTTPServer(fooPort, "foo"))

		barPort := f.AllocPort()
		f.RunServer("", newHTTPServer(barPort, "bar"))

		bazPort := f.AllocPort()
		f.RunServer("", newHTTPServer(bazPort, "baz"))

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
			[foo]
			type = http
			local_port = %d
			custom_domains = normal.example.com
			locations = /foo,/foo2

			[bar]
			type = http
			local_port = %d
			custom_domains = normal.example.com
			locations = /bar

			[baz]
			type = http
			local_port = %d
			custom_domains = normal.example.com
			locations = /baz,/baz2
			`, fooPort, barPort, bazPort)

		f.RunProcesses(serverConf, []string{clientConf})

		tests := []struct {
			path       string
			expectResp string
			desc       string
		}{
			{path: "/foo", expectResp: "foo", desc: "foo path"},
			{path: "/foo2", expectResp: "foo", desc: "foo2 path"},
			{path: "/bar", expectResp: "bar", desc: "bar path"},
			{path: "/baz", expectResp: "baz", desc: "baz path"},
			{path: "/baz2", expectResp: "baz", desc: "baz2 path"},
			{path: "/other", expectResp: `<!DOCTYPE html>
<html>
<head>
<title>Not Found</title>
<style>
    body {
        width: 35em;
        margin: 0 auto;
        font-family: Tahoma, Verdana, Arial, sans-serif;
    }
</style>
</head>
<body>
<h1>The page you requested was not found.</h1>
<p>Sorry, the page you are looking for is currently unavailable.<br/>
Please try again later.</p>
<p>The server is powered by <a href="https://github.com/fatedier/frp">frp</a>.</p>
<p><em>Faithfully yours, frp.</em></p>
</body>
</html>
`, desc: "other path falls to first"},
		}

		for _, test := range tests {
			framework.NewRequestExpect(f).Explain(test.desc).Port(vhostHTTPPort).
				RequestModify(func(r *request.Request) {
					r.HTTP().HTTPHost("normal.example.com").HTTPPath(test.path)
				}).
				ExpectResp([]byte(test.expectResp)).
				Ensure()
		}
	})

	ginkgo.It("HTTP with multiple custom domains and locations", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := getDefaultServerConf(vhostHTTPPort)

		fooPort := f.AllocPort()
		f.RunServer("", newHTTPServer(fooPort, "foo"))

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
			[multi-domain]
			type = http
			local_port = %d
			custom_domains = a.example.com, b.example.com
			locations = /
			`, fooPort)

		f.RunProcesses(serverConf, []string{clientConf})

		for _, domain := range []string{"a.example.com", "b.example.com"} {
			framework.NewRequestExpect(f).Explain(domain).Port(vhostHTTPPort).
				RequestModify(func(r *request.Request) {
					r.HTTP().HTTPHost(domain)
				}).
				ExpectResp([]byte("foo")).
				Ensure()
		}
	})

	ginkgo.It("HTTP with host header rewrite and custom header", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := getDefaultServerConf(vhostHTTPPort)

		localPort := f.AllocPort()
		f.RunServer("", httpserver.New(
			httpserver.WithBindPort(localPort),
			httpserver.WithHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_, _ = w.Write([]byte(req.Header.Get("X-Custom-Header")))
			})),
		))

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
			[rewrite]
			type = http
			local_port = %d
			custom_domains = normal.example.com
			host_header_rewrite = rewrite.example.com
			header_X-Custom-Header = custom-value
			`, localPort)

		f.RunProcesses(serverConf, []string{clientConf})

		framework.NewRequestExpect(f).Port(vhostHTTPPort).
			RequestModify(func(r *request.Request) {
				r.HTTP().HTTPHost("normal.example.com")
			}).
			ExpectResp([]byte("custom-value")).
			Ensure()
	})
})
