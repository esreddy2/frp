package features

import (
	"fmt"
	"html"
	"net/http"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/httpserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

var _ = ginkgo.Describe("[Feature: Real IP Advanced]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.It("HTTP X-Forwarded-For with multiple proxies", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := consts.LegacyDefaultServerConfig + fmt.Sprintf(`
		vhost_http_port = %d
		`, vhostHTTPPort)

		localPort := f.AllocPort()
		localServer := httpserver.New(
			httpserver.WithBindPort(localPort),
			httpserver.WithHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte(html.EscapeString(req.Header.Get("X-Forwarded-For"))))
			})),
		)
		f.RunServer("", localServer)

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
		[test]
		type = http
		local_port = %d
		custom_domains = normal.example.com
		`, localPort)

		f.RunProcesses(serverConf, []string{clientConf})

		framework.NewRequestExpect(f).Port(vhostHTTPPort).
			RequestModify(func(r *request.Request) {
				r.HTTP().HTTPHost("normal.example.com")
			}).
			ExpectResp([]byte("127.0.0.1")).
			Ensure()
	})

	ginkgo.It("HTTP X-Real-IP header", func() {
		vhostHTTPPort := f.AllocPort()
		serverConf := consts.LegacyDefaultServerConfig + fmt.Sprintf(`
		vhost_http_port = %d
		`, vhostHTTPPort)

		localPort := f.AllocPort()
		localServer := httpserver.New(
			httpserver.WithBindPort(localPort),
			httpserver.WithHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte(html.EscapeString(req.Header.Get("X-Real-IP"))))
			})),
		)
		f.RunServer("", localServer)

		clientConf := consts.LegacyDefaultClientConfig
		clientConf += fmt.Sprintf(`
		[test]
		type = http
		local_port = %d
		custom_domains = normal.example.com
		`, localPort)

		f.RunProcesses(serverConf, []string{clientConf})

		framework.NewRequestExpect(f).Port(vhostHTTPPort).
			RequestModify(func(r *request.Request) {
				r.HTTP().HTTPHost("normal.example.com")
			}).
			ExpectResp([]byte("")).
			Ensure()
	})
})
