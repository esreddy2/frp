package features

import (
	"fmt"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/httpserver"
	"github.com/fatedier/frp/test/e2e/mock/server/streamserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

var _ = ginkgo.Describe("[Feature: Group Advanced]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("Load Balancing with multiple groups", func() {
		ginkgo.It("TCP with different group keys", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			fooPort := f.AllocPort()
			fooServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(fooPort), streamserver.WithRespContent([]byte("foo")))
			f.RunServer("", fooServer)

			barPort := f.AllocPort()
			barServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(barPort), streamserver.WithRespContent([]byte("bar")))
			f.RunServer("", barServer)

			bazPort := f.AllocPort()
			bazServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(bazPort), streamserver.WithRespContent([]byte("baz")))
			f.RunServer("", bazServer)

			remotePort1 := f.AllocPort()
			remotePort2 := f.AllocPort()

			clientConf += fmt.Sprintf(`
			[foo]
			type = tcp
			local_port = %d
			remote_port = %d
			group = test1
			group_key = 123

			[bar]
			type = tcp
			local_port = %d
			remote_port = %d
			group = test1
			group_key = 123

			[baz]
			type = tcp
			local_port = %d
			remote_port = %d
			group = test2
			group_key = 456
			`, fooPort, remotePort1, barPort, remotePort1, bazPort, remotePort2)

			f.RunProcesses(serverConf, []string{clientConf})

			// Group 1 should only get foo and bar
			fooCount := 0
			barCount := 0
			for range 10 {
				framework.NewRequestExpect(f).Explain("group1").Port(remotePort1).Ensure(func(resp *request.Response) bool {
					switch string(resp.Content) {
					case "foo":
						fooCount++
					case "bar":
						barCount++
					default:
						return false
					}
					return true
				})
			}

			framework.ExpectTrue(fooCount > 1 && barCount > 1, "fooCount: %d, barCount: %d", fooCount, barCount)

			// Group 2 should only get baz
			for range 5 {
				framework.NewRequestExpect(f).Explain("group2").Port(remotePort2).ExpectResp([]byte("baz")).Ensure()
			}
		})

		ginkgo.It("TCP with health check and group key", func() {
			serverConf := consts.LegacyDefaultServerConfig
			clientConf := consts.LegacyDefaultClientConfig

			fooPort := f.AllocPort()
			fooServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(fooPort), streamserver.WithRespContent([]byte("foo")))
			f.RunServer("", fooServer)

			barPort := f.AllocPort()
			barServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(barPort), streamserver.WithRespContent([]byte("bar")))
			f.RunServer("", barServer)

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[foo]
			type = tcp
			local_port = %d
			remote_port = %d
			group = test
			group_key = 123
			health_check_type = tcp
			health_check_interval_s = 1

			[bar]
			type = tcp
			local_port = %d
			remote_port = %d
			group = test
			group_key = 123
			health_check_type = tcp
			health_check_interval_s = 1
			`, fooPort, remotePort, barPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			// Check both are ok
			results := []string{}
			for range 10 {
				framework.NewRequestExpect(f).Port(remotePort).Ensure(func(resp *request.Response) bool {
					results = append(results, string(resp.Content))
					return true
				})
			}
			framework.ExpectContainElements(results, []string{"foo", "bar"})

			// Close bar server, check only foo is returned
			barServer.Close()
			time.Sleep(2 * time.Second)
			results = make([]string, 0, 10)
			for range 10 {
				framework.NewRequestExpect(f).Port(remotePort).ExpectResp([]byte("foo")).Ensure()
				results = append(results, "foo")
			}
			framework.ExpectNotContainElements(results, []string{"bar"})

			// Resume bar server, check both are returned
			f.RunServer("", barServer)
			time.Sleep(2 * time.Second)
			results = []string{}
			for range 10 {
				framework.NewRequestExpect(f).Port(remotePort).Ensure(func(resp *request.Response) bool {
					results = append(results, string(resp.Content))
					return true
				})
			}
			framework.ExpectContainElements(results, []string{"foo", "bar"})
		})

		ginkgo.It("HTTP load balancing with group", func() {
			vhostPort := f.AllocPort()
			serverConf := consts.LegacyDefaultServerConfig + fmt.Sprintf(`
			vhost_http_port = %d
			`, vhostPort)
			clientConf := consts.LegacyDefaultClientConfig

			fooPort := f.AllocPort()
			f.RunServer("", httpserver.New(
				httpserver.WithBindPort(fooPort),
				httpserver.WithHandler(framework.SpecifiedHTTPBodyHandler([]byte("foo"))),
			))

			barPort := f.AllocPort()
			f.RunServer("", httpserver.New(
				httpserver.WithBindPort(barPort),
				httpserver.WithHandler(framework.SpecifiedHTTPBodyHandler([]byte("bar"))),
			))

			clientConf += fmt.Sprintf(`
			[foo]
			type = http
			local_port = %d
			custom_domains = example.com
			group = test
			group_key = 123

			[bar]
			type = http
			local_port = %d
			custom_domains = example.com
			group = test
			group_key = 123
			`, fooPort, barPort)

			f.RunProcesses(serverConf, []string{clientConf})

			var mu sync.Mutex
			results := []string{}
			var wait sync.WaitGroup
			for range 10 {
				wait.Go(func() {
					framework.NewRequestExpect(f).Port(vhostPort).
						RequestModify(func(r *request.Request) {
							r.HTTP().HTTPHost("example.com")
						}).
						Ensure(func(resp *request.Response) bool {
							mu.Lock()
							defer mu.Unlock()
							results = append(results, string(resp.Content))
							return true
						})
				})
			}
			wait.Wait()

			framework.ExpectContainElements(results, []string{"foo", "bar"})
		})
	})
})
