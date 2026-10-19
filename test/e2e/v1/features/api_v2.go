package features

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/pkg/request"
)

const (
	apiV2User     = "admin"
	apiV2Password = "admin"
)

// v2Envelope is the response envelope used by the fork's v2 dashboard API.
// The legacy /api/* endpoints deliberately do not use it.
type v2Envelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type v2Page struct {
	Total    int               `json:"total"`
	Page     int               `json:"page"`
	PageSize int               `json:"pageSize"`
	Items    []json.RawMessage `json:"items"`
}

type v2ClientInfo struct {
	Key          string `json:"key"`
	User         string `json:"user"`
	WireProtocol string `json:"wireProtocol"`
	Online       bool   `json:"online"`
}

type v2ProxyInfo struct {
	Name string `json:"name"`
	User string `json:"user"`
	Spec struct {
		Type string `json:"type"`
		TCP  *struct {
			RemotePort *int `json:"remotePort"`
		} `json:"tcp"`
	} `json:"spec"`
}

// getV2 performs an authenticated request against the v2 dashboard API and
// decodes the envelope. It returns the HTTP status code and the envelope.
func getV2(method string, port int, path string) (int, *v2Envelope, error) {
	req := request.New().
		HTTP().
		Port(port).
		HTTPParams(method, "", path, nil).
		HTTPAuth(apiV2User, apiV2Password).
		Timeout(2 * time.Second)

	resp, err := req.Do()
	if err != nil {
		return 0, nil, err
	}
	env := &v2Envelope{}
	if err := json.Unmarshal(resp.Content, env); err != nil {
		return resp.Code, nil, err
	}
	return resp.Code, env, nil
}

// waitV2 polls the v2 dashboard API until check accepts the response or the
// timeout expires. It returns the last response it saw.
func waitV2(port int, path string, check func(env *v2Envelope) bool) *v2Envelope {
	deadline := time.Now().Add(10 * time.Second)
	var last *v2Envelope
	for {
		_, env, err := getV2("GET", port, path)
		if err == nil {
			last = env
			if check(env) {
				return env
			}
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
}

var _ = ginkgo.Describe("[Feature: APIV2]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.It("serves the v2 dashboard API for a connected client", func() {
		dashboardPort := f.AllocPort()
		remotePort := f.AllocPort()

		serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		webServer.addr = "127.0.0.1"
		webServer.port = %d
		webServer.user = "%s"
		webServer.password = "%s"
		`, dashboardPort, apiV2User, apiV2Password)

		clientConf := consts.DefaultClientConfig + `
		transport.wireProtocol = "v2"
		` + fmt.Sprintf(`
		[[proxies]]
		name = "tcp"
		type = "tcp"
		localPort = {{ .%s }}
		remotePort = %d
		`, framework.TCPEchoServerPort, remotePort)

		f.RunProcesses(serverConf, []string{clientConf})

		// The server bind port is only known once the config is rendered.
		serverPort := f.PortByName(consts.PortServerName)

		// Push traffic through the proxy so the counters are not empty.
		framework.NewRequestExpect(f).Port(remotePort).Ensure()

		// /api/v2/system/info reports the server configuration and the
		// connected clients.
		infoEnv := waitV2(dashboardPort, "/api/v2/system/info", func(env *v2Envelope) bool {
			if env.Code != 200 {
				return false
			}
			var info struct {
				Version string `json:"version"`
				Config  struct {
					BindPort int `json:"bindPort"`
				} `json:"config"`
				Status struct {
					ClientCounts int64 `json:"clientCounts"`
				} `json:"status"`
			}
			if err := json.Unmarshal(env.Data, &info); err != nil {
				return false
			}
			return info.Config.BindPort == serverPort && info.Status.ClientCounts == 1
		})
		framework.ExpectTrue(infoEnv != nil, "system info envelope")
		framework.ExpectEqual(infoEnv.Code, 200, "system info code")
		framework.ExpectEqual(infoEnv.Msg, "success", "system info message")

		// /api/v2/clients lists the client and the wire protocol it
		// negotiated. The wire protocol only exists in the fork.
		clientsEnv := waitV2(dashboardPort, "/api/v2/clients?page=1&pageSize=50", func(env *v2Envelope) bool {
			if env.Code != 200 {
				return false
			}
			page := &v2Page{}
			if err := json.Unmarshal(env.Data, page); err != nil {
				return false
			}
			if page.Total != 1 || len(page.Items) != 1 {
				return false
			}
			client := &v2ClientInfo{}
			if err := json.Unmarshal(page.Items[0], client); err != nil {
				return false
			}
			return client.WireProtocol == "v2" && client.Online
		})
		framework.ExpectTrue(clientsEnv != nil, "clients envelope")
		framework.ExpectEqual(clientsEnv.Code, 200, "clients code")
		clientsPage := &v2Page{}
		framework.ExpectNoError(json.Unmarshal(clientsEnv.Data, clientsPage))
		framework.ExpectEqual(clientsPage.Total, 1, "clients total")
		client := &v2ClientInfo{}
		framework.ExpectNoError(json.Unmarshal(clientsPage.Items[0], client))
		framework.ExpectEqual(client.WireProtocol, "v2", "client wire protocol")
		framework.ExpectTrue(client.Online, "client online")

		// /api/v2/proxies reports the proxy spec of the connected client.
		proxiesEnv := waitV2(dashboardPort, "/api/v2/proxies?type=tcp&page=1&pageSize=50", func(env *v2Envelope) bool {
			if env.Code != 200 {
				return false
			}
			page := &v2Page{}
			if err := json.Unmarshal(env.Data, page); err != nil {
				return false
			}
			if len(page.Items) != 1 {
				return false
			}
			proxy := &v2ProxyInfo{}
			if err := json.Unmarshal(page.Items[0], proxy); err != nil {
				return false
			}
			return proxy.Name == "tcp" && proxy.Spec.Type == "tcp" &&
				proxy.Spec.TCP != nil && proxy.Spec.TCP.RemotePort != nil &&
				*proxy.Spec.TCP.RemotePort == remotePort
		})
		framework.ExpectTrue(proxiesEnv != nil, "proxies envelope")
		framework.ExpectEqual(proxiesEnv.Code, 200, "proxies code")
		proxiesPage := &v2Page{}
		framework.ExpectNoError(json.Unmarshal(proxiesEnv.Data, proxiesPage))
		framework.ExpectEqual(len(proxiesPage.Items), 1, "proxies items")
		proxy := &v2ProxyInfo{}
		framework.ExpectNoError(json.Unmarshal(proxiesPage.Items[0], proxy))
		framework.ExpectEqual(proxy.Name, "tcp", "proxy name")
		framework.ExpectEqual(proxy.Spec.Type, "tcp", "proxy type")
		framework.ExpectTrue(proxy.Spec.TCP != nil, "proxy tcp spec")
		framework.ExpectEqual(*proxy.Spec.TCP.RemotePort, remotePort, "proxy remote port")

		// An invalid prune type is reported with the v2 error envelope.
		code, pruneEnv, err := getV2("POST", dashboardPort, "/api/v2/system/prune?type=invalid")
		framework.ExpectNoError(err)
		framework.ExpectTrue(pruneEnv != nil, "prune envelope")
		framework.ExpectEqual(code, 400, "prune status code")
		framework.ExpectEqual(pruneEnv.Code, 400, "prune envelope code")

		// The legacy API stays bare: it is not wrapped in the envelope.
		resp, err := request.New().
			HTTP().
			Port(dashboardPort).
			HTTPPath("/api/serverinfo").
			HTTPAuth(apiV2User, apiV2Password).
			Timeout(2 * time.Second).
			Do()
		framework.ExpectNoError(err)
		framework.ExpectEqual(resp.Code, 200, "legacy server info status code")
		legacy := map[string]any{}
		framework.ExpectNoError(json.Unmarshal(resp.Content, &legacy))
		framework.ExpectTrue(legacy["version"] != nil, "legacy server info version")
		_, hasEnvelope := legacy["data"]
		framework.ExpectTrue(!hasEnvelope, "legacy server info must not use the v2 envelope")
	})
})
