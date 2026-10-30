package features

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"html"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	pp "github.com/pires/go-proxyproto"

	"github.com/fatedier/frp/pkg/transport"
	"github.com/fatedier/frp/pkg/util/log"
	"github.com/fatedier/frp/test/e2e/framework"
	"github.com/fatedier/frp/test/e2e/framework/consts"
	"github.com/fatedier/frp/test/e2e/mock/server/httpserver"
	"github.com/fatedier/frp/test/e2e/mock/server/streamserver"
	"github.com/fatedier/frp/test/e2e/pkg/request"
	"github.com/fatedier/frp/test/e2e/pkg/rpc"
)

var _ = ginkgo.Describe("[Feature: Real IP]", func() {
	f := framework.NewDefaultFramework()

	ginkgo.Describe("HTTP X-forwarded-For", func() {
		ginkgo.It("Client Without Header", func() {
			vhostHTTPPort := f.AllocPort()
			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPPort = %d
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

			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
		[[proxies]]
		name = "test"
		type = "http"
		localPort = %d
		customDomains = ["normal.example.com"]
		`, localPort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(vhostHTTPPort).
				RequestModify(func(r *request.Request) {
					r.HTTP().HTTPHost("normal.example.com")
				}).
				ExpectResp([]byte("127.0.0.1")).
				Ensure()
		})

		ginkgo.It("Client With Header", func() {
			vhostHTTPPort := f.AllocPort()
			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPPort = %d
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

			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
		[[proxies]]
		name = "test"
		type = "http"
		localPort = %d
		customDomains = ["normal.example.com"]
		`, localPort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(vhostHTTPPort).
				RequestModify(func(r *request.Request) {
					r.HTTP().HTTPHost("normal.example.com")
					r.HTTP().HTTPHeaders(map[string]string{"x-forwarded-for": "2.2.2.2"})
				}).
				ExpectResp([]byte("2.2.2.2, 127.0.0.1")).
				Ensure()
		})

		ginkgo.It("http2https plugin", func() {
			vhostHTTPPort := f.AllocPort()
			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPPort = %d
		`, vhostHTTPPort)

			localPort := f.AllocPort()

			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
		[[proxies]]
		name = "test"
		type = "http"
		customDomains = ["normal.example.com"]
		[proxies.plugin]
		type = "http2https"
		localAddr = "127.0.0.1:%d"
		`, localPort)

			f.RunProcesses(serverConf, []string{clientConf})

			tlsConfig, err := transport.NewServerTLSConfig("", "", "")
			framework.ExpectNoError(err)

			localServer := httpserver.New(
				httpserver.WithBindPort(localPort),
				httpserver.WithHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					_, _ = w.Write([]byte(html.EscapeString(req.Header.Get("X-Forwarded-For"))))
				})),
				httpserver.WithTLSConfig(tlsConfig),
			)
			f.RunServer("", localServer)

			framework.NewRequestExpect(f).Port(vhostHTTPPort).
				RequestModify(func(r *request.Request) {
					r.HTTP().HTTPHost("normal.example.com")
					r.HTTP().HTTPHeaders(map[string]string{"x-forwarded-for": "2.2.2.2, 3.3.3.3"})
				}).
				ExpectResp([]byte("2.2.2.2, 3.3.3.3, 127.0.0.1")).
				Ensure()
		})

		ginkgo.It("https2http plugin", func() {
			vhostHTTPSPort := f.AllocPort()
			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPSPort = %d
		`, vhostHTTPSPort)

			localPort := f.AllocPort()

			clientConf := consts.DefaultClientConfig
			clientConf += fmt.Sprintf(`
		[[proxies]]
		name = "test"
		type = "https"
		customDomains = ["normal.example.com"]
		[proxies.plugin]
		type = "https2http"
		localAddr = "127.0.0.1:%d"
		`, localPort)

			f.RunProcesses(serverConf, []string{clientConf})

			localServer := httpserver.New(
				httpserver.WithBindPort(localPort),
				httpserver.WithHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					_, _ = w.Write([]byte(html.EscapeString(req.Header.Get("X-Forwarded-For"))))
				})),
			)
			f.RunServer("", localServer)

			framework.NewRequestExpect(f).Port(vhostHTTPSPort).
				RequestModify(func(r *request.Request) {
					r.HTTPS().HTTPHost("normal.example.com").
						HTTPHeaders(map[string]string{"x-forwarded-for": "2.2.2.2"}).
						TLSConfig(&tls.Config{ServerName: "normal.example.com", InsecureSkipVerify: true})
				}).
				ExpectResp([]byte("2.2.2.2, 127.0.0.1")).
				Ensure()
		})
	})

	ginkgo.Describe("Proxy Protocol", func() {
		ginkgo.It("TCP", func() {
			serverConf := consts.DefaultServerConfig
			clientConf := consts.DefaultClientConfig

			localPort := f.AllocPort()
			localServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(localPort),
				streamserver.WithCustomHandler(func(c net.Conn) {
					defer c.Close()
					rd := bufio.NewReader(c)
					ppHeader, err := pp.Read(rd)
					if err != nil {
						log.Errorf("read proxy protocol error: %v", err)
						return
					}

					for {
						if _, err := rpc.ReadBytes(rd); err != nil {
							return
						}

						buf := []byte(ppHeader.SourceAddr.String())
						_, _ = rpc.WriteBytes(c, buf)
					}
				}))
			f.RunServer("", localServer)

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "tcp"
			type = "tcp"
			localPort = %d
			remotePort = %d
			transport.proxyProtocolVersion = "v2"
			`, localPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(remotePort).Ensure(func(resp *request.Response) bool {
				log.Tracef("proxy protocol get SourceAddr: %s", string(resp.Content))
				addr, err := net.ResolveTCPAddr("tcp", string(resp.Content))
				if err != nil {
					return false
				}
				if addr.IP.String() != "127.0.0.1" {
					return false
				}
				return true
			})
		})

		ginkgo.It("UDP", func() {
			serverConf := consts.DefaultServerConfig
			clientConf := consts.DefaultClientConfig

			localPort := f.AllocPort()
			localServer := streamserver.New(streamserver.UDP, streamserver.WithBindPort(localPort),
				streamserver.WithCustomHandler(func(c net.Conn) {
					defer c.Close()
					rd := bufio.NewReader(c)
					ppHeader, err := pp.Read(rd)
					if err != nil {
						log.Errorf("read proxy protocol error: %v", err)
						return
					}

					// Read the actual UDP content after proxy protocol header
					if _, err := rpc.ReadBytes(rd); err != nil {
						return
					}

					buf := []byte(ppHeader.SourceAddr.String())
					_, _ = rpc.WriteBytes(c, buf)
				}))
			f.RunServer("", localServer)

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "udp"
			type = "udp"
			localPort = %d
			remotePort = %d
			transport.proxyProtocolVersion = "v2"
			`, localPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Protocol("udp").Port(remotePort).Ensure(func(resp *request.Response) bool {
				log.Tracef("udp proxy protocol get SourceAddr: %s", string(resp.Content))
				addr, err := net.ResolveUDPAddr("udp", string(resp.Content))
				if err != nil {
					return false
				}
				if addr.IP.String() != "127.0.0.1" {
					return false
				}
				return true
			})
		})

		ginkgo.It("UDP sends the header only once per flow", func() {
			serverConf := consts.DefaultServerConfig
			clientConf := consts.DefaultClientConfig

			// The local service echoes every datagram back verbatim, so a
			// proxy protocol header on a packet shows up in the reply.
			localPort := f.AllocPort()
			echoConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: localPort})
			framework.ExpectNoError(err)
			defer echoConn.Close()
			go func() {
				buf := make([]byte, 2048)
				for {
					n, addr, readErr := echoConn.ReadFromUDP(buf)
					if readErr != nil {
						return
					}
					_, _ = echoConn.WriteToUDP(buf[:n], addr)
				}
			}()

			remotePort := f.AllocPort()
			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "udp"
			type = "udp"
			localPort = %d
			remotePort = %d
			transport.proxyProtocolVersion = "v2"
			`, localPort, remotePort)

			f.RunProcesses(serverConf, []string{clientConf})

			// Both packets belong to the same flow: they come from the same
			// socket, so the user address is the same for both of them.
			conn, err := net.Dial("udp", net.JoinHostPort("127.0.0.1", strconv.Itoa(remotePort)))
			framework.ExpectNoError(err)
			defer conn.Close()
			framework.ExpectNoError(conn.SetDeadline(time.Now().Add(10 * time.Second)))

			buf := make([]byte, 2048)
			_, err = conn.Write([]byte("first"))
			framework.ExpectNoError(err)
			n, err := conn.Read(buf)
			framework.ExpectNoError(err)
			framework.ExpectTrue(bytes.Contains(buf[:n], []byte("first")),
				"the first packet of a flow must reach the service")

			// The proxy protocol header describes the start of a flow, so
			// only the first packet carries it.
			_, err = conn.Write([]byte("second"))
			framework.ExpectNoError(err)
			n, err = conn.Read(buf)
			framework.ExpectNoError(err)
			framework.ExpectEqualValues("second", string(buf[:n]),
				"only the first packet of a flow may carry the proxy protocol header")
		})

		ginkgo.It("HTTP", func() {
			vhostHTTPPort := f.AllocPort()
			serverConf := consts.DefaultServerConfig + fmt.Sprintf(`
		vhostHTTPPort = %d
		`, vhostHTTPPort)

			clientConf := consts.DefaultClientConfig

			localPort := f.AllocPort()
			var srcAddrRecord string
			localServer := streamserver.New(streamserver.TCP, streamserver.WithBindPort(localPort),
				streamserver.WithCustomHandler(func(c net.Conn) {
					defer c.Close()
					rd := bufio.NewReader(c)
					ppHeader, err := pp.Read(rd)
					if err != nil {
						log.Errorf("read proxy protocol error: %v", err)
						return
					}
					srcAddrRecord = ppHeader.SourceAddr.String()
				}))
			f.RunServer("", localServer)

			clientConf += fmt.Sprintf(`
			[[proxies]]
			name = "test"
			type = "http"
			localPort = %d
			customDomains = ["normal.example.com"]
			transport.proxyProtocolVersion = "v2"
			`, localPort)

			f.RunProcesses(serverConf, []string{clientConf})

			framework.NewRequestExpect(f).Port(vhostHTTPPort).RequestModify(func(r *request.Request) {
				r.HTTP().HTTPHost("normal.example.com")
			}).Ensure(framework.ExpectResponseCode(404))

			log.Tracef("proxy protocol get SourceAddr: %s", srcAddrRecord)
			addr, err := net.ResolveTCPAddr("tcp", srcAddrRecord)
			framework.ExpectNoError(err, srcAddrRecord)
			framework.ExpectEqualValues("127.0.0.1", addr.IP.String())
		})
	})
})
