package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fatedier/frp/pkg/config/source"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
)

// cancelOnReadConn cancels the service context as soon as the client reads
// anything, which simulates a shutdown that races the accepted Login.
type cancelOnReadConn struct {
	net.Conn
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOnReadConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.once.Do(c.cancel)
	}
	return n, err
}

// observedProxyConfigurer records how often the proxy configuration is read,
// which shows whether a control was started.
type observedProxyConfigurer struct {
	*v1.TCPProxyConfig
	baseConfigReads atomic.Int32
}

func (c *observedProxyConfigurer) GetBaseConfig() *v1.ProxyBaseConfig {
	c.baseConfigReads.Add(1)
	return c.TCPProxyConfig.GetBaseConfig()
}

func waitForInstalledControlRunID(t *testing.T, svr *Service, want string) {
	t.Helper()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		svr.ctlMu.RLock()
		ctl := svr.ctl
		got := ""
		if ctl != nil {
			got = ctl.sessionCtx.RunID
		}
		svr.ctlMu.RUnlock()
		if ctl != nil && got == want {
			return
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for installed control RunID %q; last RunID %q", want, got)
		}
	}
}

func TestRunSendsInitialRunIDOnFirstLoginAndReconnect(t *testing.T) {
	firstClientConn, firstServerConn := net.Pipe()
	secondClientConn, secondServerConn := net.Pipe()
	t.Cleanup(func() {
		_ = firstClientConn.Close()
		_ = firstServerConn.Close()
		_ = secondClientConn.Close()
		_ = secondServerConn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secondLoginAccepted := make(chan struct{})
	serverErrCh := make(chan error, 1)
	go func() {
		defer firstServerConn.Close()
		defer secondServerConn.Close()

		rw := msg.NewV1ReadWriter(firstServerConn)
		var loginMsg msg.Login
		if err := rw.ReadMsgInto(&loginMsg); err != nil {
			serverErrCh <- err
			return
		}
		if loginMsg.RunID != "initial-run-id" {
			serverErrCh <- fmt.Errorf("first login RunID = %q, want %q", loginMsg.RunID, "initial-run-id")
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "initial-run-id"}); err != nil {
			serverErrCh <- err
			return
		}
		if err := firstServerConn.Close(); err != nil {
			serverErrCh <- err
			return
		}

		rw = msg.NewV1ReadWriter(secondServerConn)
		loginMsg = msg.Login{}
		if err := rw.ReadMsgInto(&loginMsg); err != nil {
			serverErrCh <- err
			return
		}
		if loginMsg.RunID != "initial-run-id" {
			serverErrCh <- fmt.Errorf("reconnect login RunID = %q, want %q", loginMsg.RunID, "initial-run-id")
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "reconnected-run-id"}); err != nil {
			serverErrCh <- err
			return
		}
		close(secondLoginAccepted)
		<-ctx.Done()
		serverErrCh <- nil
	}()

	var connectorCount atomic.Int32
	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		InitialRunID:           "initial-run-id",
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			switch connectorCount.Add(1) {
			case 1:
				return &testConnector{conn: &trackingConn{Conn: firstClientConn}}
			case 2:
				return &testConnector{conn: &trackingConn{Conn: secondClientConn}}
			default:
				return &failingConnector{err: context.Canceled}
			}
		},
		OnFirstLoginSuccess: func(runID string) error {
			if runID != "initial-run-id" {
				t.Errorf("OnFirstLoginSuccess RunID = %q, want initial-run-id", runID)
			}
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svr.Run(ctx)
	}()

	select {
	case <-secondLoginAccepted:
		waitForInstalledControlRunID(t, svr, "reconnected-run-id")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect login")
	}
	cancel()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("run service: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for service shutdown")
	}
	if err := <-serverErrCh; err != nil {
		t.Fatalf("mock server: %v", err)
	}

	if got := connectorCount.Load(); got != 2 {
		t.Fatalf("connector attempts = %d, want 2", got)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 1 across reconnects", got)
	}
}

func TestServiceOnFirstLoginSuccessFiresOnlyAfterAcceptedLogin(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	trackedConn := &trackingConn{Conn: clientConn}
	connector := &testConnector{conn: trackedConn}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan string, 1)
	serverErr := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		if err := rw.WriteMsg(&msg.LoginResp{RunID: "accepted-run"}); err != nil {
			serverErr <- err
			return
		}
		<-ctx.Done()
		serverErr <- nil
	}()

	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return connector
		},
		OnFirstLoginSuccess: func(runID string) error {
			accepted <- runID
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- svr.Run(ctx) }()

	select {
	case runID := <-accepted:
		if runID != "accepted-run" {
			t.Fatalf("OnFirstLoginSuccess RunID = %q, want accepted-run", runID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for accepted-login callback")
	}
	waitForInstalledControlRunID(t, svr, "accepted-run")
	select {
	case err := <-runErrCh:
		t.Fatalf("Service.Run returned before external cancellation: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("run service: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for service shutdown")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if !trackedConn.closed.Load() {
		t.Fatal("control connection was not closed during Service.Run cleanup")
	}
	if !connector.closed.Load() {
		t.Fatal("connector was not closed during Service.Run cleanup")
	}
}

func TestServiceOnFirstLoginSuccessDoesNotFireOnAuthenticatedLoginRejection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.LoginResp{RunID: "rejected-run", Error: "token rejected"})
	}()

	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return &testConnector{conn: &trackingConn{Conn: clientConn}}
		},
		OnFirstLoginSuccess: func(string) error {
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	err = svr.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want authenticated LoginResp rejection")
	}
	if !strings.Contains(err.Error(), "token rejected") {
		t.Fatalf("Run() error = %q, want the login rejection", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 0", got)
	}
}

func TestServiceOnFirstLoginSuccessErrorRejectsSessionBeforeControl(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	trackedConn := &trackingConn{Conn: clientConn}
	connector := &testConnector{conn: trackedConn}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.LoginResp{RunID: "untrusted-run"})
	}()

	hookCause := errors.New("sensitive hook detail: " + strings.Repeat("x", 8_192))
	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return connector
		},
		OnFirstLoginSuccess: func(runID string) error {
			callbacks.Add(1)
			if runID != "untrusted-run" {
				t.Errorf("OnFirstLoginSuccess RunID = %q, want untrusted-run", runID)
			}
			return hookCause
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- svr.Run(context.Background()) }()

	var runErr error
	select {
	case runErr = <-runErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the hook rejection to stop the service")
	}
	if runErr == nil {
		t.Fatal("Run() error = nil, want hook rejection")
	}
	if !errors.Is(runErr, hookCause) {
		t.Fatal("Run() error does not wrap the original hook cause")
	}
	if !strings.Contains(runErr.Error(), "first login success hook rejected authenticated session") {
		t.Fatalf("Run() error = %q, want bounded hook rejection", runErr)
	}
	if strings.Contains(runErr.Error(), "sensitive hook detail") || len(runErr.Error()) > 256 {
		t.Fatalf("Run() exposed unbounded hook error: length=%d error=%q", len(runErr.Error()), runErr)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("OnFirstLoginSuccess calls = %d, want 1", got)
	}
	if !trackedConn.closed.Load() {
		t.Fatal("authenticated session connection was not closed after hook rejection")
	}
	if !connector.closed.Load() {
		t.Fatal("authenticated session connector was not closed after hook rejection")
	}
	svr.ctlMu.RLock()
	ctl := svr.ctl
	svr.ctlMu.RUnlock()
	if ctl != nil {
		t.Fatal("control was installed after hook rejection")
	}
}

func TestServiceCancellationAfterAcceptedLoginStopsBeforeHookAndControl(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	trackedConn := &trackingConn{Conn: &cancelOnReadConn{Conn: clientConn, cancel: cancel}}
	connector := &testConnector{conn: trackedConn}
	t.Cleanup(func() {
		cancel()
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverErr := make(chan error, 1)
	go func() {
		rw := msg.NewV1ReadWriter(serverConn)
		var login msg.Login
		if err := rw.ReadMsgInto(&login); err != nil {
			serverErr <- err
			return
		}
		serverErr <- rw.WriteMsg(&msg.LoginResp{RunID: "accepted-after-cancel"})
	}()

	proxyCfg := &observedProxyConfigurer{TCPProxyConfig: &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "must-not-start",
			Type: "tcp",
			ProxyBackend: v1.ProxyBackend{
				LocalPort: 10080,
			},
		},
	}}
	configSource := source.NewConfigSource()
	if err := configSource.ReplaceAll([]v1.ProxyConfigurer{proxyCfg}, nil); err != nil {
		t.Fatalf("seed proxy config: %v", err)
	}

	var callbacks atomic.Int32
	svr, err := NewService(ServiceOptions{
		Common:                 &v1.ClientCommonConfig{},
		ConfigSourceAggregator: source.NewAggregator(configSource),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			return connector
		},
		OnFirstLoginSuccess: func(string) error {
			callbacks.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	proxyCfg.baseConfigReads.Store(0)

	if err := svr.Run(ctx); err == nil {
		t.Fatal("Run() error = nil after cancellation during accepted Login")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("OnFirstLoginSuccess calls = %d after cancellation, want 0", got)
	}
	if got := proxyCfg.baseConfigReads.Load(); got != 0 {
		t.Fatalf("proxy config reads after cancellation = %d, want 0 (control was started)", got)
	}
	if !trackedConn.closed.Load() {
		t.Fatal("accepted session connection was not closed after cancellation")
	}
	if !connector.closed.Load() {
		t.Fatal("accepted session connector was not closed after cancellation")
	}
	svr.ctlMu.RLock()
	ctl := svr.ctl
	svr.ctlMu.RUnlock()
	if ctl != nil {
		t.Fatal("control was installed after cancellation")
	}
}
