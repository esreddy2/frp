// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/server/controller"
	servermetrics "github.com/fatedier/frp/server/metrics"
	"github.com/fatedier/frp/server/proxy"
)

type admissionTestPlugin struct {
	handle func(context.Context, string, any) (*plugin.Response, any, error)
}

func (p *admissionTestPlugin) Name() string { return "admission-test" }

func (p *admissionTestPlugin) IsSupport(op string) bool {
	return op == plugin.OpNewProxy || op == plugin.OpNewProxyResult || op == plugin.OpCloseProxy
}

func (p *admissionTestPlugin) Handle(ctx context.Context, op string, content any) (*plugin.Response, any, error) {
	return p.handle(ctx, op, content)
}

// admissionTestProxy is a proxy double that records its own teardown.
type admissionTestProxy struct {
	name       string
	configurer v1.ProxyConfigurer

	mu       sync.Mutex
	closed   bool
	closeCnt int
}

func newAdmissionTestProxy(name string) *admissionTestProxy {
	return &admissionTestProxy{
		name: name,
		configurer: &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: name,
			Type: "tcp",
		}},
	}
}

func (*admissionTestProxy) Context() context.Context { return context.Background() }

func (*admissionTestProxy) Run() (string, error) { return "", nil }

func (p *admissionTestProxy) GetName() string { return p.name }

func (p *admissionTestProxy) GetConfigurer() v1.ProxyConfigurer { return p.configurer }

func (*admissionTestProxy) GetWorkConnFromPool(net.Addr, net.Addr) (net.Conn, error) {
	return nil, nil
}

func (*admissionTestProxy) GetUsedPortsNum() int { return 0 }

func (*admissionTestProxy) GetResourceController() *controller.ResourceController { return nil }

func (*admissionTestProxy) GetUserInfo() plugin.UserInfo { return plugin.UserInfo{} }

func (*admissionTestProxy) GetLimiter() *rate.Limiter { return nil }

func (*admissionTestProxy) GetLoginMsg() *msg.Login { return nil }

func (p *admissionTestProxy) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.closeCnt++
}

func newAdmissionContent(runID string, proxyName string) *plugin.NewProxyContent {
	return &plugin.NewProxyContent{
		User: plugin.UserInfo{User: "alice", RunID: runID},
		NewProxy: msg.NewProxy{
			ProxyName: proxyName,
			ProxyType: "tcp",
		},
	}
}

func TestProcessNewProxyAttemptReportsAdmissionSynchronously(t *testing.T) {
	var order []string
	var preparedAttemptID string
	var result plugin.NewProxyResultContent

	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			order = append(order, "plugin")
			prepared := content.(plugin.NewProxyContent)
			if len(prepared.AttemptID) != 32 {
				t.Fatalf("attempt id = %q, want a 32 character identifier", prepared.AttemptID)
			}
			if _, err := hex.DecodeString(prepared.AttemptID); err != nil {
				t.Fatalf("attempt id = %q, want a hex identifier", prepared.AttemptID)
			}
			preparedAttemptID = prepared.AttemptID
			return &plugin.Response{Unchange: true}, nil, nil
		case plugin.OpNewProxyResult:
			order = append(order, "result")
			result = content.(plugin.NewProxyResultContent)
			return &plugin.Response{Unchange: true}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	content := newAdmissionContent("0123456789abcdef", "proxy-exact")
	effective, remoteAddr, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		content,
		func(got *msg.NewProxy, gotUser plugin.UserInfo, gotAttemptID string) (string, error) {
			order = append(order, "register")
			if got.ProxyName != "proxy-exact" {
				t.Fatalf("register proxy name = %q, want the exact input", got.ProxyName)
			}
			if gotAttemptID != preparedAttemptID {
				t.Fatalf("register attempt id = %q, want %q", gotAttemptID, preparedAttemptID)
			}
			if gotUser.RunID != "0123456789abcdef" {
				t.Fatalf("register run id = %q, want the exact input", gotUser.RunID)
			}
			return "127.0.0.1:8080", nil
		},
	)

	if admissionErr != nil || resultErr != nil {
		t.Fatalf("errors = admission %v, result %v; want nil", admissionErr, resultErr)
	}
	if effective.ProxyName != "proxy-exact" || remoteAddr != "127.0.0.1:8080" {
		t.Fatalf("effective = %+v, remote addr = %q", effective, remoteAddr)
	}
	if result.User.RunID != "0123456789abcdef" || result.AttemptID != preparedAttemptID ||
		result.ProxyName != "proxy-exact" || !result.Admitted {
		t.Fatalf("result = %+v, want the exact admitted identity", result)
	}
	if !slices.Equal(order, []string{"plugin", "register", "result"}) {
		t.Fatalf("order = %v, want [plugin register result]", order)
	}
}

func TestProcessNewProxyAttemptReportsFailedAdmission(t *testing.T) {
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)

	var result plugin.NewProxyResultContent
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			return &plugin.Response{Unchange: true}, nil, nil
		case plugin.OpNewProxyResult:
			result = content.(plugin.NewProxyResultContent)
			return &plugin.Response{Unchange: true}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	admissionErr := errors.New("proxy name already exists")
	_, _, gotAdmissionErr, resultErr := processNewProxyAttempt(
		manager,
		newAdmissionContent("run-1", "proxy-failed"),
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) {
			return "", admissionErr
		},
	)
	if !errors.Is(gotAdmissionErr, admissionErr) {
		t.Fatalf("admission error = %v, want %v", gotAdmissionErr, admissionErr)
	}
	if resultErr != nil {
		t.Fatalf("result error = %v, want nil", resultErr)
	}
	if result.Admitted {
		t.Fatalf("result = %+v, want a not admitted result", result)
	}
	if result.AttemptID == "" || result.ProxyName != "proxy-failed" {
		t.Fatalf("result = %+v, want the failed attempt identity", result)
	}
}

func TestProcessNewProxyAttemptReportsRejectedChainToResultPlugin(t *testing.T) {
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)

	var result plugin.NewProxyResultContent
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		switch op {
		case plugin.OpNewProxy:
			return &plugin.Response{Reject: true, RejectReason: "not allowed"}, nil, nil
		case plugin.OpNewProxyResult:
			result = content.(plugin.NewProxyResultContent)
			return &plugin.Response{Unchange: true}, nil, nil
		default:
			t.Fatalf("unexpected op %q", op)
			return nil, nil, nil
		}
	}})

	registerCalls := 0
	_, _, admissionErr, resultErr := processNewProxyAttempt(
		manager,
		newAdmissionContent("run-3", "proxy-rejected"),
		func(*msg.NewProxy, plugin.UserInfo, string) (string, error) {
			registerCalls++
			return "", nil
		},
	)
	if admissionErr == nil {
		t.Fatal("expected the rejected chain to fail the admission")
	}
	if resultErr != nil {
		t.Fatalf("result error = %v, want nil", resultErr)
	}
	if registerCalls != 0 {
		t.Fatalf("register calls = %d, want 0", registerCalls)
	}
	if result.Admitted {
		t.Fatalf("result = %+v, want a not admitted result", result.Admitted)
	}
	if result.ProxyName != "proxy-rejected" || result.AttemptID == "" {
		t.Fatalf("result = %+v, want the rejected attempt identity", result)
	}
}

func TestConfirmNewProxyResultRejectRollsBackBeforeClientSuccess(t *testing.T) {
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		if op != plugin.OpNewProxyResult {
			t.Fatalf("op = %q, want %q", op, plugin.OpNewProxyResult)
		}
		if got := content.(plugin.NewProxyResultContent); !got.Admitted {
			t.Fatalf("result = %+v, want an admitted result", got)
		}
		return &plugin.Response{Reject: true, RejectReason: "external registration not committed"}, nil, nil
	}})

	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: true},
		nil,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if finalErr == nil || resultErr == nil {
		t.Fatalf("errors = final %v, result %v; want both non-nil", finalErr, resultErr)
	}
	if !strings.Contains(finalErr.Error(), "confirm new proxy admission") ||
		!strings.Contains(resultErr.Error(), "external registration not committed") {
		t.Fatalf("errors = final %q, result %q", finalErr, resultErr)
	}
	if rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", rollbackCalls)
	}
}

func TestConfirmNewProxyResultFailedAdmissionKeepsOriginalError(t *testing.T) {
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(context.Context, string, any) (*plugin.Response, any, error) {
		return nil, nil, errors.New("cleanup callback unavailable")
	}})

	admissionErr := errors.New("proxy name already exists")
	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: false},
		admissionErr,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if !errors.Is(finalErr, admissionErr) || resultErr == nil {
		t.Fatalf("errors = final %v, result %v; want the original admission error and the callback error", finalErr, resultErr)
	}
	if rollbackCalls != 0 {
		t.Fatalf("rollback calls = %d, want 0", rollbackCalls)
	}
}

func TestConfirmNewProxyResultReportsFailedRollback(t *testing.T) {
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(context.Context, string, any) (*plugin.Response, any, error) {
		return &plugin.Response{Reject: true, RejectReason: "not committed"}, nil, nil
	}})

	rollbackErr := errors.New("rollback failed")
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: true},
		nil,
		func() error { return rollbackErr },
	)
	if finalErr == nil || resultErr == nil {
		t.Fatalf("errors = final %v, result %v; want both non-nil", finalErr, resultErr)
	}
	if !errors.Is(finalErr, rollbackErr) {
		t.Fatalf("final error = %v, want the rollback error", finalErr)
	}
}

func TestNewProxyResultDeliveryErrorsStaySanitizedForClientDetailModes(t *testing.T) {
	const sensitiveTransportError = "dial tcp 10.0.0.9:9443: tls certificate secret-plugin.internal"

	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(context.Context, string, any) (*plugin.Response, any, error) {
		return nil, nil, errors.New(sensitiveTransportError)
	}})

	rollbackCalls := 0
	finalErr, resultErr := confirmNewProxyResult(
		manager,
		&plugin.NewProxyResultContent{Admitted: true},
		nil,
		func() error {
			rollbackCalls++
			return nil
		},
	)
	if finalErr == nil || resultErr == nil || rollbackCalls != 1 {
		t.Fatalf("confirmation = final %v result %v rollback %d, want a sanitized failure and one rollback",
			finalErr, resultErr, rollbackCalls)
	}

	for _, detailed := range []bool{true, false} {
		got := util.GenerateResponseErrorString("new proxy error", finalErr, detailed)
		if strings.Contains(got, sensitiveTransportError) || strings.Contains(got, "secret-plugin.internal") {
			t.Fatalf("detailed=%t client error %q leaked raw transport details", detailed, got)
		}
		if detailed && !strings.Contains(got, "result plugin delivery failed") {
			t.Fatalf("detailed client error = %q, want the sanitized delivery reason", got)
		}
		if !detailed && got != "new proxy error" {
			t.Fatalf("non-detailed client error = %q, want the summary", got)
		}
	}
}

// A tentatively registered proxy whose result confirmation fails is retired
// and produces the compensating CloseProxy callback, so a plugin that already
// observed the admitted result can undo its external state.
func TestControlRollbackRegisteredProxyEmitsCompensatingCloseProxy(t *testing.T) {
	delivered := make(chan plugin.CloseProxyContent, 1)
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		if op == plugin.OpCloseProxy {
			delivered <- content.(plugin.CloseProxyContent)
		}
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	pxyManager := proxy.NewManager()
	if err := pxyManager.Add("proxy-1", newAdmissionTestProxy("proxy-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctl := &Control{
		serverMetrics: servermetrics.Server,
		sessionCtx: &SessionContext{
			PxyManager:    pxyManager,
			PluginManager: manager,
			ServerCfg:     &v1.ServerConfig{},
		},
		xl: xlog.FromContextSafe(context.Background()),
	}
	ctl.proxies = make(map[string]registeredProxy)
	ctl.proxies["proxy-1"] = newRegisteredProxy(
		newAdmissionTestProxy("proxy-1"),
		plugin.UserInfo{User: "alice", RunID: "run-1"},
		"attempt-1",
	)

	if err := ctl.CloseProxy(&msg.CloseProxy{ProxyName: "proxy-1"}); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, ok := ctl.proxies["proxy-1"]; ok {
		t.Fatal("expected the rolled back proxy to be removed from the control")
	}
	if pxyManager.Exist("proxy-1") {
		t.Fatal("expected the rolled back proxy to be removed from the proxy manager")
	}
	select {
	case got := <-delivered:
		if got.AttemptID != "attempt-1" {
			t.Fatalf("attempt id = %q, want %q", got.AttemptID, "attempt-1")
		}
		if got.ProxyName != "proxy-1" {
			t.Fatalf("proxy name = %q, want %q", got.ProxyName, "proxy-1")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the compensating CloseProxy notification")
	}
}

func TestControlCloseProxyPublishesAttemptID(t *testing.T) {
	delivered := make(chan plugin.CloseProxyContent, 1)
	manager := plugin.NewManager()
	t.Cleanup(manager.Close)
	manager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		if op == plugin.OpCloseProxy {
			delivered <- content.(plugin.CloseProxyContent)
		}
		return &plugin.Response{Unchange: true}, nil, nil
	}})

	pxyManager := proxy.NewManager()
	if err := pxyManager.Add("proxy-2", newAdmissionTestProxy("proxy-2")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctl := &Control{
		serverMetrics: servermetrics.Server,
		sessionCtx: &SessionContext{
			PxyManager:    pxyManager,
			PluginManager: manager,
			LoginMsg:      &msg.Login{RunID: "run-2", User: "alice"},
			ServerCfg:     &v1.ServerConfig{},
		},
		xl: xlog.FromContextSafe(context.Background()),
	}
	ctl.proxies = make(map[string]registeredProxy)
	ctl.proxies["proxy-2"] = newRegisteredProxy(
		newAdmissionTestProxy("proxy-2"),
		plugin.UserInfo{User: "alice", RunID: "run-2"},
		"attempt-2",
	)

	if err := ctl.CloseProxy(&msg.CloseProxy{ProxyName: "proxy-2"}); err != nil {
		t.Fatalf("close proxy failed: %v", err)
	}
	select {
	case got := <-delivered:
		if got.AttemptID != "attempt-2" {
			t.Fatalf("attempt id = %q, want %q", got.AttemptID, "attempt-2")
		}
		if got.ProxyName != "proxy-2" {
			t.Fatalf("proxy name = %q, want %q", got.ProxyName, "proxy-2")
		}
		if got.User.RunID != "run-2" {
			t.Fatalf("run id = %q, want %q", got.User.RunID, "run-2")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the CloseProxy notification")
	}
}
