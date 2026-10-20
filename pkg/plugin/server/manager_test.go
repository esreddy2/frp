// Copyright 2019 fatedier, fatedier@gmail.com
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
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goliblog "github.com/fatedier/golib/log"

	"github.com/fatedier/frp/pkg/msg"
	frplog "github.com/fatedier/frp/pkg/util/log"
)

type testPlugin struct {
	name    string
	ops     map[string]bool
	handler func(context.Context, string, any) (*Response, any, error)
}

// Log-capturing subtests serialize global logger swaps; do not use t.Parallel.
var logCaptureMu sync.Mutex

type logCapture struct {
	bytes.Buffer
	mu     sync.Mutex
	levels []goliblog.Level
}

func (p testPlugin) Name() string {
	return p.name
}

func (p testPlugin) IsSupport(op string) bool {
	return p.ops[op]
}

func (p testPlugin) Handle(ctx context.Context, op string, content any) (*Response, any, error) {
	return p.handler(ctx, op, content)
}

func (w *logCapture) WriteLog(p []byte, level goliblog.Level, _ time.Time) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.levels = append(w.levels, level)
	return w.Write(p)
}

func captureLogOutput(t *testing.T) *logCapture {
	t.Helper()

	logCaptureMu.Lock()
	logOutput := &logCapture{}
	oldLogger := frplog.Logger
	frplog.Logger = goliblog.New(
		goliblog.WithOutput(logOutput),
		goliblog.WithLevel(goliblog.TraceLevel),
		goliblog.WithCaller(false),
	)
	t.Cleanup(func() {
		frplog.Logger = oldLogger
		logCaptureMu.Unlock()
	})
	return logOutput
}

var mutablePluginOps = []struct {
	name string
	op   string
}{
	{name: "login", op: OpLogin},
	{name: "new proxy", op: OpNewProxy},
	{name: "ping", op: OpPing},
	{name: "new work conn", op: OpNewWorkConn},
	{name: "new user conn", op: OpNewUserConn},
}

func callMutableWithUser(m *Manager, op string, user string) (string, error) {
	switch op {
	case OpLogin:
		got, err := m.Login(&LoginContent{Login: msg.Login{User: user}})
		if got == nil {
			return "", err
		}
		return got.User, err
	case OpNewProxy:
		got, err := m.NewProxy(&NewProxyContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpPing:
		got, err := m.Ping(&PingContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpNewWorkConn:
		got, err := m.NewWorkConn(&NewWorkConnContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	case OpNewUserConn:
		got, err := m.NewUserConn(&NewUserConnContent{User: UserInfo{User: user}})
		if got == nil {
			return "", err
		}
		return got.User.User, err
	default:
		panic("unsupported mutable op: " + op)
	}
}

func mutableUser(t *testing.T, op string, content any) string {
	t.Helper()

	switch op {
	case OpLogin:
		return content.(LoginContent).User
	case OpNewProxy:
		return content.(NewProxyContent).User.User
	case OpPing:
		return content.(PingContent).User.User
	case OpNewWorkConn:
		return content.(NewWorkConnContent).User.User
	case OpNewUserConn:
		return content.(NewUserConnContent).User.User
	default:
		t.Fatalf("unsupported mutable op: %s", op)
		return ""
	}
}

func mutateMutableContent(t *testing.T, op string, content any, user string) any {
	t.Helper()

	switch op {
	case OpLogin:
		got := content.(LoginContent)
		got.User = user
		return &got
	case OpNewProxy:
		got := content.(NewProxyContent)
		got.User.User = user
		return &got
	case OpPing:
		got := content.(PingContent)
		got.User.User = user
		return &got
	case OpNewWorkConn:
		got := content.(NewWorkConnContent)
		got.User.User = user
		return &got
	case OpNewUserConn:
		got := content.(NewUserConnContent)
		got.User.User = user
		return &got
	default:
		t.Fatalf("unsupported mutable op: %s", op)
		return nil
	}
}

func TestManagerMutableContentAcrossOps(t *testing.T) {
	for _, tt := range mutablePluginOps {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()
			m.Register(testPlugin{
				name: "mutate",
				ops:  map[string]bool{tt.op: true},
				handler: func(ctx context.Context, op string, content any) (*Response, any, error) {
					if op != tt.op {
						t.Fatalf("unexpected op: %s", op)
					}
					if GetReqidFromContext(ctx) == "" {
						t.Fatal("expected request id in context")
					}
					if got := mutableUser(t, tt.op, content); got != "initial" {
						t.Fatalf("expected initial user, got %q", got)
					}
					return &Response{Unchange: false}, mutateMutableContent(t, tt.op, content, "mutated"), nil
				},
			})
			m.Register(testPlugin{
				name: "observe",
				ops:  map[string]bool{tt.op: true},
				handler: func(ctx context.Context, op string, content any) (*Response, any, error) {
					if op != tt.op {
						t.Fatalf("unexpected op: %s", op)
					}
					if GetReqidFromContext(ctx) == "" {
						t.Fatal("expected request id in context")
					}
					if got := mutableUser(t, tt.op, content); got != "mutated" {
						t.Fatalf("expected mutated user, got %q", got)
					}
					return &Response{Unchange: true}, mutateMutableContent(t, tt.op, content, "ignored"), nil
				},
			})

			got, err := callMutableWithUser(m, tt.op, "initial")
			if err != nil {
				t.Fatalf("mutable op failed: %v", err)
			}
			if got != "mutated" {
				t.Fatalf("expected mutated user, got %q", got)
			}
		})
	}
}

func TestManagerMutableContentRejectStopsChain(t *testing.T) {
	m := NewManager()

	var called bool
	m.Register(testPlugin{
		name: "reject",
		ops:  map[string]bool{OpPing: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			return &Response{Reject: true, RejectReason: "blocked"}, nil, nil
		},
	})
	m.Register(testPlugin{
		name: "unused",
		ops:  map[string]bool{OpPing: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			called = true
			return &Response{Unchange: true}, nil, nil
		},
	})

	got, err := m.Ping(&PingContent{})
	if err == nil {
		t.Fatal("expected reject error")
	}
	if got != nil {
		t.Fatalf("expected no returned content, got %#v", got)
	}
	if err.Error() != "blocked" {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("expected plugin chain to stop after reject")
	}
}

func TestManagerMutableContentPluginErrorLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		op    string
		level goliblog.Level
	}{
		{name: "default warning", op: OpLogin, level: goliblog.WarnLevel},
		{name: "new user conn info", op: OpNewUserConn, level: goliblog.InfoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := captureLogOutput(t)
			m := NewManager()
			m.Register(testPlugin{
				name: "error",
				ops:  map[string]bool{tt.op: true},
				handler: func(context.Context, string, any) (*Response, any, error) {
					return nil, nil, errors.New("boom")
				},
			})

			_, err := callMutableWithUser(m, tt.op, "initial")
			if err == nil {
				t.Fatal("expected plugin error")
			}
			if want := "send " + tt.op + " request to plugin error"; err.Error() != want {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(logOutput.levels) != 1 || logOutput.levels[0] != tt.level {
				t.Fatalf("expected log level %v, got %v in %q", tt.level, logOutput.levels, logOutput.String())
			}
		})
	}
}

// TestManagerCloseProxyReportsDeliveryFailureInLog covers the asynchronous
// delivery contract: CloseProxy only reports a failure to enqueue the
// notification, while a delivery failure is reported through the log.
func TestManagerCloseProxyReportsDeliveryFailureInLog(t *testing.T) {
	logOutput := captureLogOutput(t)
	m := NewManager()
	defer m.Close()

	for _, name := range []string{"first", "second"} {
		m.Register(testPlugin{
			name: name,
			ops:  map[string]bool{OpCloseProxy: true},
			handler: func(ctx context.Context, op string, content any) (*Response, any, error) {
				if GetReqidFromContext(ctx) == "" {
					t.Error("expected request id in context")
				}
				if op != OpCloseProxy {
					t.Errorf("unexpected op: %s", op)
				}
				return nil, nil, errors.New(name + " error")
			},
		})
	}

	if err := m.CloseProxy(&CloseProxyContent{}); err != nil {
		t.Fatalf("expected the enqueue to succeed, got %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		logOutput.mu.Lock()
		count := len(logOutput.levels)
		logOutput.mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	logOutput.mu.Lock()
	levels := append([]goliblog.Level(nil), logOutput.levels...)
	body := logOutput.String()
	logOutput.mu.Unlock()
	if len(levels) != 2 {
		t.Fatalf("expected two warning logs, got %v in %q", levels, body)
	}
	for _, level := range levels {
		if level != goliblog.WarnLevel {
			t.Fatalf("expected warning log level, got %v", level)
		}
	}
	if !strings.Contains(body, "first error") || !strings.Contains(body, "second error") {
		t.Fatalf("missing delivery errors in log: %q", body)
	}
}

// testRetryableError marks a failure that never reached the plugin, so the
// notification may be delivered again.
type testRetryableError struct {
	err error
}

func (e *testRetryableError) Error() string { return e.err.Error() }

func (e *testRetryableError) Unwrap() error { return e.err }

func (e *testRetryableError) retryableNotificationTransport() {}

func TestManagerCloseProxyRetriesUndeliveredNotification(t *testing.T) {
	m := newManagerWithCloseProxyDelivery(
		8,
		func(int) time.Duration { return time.Millisecond },
		2*time.Second,
		2*time.Second,
	)
	defer m.Close()

	var calls atomic.Int32
	delivered := make(chan CloseProxyContent, 1)
	m.Register(testPlugin{
		name: "flaky",
		ops:  map[string]bool{OpCloseProxy: true},
		handler: func(_ context.Context, _ string, content any) (*Response, any, error) {
			if calls.Add(1) < 3 {
				return nil, nil, &testRetryableError{err: errors.New("connection reset")}
			}
			delivered <- content.(CloseProxyContent)
			return &Response{Unchange: true}, nil, nil
		},
	})

	if err := m.CloseProxy(&CloseProxyContent{
		AttemptID: "attempt-1",
		User:      UserInfo{User: "alice"},
	}); err != nil {
		t.Fatalf("expected the enqueue to succeed, got %v", err)
	}

	select {
	case got := <-delivered:
		if got.AttemptID != "attempt-1" {
			t.Fatalf("unexpected attempt id: %q", got.AttemptID)
		}
		if got.User.User != "alice" {
			t.Fatalf("unexpected user: %q", got.User.User)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the retried notification")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected three delivery attempts, got %d", got)
	}
}

func TestManagerCloseProxyDoesNotRetryAnsweredNotification(t *testing.T) {
	m := newManagerWithCloseProxyDelivery(
		8,
		func(int) time.Duration { return time.Millisecond },
		2*time.Second,
		2*time.Second,
	)
	defer m.Close()

	var calls atomic.Int32
	m.Register(testPlugin{
		name: "rejecting",
		ops:  map[string]bool{OpCloseProxy: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			calls.Add(1)
			return nil, nil, errors.New("plugin rejected the notification")
		},
	})

	if err := m.CloseProxy(&CloseProxyContent{}); err != nil {
		t.Fatalf("expected the enqueue to succeed, got %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected a single delivery attempt, got %d", got)
	}
}

func TestManagerCloseProxyRejectsNotificationsAfterClose(t *testing.T) {
	m := NewManager()
	m.Register(testPlugin{
		name: "closer",
		ops:  map[string]bool{OpCloseProxy: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			return &Response{Unchange: true}, nil, nil
		},
	})
	m.Close()

	err := m.CloseProxy(&CloseProxyContent{})
	if !errors.Is(err, errPluginManagerClosed) {
		t.Fatalf("expected the closed manager error, got %v", err)
	}
}

func TestManagerCloseDrainsQueuedNotifications(t *testing.T) {
	m := newManagerWithCloseProxyDelivery(
		8,
		func(int) time.Duration { return time.Millisecond },
		2*time.Second,
		2*time.Second,
	)

	delivered := make(chan CloseProxyContent, 1)
	release := make(chan struct{})
	m.Register(testPlugin{
		name: "slow",
		ops:  map[string]bool{OpCloseProxy: true},
		handler: func(_ context.Context, _ string, content any) (*Response, any, error) {
			<-release
			delivered <- content.(CloseProxyContent)
			return &Response{Unchange: true}, nil, nil
		},
	})

	if err := m.CloseProxy(&CloseProxyContent{AttemptID: "attempt-2"}); err != nil {
		t.Fatalf("expected the enqueue to succeed, got %v", err)
	}
	close(release)

	m.Close()

	select {
	case got := <-delivered:
		if got.AttemptID != "attempt-2" {
			t.Fatalf("unexpected attempt id: %q", got.AttemptID)
		}
	default:
		t.Fatal("expected the queued notification to be delivered before Close returned")
	}
}

func TestManagerNewProxyResultReportsRejection(t *testing.T) {
	m := NewManager()
	defer m.Close()

	var received []NewProxyResultContent
	m.Register(testPlugin{
		name: "result",
		ops:  map[string]bool{OpNewProxyResult: true},
		handler: func(_ context.Context, op string, content any) (*Response, any, error) {
			if op != OpNewProxyResult {
				t.Errorf("unexpected op: %s", op)
			}
			received = append(received, content.(NewProxyResultContent))
			return &Response{Reject: true, RejectReason: "quota exceeded"}, nil, nil
		},
	})

	err := m.NewProxyResult(&NewProxyResultContent{
		User:      UserInfo{User: "alice"},
		AttemptID: "attempt-3",
		ProxyName: "web",
		Admitted:  true,
	})
	if err == nil {
		t.Fatal("expected the rejection to fail the admission")
	}
	if !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("expected the reject reason in the error, got %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("expected one result delivery, got %d", len(received))
	}
	if received[0].AttemptID != "attempt-3" || received[0].ProxyName != "web" || !received[0].Admitted {
		t.Fatalf("unexpected result content: %#v", received[0])
	}
}

func TestManagerNewProxyResultReportsDeliveryFailure(t *testing.T) {
	m := NewManager()
	defer m.Close()

	m.Register(testPlugin{
		name: "result",
		ops:  map[string]bool{OpNewProxyResult: true},
		handler: func(context.Context, string, any) (*Response, any, error) {
			return nil, nil, errors.New("plugin is unreachable")
		},
	})

	err := m.NewProxyResult(&NewProxyResultContent{AttemptID: "attempt-4"})
	if err == nil {
		t.Fatal("expected the delivery failure to fail the admission")
	}
	if !strings.Contains(err.Error(), "result plugin delivery failed") {
		t.Fatalf("expected the sanitized delivery failure, got %v", err)
	}
	if strings.Contains(err.Error(), "plugin is unreachable") {
		t.Fatalf("expected the raw plugin error to stay out of the returned error, got %v", err)
	}
}

func TestManagerNewProxyResultSucceedsWithoutPlugins(t *testing.T) {
	m := NewManager()
	defer m.Close()

	if err := m.NewProxyResult(&NewProxyResultContent{AttemptID: "attempt-5"}); err != nil {
		t.Fatalf("expected no error without result plugins, got %v", err)
	}
}

func TestManagerNewProxyKeepsAttemptIDAcrossMutatingPlugins(t *testing.T) {
	m := NewManager()
	defer m.Close()

	m.Register(testPlugin{
		name: "mutate",
		ops:  map[string]bool{OpNewProxy: true},
		handler: func(_ context.Context, _ string, content any) (*Response, any, error) {
			got := content.(NewProxyContent)
			got.AttemptID = "hijacked"
			got.ProxyName = "renamed"
			return &Response{Unchange: false}, &got, nil
		},
	})

	content := &NewProxyContent{
		User:      UserInfo{User: "alice"},
		AttemptID: "attempt-6",
	}
	got, err := m.NewProxy(content)
	if err != nil {
		t.Fatalf("new proxy failed: %v", err)
	}
	if got.AttemptID != "attempt-6" {
		t.Fatalf("expected the attempt id to be preserved, got %q", got.AttemptID)
	}
	if got.ProxyName != "renamed" {
		t.Fatalf("expected the plugin rename to be kept, got %q", got.ProxyName)
	}
}

func TestUserInfoCloneIsIndependent(t *testing.T) {
	metas := map[string]string{"key": "value"}
	original := UserInfo{User: "alice", Metas: metas, RunID: "run-1"}

	clone := original.Clone()
	clone.Metas["key"] = "changed"

	if original.Metas["key"] != "value" {
		t.Fatalf("expected the original metas to be unchanged, got %q", original.Metas["key"])
	}
	if clone.User != "alice" || clone.RunID != "run-1" {
		t.Fatalf("unexpected cloned identity: %#v", clone)
	}
}
