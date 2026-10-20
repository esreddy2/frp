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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func TestHTTPPluginRequestTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The handler must be released before the server is closed, otherwise the
	// server waits for the outstanding request forever.
	defer close(release)

	plugin := newHTTPPluginWithTimeout(v1.HTTPPluginOptions{
		Name: "slow",
		Addr: srv.URL,
		Path: "/plugin",
		Ops:  []string{OpPing},
	}, 100*time.Millisecond)

	start := time.Now()
	_, _, err := plugin.Handle(context.Background(), OpPing, PingContent{})
	if err == nil {
		t.Fatal("expected the request to time out")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("expected the timeout to bound the request, took %s", elapsed)
	}
}

func TestHTTPPluginCloseProxyDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/plugin" {
			http.Redirect(w, r, "/moved", http.StatusFound)
			return
		}
		redirected.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	plugin := newHTTPPluginWithTimeout(v1.HTTPPluginOptions{
		Name: "redirecting",
		Addr: srv.URL,
		Path: "/plugin",
		Ops:  []string{OpCloseProxy},
	}, 2*time.Second)

	_, _, err := plugin.Handle(context.Background(), OpCloseProxy, CloseProxyContent{})
	if err == nil {
		t.Fatal("expected the redirect response to fail the notification")
	}
	if got := redirected.Load(); got != 0 {
		t.Fatalf("expected the redirect to be terminal, the target was called %d times", got)
	}

	// The redirect produced a response, so the failure is semantic and must not
	// be treated as an undelivered notification.
	var transportErr retryableNotificationTransportError
	if errors.As(err, &transportErr) {
		t.Fatalf("expected a semantic error, got a retryable transport error: %v", err)
	}
}

func TestHTTPPluginCloseProxyUnreachableIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close()

	plugin := newHTTPPluginWithTimeout(v1.HTTPPluginOptions{
		Name: "gone",
		Addr: addr,
		Path: "/plugin",
		Ops:  []string{OpCloseProxy},
	}, 2*time.Second)

	_, _, err := plugin.Handle(context.Background(), OpCloseProxy, CloseProxyContent{})
	if err == nil {
		t.Fatal("expected the unreachable plugin to fail")
	}
	var transportErr retryableNotificationTransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("expected a retryable transport error, got %v", err)
	}
}
