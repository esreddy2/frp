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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	plugin "github.com/fatedier/frp/pkg/plugin/server"
	"github.com/fatedier/frp/server/registry"
)

func TestServiceCloseControlAbsentOrFinishedReturnsFalse(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}
	require.False(t, service.CloseControl("missing"), "CloseControl(missing)")

	metrics := newCountingServerMetrics()
	ctl, _ := newLifecycleTestControl(t, "finished", "client", metrics)
	mustAddAndActivate(t, manager, ctl)
	require.True(t, ctl.Start())
	require.NoError(t, ctl.Close())
	waitForControlDone(t, ctl)

	require.False(t, service.CloseControl("finished"), "CloseControl(finished)")
}

func TestServiceCloseControlClosesCurrentGeneration(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}

	metrics := newCountingServerMetrics()
	ctl, _ := newLifecycleTestControl(t, "same-run", "client", metrics)
	mustAddAndActivate(t, manager, ctl)
	require.True(t, ctl.Start())

	require.True(t, service.CloseControl("same-run"), "CloseControl(current generation)")
	waitForControlDone(t, ctl)

	require.False(t, service.CloseControl("same-run"),
		"CloseControl on a finished current generation reports false")
}

// A replacement control for the same run ID becomes the current generation.
// CloseControl must act on the current generation and must not re-close the
// retired one.
func TestServiceCloseControlClosesExactCurrentGenerationAcrossReplacement(t *testing.T) {
	manager := NewControlManager(registry.NewClientRegistry())
	service := &Service{ctlManager: manager}

	metrics := newCountingServerMetrics()
	oldCtl, _ := newLifecycleTestControl(t, "same-run", "client", metrics)
	mustAddAndActivate(t, manager, oldCtl)
	require.True(t, oldCtl.Start())

	newCtl, _ := newLifecycleTestControl(t, "same-run", "client", metrics)
	mustAddAndActivate(t, manager, newCtl)
	require.True(t, newCtl.Start())
	require.NotSame(t, oldCtl, newCtl)

	current, ok := manager.GetByID("same-run")
	require.True(t, ok)
	require.Same(t, newCtl, current, "the replacement is the current manager entry")

	require.True(t, service.CloseControl("same-run"), "CloseControl(current generation)")
	waitForControlDone(t, newCtl)
}

// The control worker owns the CloseProxy plugin notification, so the exact
// attempt identity must reach the plugin.
func TestServiceCloseControlWorkerEmitsExactCloseProxy(t *testing.T) {
	const (
		runID     = "0123456789abcdef"
		proxyName = "proxy-exact"
		attemptID = "0123456789abcdef0123456789abcdef"
	)

	delivered := make(chan plugin.CloseProxyContent, 1)
	pluginManager := plugin.NewManager()
	pluginManager.Register(&admissionTestPlugin{handle: func(_ context.Context, op string, content any) (*plugin.Response, any, error) {
		if op != plugin.OpCloseProxy {
			return &plugin.Response{Unchange: true}, nil, nil
		}
		delivered <- content.(plugin.CloseProxyContent)
		return &plugin.Response{Unchange: true}, nil, nil
	}})
	t.Cleanup(pluginManager.Close)

	manager := NewControlManager(registry.NewClientRegistry())
	metrics := newCountingServerMetrics()
	ctl, _ := newLifecycleTestControl(t, runID, "client", metrics)
	ctl.sessionCtx.PluginManager = pluginManager
	ctl.proxies[proxyName] = newRegisteredProxy(
		newAdmissionTestProxy(proxyName),
		plugin.UserInfo{User: "alice", RunID: runID},
		attemptID,
	)
	mustAddAndActivate(t, manager, ctl)
	require.True(t, ctl.Start())

	service := &Service{ctlManager: manager}
	require.True(t, service.CloseControl(runID), "CloseControl(active worker)")
	waitForControlDone(t, ctl)

	select {
	case got := <-delivered:
		require.Equal(t, runID, got.User.RunID)
		require.Equal(t, proxyName, got.ProxyName)
		require.Equal(t, attemptID, got.AttemptID)
	case <-time.After(5 * time.Second):
		t.Fatal("the control worker did not emit the CloseProxy callback")
	}
}
