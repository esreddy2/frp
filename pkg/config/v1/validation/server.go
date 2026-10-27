// Copyright 2023 The frp Authors
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

package validation

import (
	"fmt"
	"slices"
	"time"

	"github.com/samber/lo"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	splugin "github.com/fatedier/frp/pkg/plugin/server"
)

func (v *ConfigValidator) ValidateServerConfig(c *v1.ServerConfig) (Warning, error) {
	var (
		warnings Warning
		errs     error
	)
	if !slices.Contains(SupportedAuthMethods, c.Auth.Method) {
		errs = AppendError(errs, fmt.Errorf("invalid auth method, optional values are %v", SupportedAuthMethods))
	}
	if !lo.Every(SupportedAuthAdditionalScopes, c.Auth.AdditionalScopes) {
		errs = AppendError(errs, fmt.Errorf("invalid auth additional scopes, optional values are %v", SupportedAuthAdditionalScopes))
	}

	errs = AppendError(errs, v.validateAuthTokenSource(c.Auth.Token, c.Auth.TokenSource))

	if err := validateLogConfig(&c.Log); err != nil {
		errs = AppendError(errs, err)
	}

	if err := validateWebServerConfig(&c.WebServer); err != nil {
		errs = AppendError(errs, err)
	}
	if err := validateMetricsServerConfig(&c.MetricsServer, &c.WebServer); err != nil {
		errs = AppendError(errs, err)
	}
	if c.Transport.TLS.UseSystemCa {
		errs = AppendError(errs, fmt.Errorf("transport.tls.useSystemCa is only supported in frpc transport.tls"))
	}

	errs = AppendError(errs, ValidatePort(c.BindPort, "bindPort"))
	errs = AppendError(errs, ValidatePort(c.KCPBindPort, "kcpBindPort"))
	errs = AppendError(errs, ValidatePort(c.QUICBindPort, "quicBindPort"))
	errs = AppendError(errs, ValidatePort(c.VhostHTTPPort, "vhostHTTPPort"))
	errs = AppendError(errs, ValidatePort(c.VhostHTTPSPort, "vhostHTTPSPort"))
	errs = AppendError(errs, ValidatePort(c.TCPMuxHTTPConnectPort, "tcpMuxHTTPConnectPort"))
	if c.Transport.MaxPoolCount < 0 {
		errs = AppendError(errs, fmt.Errorf("invalid transport.maxPoolCount, must be non-negative"))
	}

	newProxyPluginCount := 0
	newProxyResultPluginCount := 0
	for _, p := range c.HTTPPlugins {
		if !lo.Every(SupportedHTTPPluginOps, p.Ops) {
			errs = AppendError(errs, fmt.Errorf("invalid http plugin ops, optional values are %v", SupportedHTTPPluginOps))
		}
		if slices.Contains(p.Ops, splugin.OpNewProxy) {
			newProxyPluginCount++
		}
		if slices.Contains(p.Ops, splugin.OpNewProxyResult) {
			if !slices.Contains(p.Ops, splugin.OpCloseProxy) {
				errs = AppendError(errs, fmt.Errorf(
					"http plugin %q: %s requires %s for admission rollback",
					p.Name, splugin.OpNewProxyResult, splugin.OpCloseProxy,
				))
			}
			newProxyResultPluginCount++
		}
	}
	if newProxyResultPluginCount > 0 && c.Transport.HeartbeatTimeout > 0 {
		callbackCount := newProxyPluginCount + newProxyResultPluginCount
		callbackBudgetSeconds := int64(callbackCount) * int64(splugin.HTTPPluginRequestTimeout/time.Second)
		if c.Transport.HeartbeatTimeout <= callbackBudgetSeconds {
			errs = AppendError(errs, fmt.Errorf(
				"transport.heartbeatTimeout %ds must exceed aggregate synchronous NewProxy callback budget %ds "+
					"(%d NewProxy + %d NewProxyResult HTTP callbacks); raise it or disable the application heartbeat",
				c.Transport.HeartbeatTimeout,
				callbackBudgetSeconds,
				newProxyPluginCount,
				newProxyResultPluginCount,
			))
		}
	}

	if !c.Transport.RequireKeyShare && c.Auth.Method != v1.AuthMethodToken {
		tlsEnabled := c.Transport.TLS.CertFile != "" || c.Transport.TLS.KeyFile != ""
		if !tlsEnabled {
			warnings = AppendError(warnings, fmt.Errorf(
				"transport.requireKeyShare is false on unencrypted server without token auth; "+
					"wireProtocol v2 clients that omit an X25519 key share can be passively decrypted "+
					"(set transport.requireKeyShare = true or configure TLS)",
			))
		}
	}
	return warnings, errs
}
