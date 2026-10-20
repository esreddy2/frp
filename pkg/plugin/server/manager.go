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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/xlog"
)

const (
	closeProxyNotificationQueueSize = 2048
	closeProxyNotificationTimeout   = 30 * time.Second
	closeProxyGraceTimeout          = 5 * time.Second
	closeProxyRetryInitialDelay     = 100 * time.Millisecond
	closeProxyRetryMaxDelay         = 5 * time.Second
)

var (
	errPluginManagerClosed         = errors.New("plugin manager is closed")
	errPluginNotificationQueueFull = errors.New("plugin notification queue is full")
)

// retryableNotificationTransportError marks a notification failure that the
// plugin has not acknowledged, so the delivery may be retried.
type retryableNotificationTransportError interface {
	error
	retryableNotificationTransport()
}

type closeProxyNotification struct {
	plugin  Plugin
	content CloseProxyContent
	ctx     context.Context
	xl      *xlog.Logger
}

type Manager struct {
	loginPlugins          []Plugin
	newProxyPlugins       []Plugin
	newProxyResultPlugins []Plugin
	closeProxyPlugins     []Plugin
	pingPlugins           []Plugin
	newWorkConnPlugins    []Plugin
	newUserConnPlugins    []Plugin

	closeProxyQueue      chan closeProxyNotification
	closeProxyDrain      chan struct{}
	closeProxyTimeout    time.Duration
	closeProxyRetryDelay func(int) time.Duration
	closeGraceTimeout    time.Duration
	notificationCtx      context.Context
	notificationCancel   context.CancelFunc
	notificationMu       sync.Mutex
	notificationWG       sync.WaitGroup
	pendingNotifications atomic.Int64
	closeProxyWorkerOn   bool
	closed               bool
}

func NewManager() *Manager {
	return newManagerWithCloseProxyDelivery(
		closeProxyNotificationQueueSize,
		defaultCloseProxyRetryDelay,
		closeProxyNotificationTimeout,
		closeProxyGraceTimeout,
	)
}

func newManagerWithCloseProxyDelivery(
	queueSize int,
	retryDelay func(int) time.Duration,
	deliveryTimeout time.Duration,
	graceTimeout time.Duration,
) *Manager {
	if queueSize < 1 {
		queueSize = 1
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = closeProxyNotificationTimeout
	}
	if graceTimeout <= 0 {
		graceTimeout = closeProxyGraceTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		loginPlugins:          make([]Plugin, 0),
		newProxyPlugins:       make([]Plugin, 0),
		newProxyResultPlugins: make([]Plugin, 0),
		closeProxyPlugins:     make([]Plugin, 0),
		pingPlugins:           make([]Plugin, 0),
		newWorkConnPlugins:    make([]Plugin, 0),
		newUserConnPlugins:    make([]Plugin, 0),
		closeProxyQueue:       make(chan closeProxyNotification, queueSize),
		closeProxyDrain:       make(chan struct{}),
		closeProxyTimeout:     deliveryTimeout,
		closeProxyRetryDelay:  retryDelay,
		closeGraceTimeout:     graceTimeout,
		notificationCtx:       ctx,
		notificationCancel:    cancel,
	}
}

func defaultCloseProxyRetryDelay(failureCount int) time.Duration {
	delay := closeProxyRetryInitialDelay
	for i := 1; i < failureCount && delay < closeProxyRetryMaxDelay; i++ {
		delay = min(delay*2, closeProxyRetryMaxDelay)
	}
	return delay
}

func newPluginRequestContext() (context.Context, *xlog.Logger) {
	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(context.Background(), xl)
	return NewReqidContext(ctx, reqid), xl
}

type pluginErrorLogMode bool

const (
	// Warn is the zero value because it is the default for mutable plugin operations.
	pluginErrorLogWarn pluginErrorLogMode = false
	pluginErrorLogInfo pluginErrorLogMode = true
)

func logPluginError(xl *xlog.Logger, p Plugin, op string, err error, mode pluginErrorLogMode) {
	if mode == pluginErrorLogInfo {
		xl.Infof("send %s request to plugin [%s] error: %v", op, p.Name(), err)
		return
	}
	xl.Warnf("send %s request to plugin [%s] error: %v", op, p.Name(), err)
}

func handleMutableContent[T any](
	plugins []Plugin,
	op string,
	content *T,
	logMode pluginErrorLogMode,
	// afterMutate lets a caller reassert server-owned fields after a plugin
	// replaces the content, before the next plugin in the chain sees it.
	afterMutate ...func(*T),
) (*T, error) {
	if len(plugins) == 0 {
		return content, nil
	}

	var (
		res = &Response{
			Reject:   false,
			Unchange: true,
		}
		retContent any
		err        error
	)
	ctx, xl := newPluginRequestContext()

	for _, p := range plugins {
		res, retContent, err = p.Handle(ctx, op, *content)
		if err != nil {
			logPluginError(xl, p, op, err, logMode)
			return nil, errors.New("send " + op + " request to plugin error")
		}
		if res.Reject {
			return nil, fmt.Errorf("%s", res.RejectReason)
		}
		if !res.Unchange {
			// Preserve the existing Plugin contract: changed content must be *T.
			// Buggy Plugin implementations still panic here, by design.
			content = retContent.(*T)
			for _, fix := range afterMutate {
				fix(content)
			}
		}
	}
	return content, nil
}

func (m *Manager) Register(p Plugin) {
	if p.IsSupport(OpLogin) {
		m.loginPlugins = append(m.loginPlugins, p)
	}
	if p.IsSupport(OpNewProxy) {
		m.newProxyPlugins = append(m.newProxyPlugins, p)
	}
	if p.IsSupport(OpNewProxyResult) {
		m.newProxyResultPlugins = append(m.newProxyResultPlugins, p)
	}
	if p.IsSupport(OpCloseProxy) {
		m.closeProxyPlugins = append(m.closeProxyPlugins, p)
	}
	if p.IsSupport(OpPing) {
		m.pingPlugins = append(m.pingPlugins, p)
	}
	if p.IsSupport(OpNewWorkConn) {
		m.newWorkConnPlugins = append(m.newWorkConnPlugins, p)
	}
	if p.IsSupport(OpNewUserConn) {
		m.newUserConnPlugins = append(m.newUserConnPlugins, p)
	}
}

func (m *Manager) Login(content *LoginContent) (*LoginContent, error) {
	return handleMutableContent(m.loginPlugins, OpLogin, content, pluginErrorLogWarn)
}

func (m *Manager) NewProxy(content *NewProxyContent) (*NewProxyContent, error) {
	// AttemptID is FRPS-owned correlation state, not mutable proxy
	// configuration. Reassert it after every plugin that replaces the content
	// so a response cannot alias a later NewProxyResult to another attempt.
	attemptID := content.AttemptID
	return handleMutableContent(m.newProxyPlugins, OpNewProxy, content, pluginErrorLogWarn,
		func(c *NewProxyContent) { c.AttemptID = attemptID })
}

// CloseProxy queues the lifecycle notification for asynchronous delivery. The
// notification is retried while the plugin transport fails without a
// response, and the immutable attempt ID makes a redelivery idempotent for
// the consumer.
//
// The returned error reports a failure to enqueue the notification, not a
// failure of the plugin itself: delivery errors are reported through the log.
func (m *Manager) CloseProxy(content *CloseProxyContent) error {
	if len(m.closeProxyPlugins) == 0 {
		return nil
	}

	reqid, _ := util.RandID()
	for _, p := range m.closeProxyPlugins {
		xl := xlog.New().AppendPrefix("reqid: " + reqid)
		ctx := xlog.NewContext(m.notificationCtx, xl)
		ctx = NewReqidContext(ctx, reqid)
		task := closeProxyNotification{
			plugin: p,
			content: CloseProxyContent{
				User:       content.User.Clone(),
				AttemptID:  content.AttemptID,
				CloseProxy: content.CloseProxy,
			},
			ctx: ctx,
			xl:  xl,
		}
		if err := m.enqueueCloseProxy(task); err != nil {
			return err
		}
	}
	return nil
}

// Close stops accepting new lifecycle notifications and waits a bounded time
// for the queued ones to be delivered.
//
// Notifications already queued are drained within closeGraceTimeout so a
// graceful server shutdown still reports proxy teardown to the plugins. When
// the grace period expires, the remaining notifications are abandoned and the
// delivery context is cancelled; consumers must reconcile their own snapshot
// when they stop.
func (m *Manager) Close() {
	m.notificationMu.Lock()
	if m.closed {
		m.notificationMu.Unlock()
		m.notificationWG.Wait()
		return
	}
	m.closed = true
	close(m.closeProxyDrain)
	m.notificationMu.Unlock()

	drained := make(chan struct{})
	go func() {
		m.notificationWG.Wait()
		close(drained)
	}()

	timer := time.NewTimer(m.closeGraceTimeout)
	defer timer.Stop()
	select {
	case <-drained:
		return
	case <-timer.C:
		m.notificationCancel()
		<-drained
	}
}

func (m *Manager) enqueueCloseProxy(task closeProxyNotification) error {
	m.notificationMu.Lock()
	defer m.notificationMu.Unlock()
	if m.closed {
		return errPluginManagerClosed
	}
	if !m.closeProxyWorkerOn {
		m.closeProxyWorkerOn = true
		m.notificationWG.Add(1)
		go m.runCloseProxyWorker()
	}
	m.pendingNotifications.Add(1)
	select {
	case m.closeProxyQueue <- task:
		return nil
	default:
		m.pendingNotifications.Add(-1)
		return errPluginNotificationQueueFull
	}
}

func (m *Manager) runCloseProxyWorker() {
	defer m.notificationWG.Done()
	for {
		// Give shutdown priority over a buffered task. A single select could
		// randomly drain queued notifications after cancellation, contrary to
		// the bounded-shutdown contract.
		select {
		case <-m.notificationCtx.Done():
			return
		default:
		}

		select {
		case <-m.notificationCtx.Done():
			return
		case task := <-m.closeProxyQueue:
			if m.notificationCtx.Err() != nil {
				m.pendingNotifications.Add(-1)
				return
			}
			m.deliverCloseProxy(task)
			m.pendingNotifications.Add(-1)
			// The queue is empty and no new notification can be accepted, so
			// the manager is drained. Stop the worker so Close can return
			// without waiting for the grace period.
			if m.isClosed() && m.pendingNotifications.Load() == 0 {
				// Every queued notification has been delivered. The manager is
				// closing, so the worker can stop without waiting for the
				// grace timer to expire.
				return
			}
		case <-m.closeProxyDrain:
			// The manager is closing. Deliver the notifications that are
			// already queued, then stop. The delivery of a single notification
			// stays bounded by its own budget, and the closing grace period
			// bounds the whole drain.
			m.drainCloseProxyQueue()
			return
		}
	}
}

// drainCloseProxyQueue delivers the notifications that are already queued. It
// stops at the first delivery that exhausts the closing grace period.
func (m *Manager) drainCloseProxyQueue() {
	for {
		select {
		case <-m.notificationCtx.Done():
			return
		case task := <-m.closeProxyQueue:
			if m.notificationCtx.Err() != nil {
				m.pendingNotifications.Add(-1)
				return
			}
			m.deliverCloseProxy(task)
			m.pendingNotifications.Add(-1)
			if m.pendingNotifications.Load() == 0 {
				return
			}
		default:
			return
		}
	}
}

func (m *Manager) isClosed() bool {
	m.notificationMu.Lock()
	defer m.notificationMu.Unlock()
	return m.closed
}

func (m *Manager) deliverCloseProxy(task closeProxyNotification) {
	ctx, cancel := context.WithTimeout(task.ctx, m.closeProxyTimeout)
	defer cancel()
	task.ctx = ctx
	attempts := 0
	defer func() {
		if errors.Is(task.ctx.Err(), context.DeadlineExceeded) {
			task.xl.Warnf(
				"send %s request to plugin [%s] abandoned after %d attempts: delivery budget %s exhausted",
				OpCloseProxy,
				task.plugin.Name(),
				attempts,
				m.closeProxyTimeout,
			)
		}
	}()
	for failureCount := 0; ; failureCount++ {
		if task.ctx.Err() != nil {
			return
		}

		attempts = failureCount + 1
		_, _, err := task.plugin.Handle(task.ctx, OpCloseProxy, task.content)
		if err == nil {
			return
		}
		if task.ctx.Err() != nil {
			return
		}

		var transportErr retryableNotificationTransportError
		if !errors.As(err, &transportErr) {
			task.xl.Warnf("send %s request to plugin [%s] error: %v", OpCloseProxy, task.plugin.Name(), err)
			return
		}

		delay := m.closeProxyRetryDelay(failureCount + 1)
		task.xl.Warnf(
			"send %s request to plugin [%s] transport error (attempt %d), retrying in %s: %v",
			OpCloseProxy,
			task.plugin.Name(),
			failureCount+1,
			delay,
			err,
		)
		timer := time.NewTimer(delay)
		select {
		case <-task.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

// NewProxyResult synchronously asks every interested plugin to confirm the
// final FRPS admission outcome. An admitted proxy is not reported to the client
// until every plugin acknowledges the result. A reject or delivery failure is
// therefore authoritative for the admission transaction even though plugins
// cannot mutate the result content.
func (m *Manager) NewProxyResult(content *NewProxyResultContent) error {
	if len(m.newProxyResultPlugins) == 0 {
		return nil
	}

	reqid, _ := util.RandID()
	xl := xlog.New().AppendPrefix("reqid: " + reqid)
	ctx := xlog.NewContext(m.notificationCtx, xl)
	ctx = NewReqidContext(ctx, reqid)
	return m.deliverNewProxyResult(ctx, xl, content)
}

func (m *Manager) deliverNewProxyResult(
	ctx context.Context,
	xl *xlog.Logger,
	content *NewProxyResultContent,
) error {
	errs := make([]string, 0)
	deliveryFailed := false
	for _, p := range m.newProxyResultPlugins {
		res, _, err := p.Handle(ctx, OpNewProxyResult, *content)
		if err != nil {
			xl.Warnf("send %s request to plugin [%s] error: %v", OpNewProxyResult, p.Name(), err)
			deliveryFailed = true
			continue
		}
		if res == nil {
			err = errors.New("empty plugin response")
			xl.Warnf("send %s request to plugin [%s] error: %v", OpNewProxyResult, p.Name(), err)
			deliveryFailed = true
			continue
		}
		if res.Reject {
			reason := strings.TrimSpace(res.RejectReason)
			if reason == "" {
				reason = "rejected without reason"
			}
			xl.Warnf("plugin [%s] rejected %s: %s", p.Name(), OpNewProxyResult, reason)
			errs = append(errs, fmt.Sprintf("[%s]: %s", p.Name(), reason))
		}
	}

	if deliveryFailed {
		// The raw plugin failure is logged but not returned: it can carry
		// internal transport details that must not reach the client.
		errs = append([]string{"result plugin delivery failed"}, errs...)
	}
	if len(errs) > 0 {
		return fmt.Errorf("send %s request to plugin errors: %s", OpNewProxyResult, strings.Join(errs, "; "))
	}
	return nil
}

func (m *Manager) Ping(content *PingContent) (*PingContent, error) {
	return handleMutableContent(m.pingPlugins, OpPing, content, pluginErrorLogWarn)
}

func (m *Manager) NewWorkConn(content *NewWorkConnContent) (*NewWorkConnContent, error) {
	return handleMutableContent(m.newWorkConnPlugins, OpNewWorkConn, content, pluginErrorLogWarn)
}

func (m *Manager) NewUserConn(content *NewUserConnContent) (*NewUserConnContent, error) {
	// Preserve the pre-refactor log level for NewUserConn plugin errors.
	return handleMutableContent(m.newUserConnPlugins, OpNewUserConn, content, pluginErrorLogInfo)
}
