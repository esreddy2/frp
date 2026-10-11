//go:build !frps

package proxy

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/util/xlog"
)

type closingWorkProxy struct {
	Proxy
	accepted atomic.Int64
}

func (p *closingWorkProxy) InWorkConn(conn net.Conn, _ *msg.StartWorkConn) {
	p.accepted.Add(1)
	_ = conn.Close()
}

func (p *closingWorkProxy) Close() {}

// newWrapperForLifecycleTest returns a wrapper that is ready to be stopped.
func newWrapperForLifecycleTest(t *testing.T) *Wrapper {
	t.Helper()

	return &Wrapper{
		pxy:            &closingWorkProxy{},
		xl:             xlog.New(),
		closeCh:        make(chan struct{}),
		healthNotifyCh: make(chan struct{}),
		handler:        func(any) error { return nil },
	}
}

// The notification channel of the health check is not closed when the proxy
// is stopped: the callbacks of the health check send on it, and a send on a
// closed channel panics even when it is in a select with a default branch.
// Closing the channel while a callback sends on it is a data race as well:
// the runtime records a send as a read of the channel and a close as a write
// of it.
func TestWrapperStopLeavesTheHealthNotifyChannelOpen(t *testing.T) {
	require := require.New(t)

	pw := newWrapperForLifecycleTest(t)
	pw.Stop()

	// A late notification does not panic: the channel of a stopped proxy is
	// not closed.
	require.NotPanics(func() {
		select {
		case pw.healthNotifyCh <- struct{}{}:
		default:
		}
	})

	// A late health check callback does not panic either.
	require.NotPanics(func() {
		pw.statusNormalCallback()
		pw.statusFailedCallback()
	})
}

// The callbacks of the health check and the stop of the proxy must not race:
// the callbacks send on the notification channel of the wrapper, and the stop
// used to close that channel.
//
// The runtime records a send as a read of the channel and a close as a write
// of it, so the race detector reports the two as a data race. The panic that
// the send on a closed channel causes is contained by a recover, but the race
// itself is not: the test is meaningful under the race detector.
func TestWrapperHealthNotifyDoesNotRaceWithStop(t *testing.T) {
	pw := newWrapperForLifecycleTest(t)

	// The callback runs in its own goroutine and the wrapper is stopped at
	// the same time: the send and the close are unordered.
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-start
		pw.statusNormalCallback()
	}()
	close(start)
	pw.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the health check callback did not finish")
	}
}

func assertWrapperWorkConnClosed(t *testing.T, pw *Wrapper) {
	t.Helper()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	pw.InWorkConn(conn, &msg.StartWorkConn{})
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestWrapperInWorkConnPhaseSnapshot(t *testing.T) {
	pxy := &closingWorkProxy{}
	pw := &Wrapper{pxy: pxy, xl: xlog.New()}
	for _, phase := range []string{ProxyPhaseNew, ProxyPhaseWaitStart, ProxyPhaseStartErr, ProxyPhaseCheckFailed, ProxyPhaseClosed} {
		pw.Phase = phase
		assertWrapperWorkConnClosed(t, pw)
	}
	require.Zero(t, pxy.accepted.Load(), "inactive proxy received a work connection")
	pw.Phase = ProxyPhaseRunning
	pw.pxy = nil
	assertWrapperWorkConnClosed(t, pw)
	pw.pxy = pxy
	assertWrapperWorkConnClosed(t, pw)
	require.Equal(t, int64(1), pxy.accepted.Load())

	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		<-start
		for range 20000 {
			pw.mu.Lock()
			pw.Phase = ProxyPhaseRunning
			pw.mu.Unlock()
			pw.mu.Lock()
			pw.Phase = ProxyPhaseClosed
			pw.mu.Unlock()
		}
		close(done)
	}()
	close(start)
	for range 10000 {
		assertWrapperWorkConnClosed(t, pw)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("phase writer did not finish")
	}
}
