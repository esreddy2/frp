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

package net

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A connection that keeps receiving data must not hit its read deadline:
// the deadline is an idle timeout, so it has to be pushed forward while the
// connection is busy.
func TestIdleReadDeadlineKeepsBusyConnectionAlive(t *testing.T) {
	require := require.New(t)

	const timeout = 200 * time.Millisecond

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	deadline := NewIdleReadDeadline(server, timeout)

	go func() {
		for {
			if _, err := client.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(timeout / 10)
		}
	}()

	buf := make([]byte, 1)
	busyUntil := time.Now().Add(5 * timeout)
	for time.Now().Before(busyUntil) {
		_, err := server.Read(buf)
		require.NoError(err, "the busy connection must not hit its read deadline")
		deadline.Refresh()
		time.Sleep(timeout / 10)
	}
}

// A connection that stays silent is torn down once the idle timeout expires.
func TestIdleReadDeadlineExpiresOnIdleConnection(t *testing.T) {
	require := require.New(t)

	const timeout = 200 * time.Millisecond

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	_ = NewIdleReadDeadline(server, timeout)

	buf := make([]byte, 1)
	_, err := server.Read(buf)
	require.Error(err)
	require.ErrorIs(err, os.ErrDeadlineExceeded)
}

// Refresh only moves the deadline once at least a quarter of the timeout has elapsed,
// so the number of runtime poller updates stays proportional to the timeout
// instead of to the message rate, while safely tolerating jitter around the half-window.
func TestIdleReadDeadlineRefreshIsLazy(t *testing.T) {
	require := require.New(t)

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	deadline := NewIdleReadDeadline(server, 60*time.Millisecond)
	require.False(deadline.Refresh(), "a fresh deadline must not be moved")

	time.Sleep(20 * time.Millisecond)
	require.True(deadline.Refresh(), "a deadline past the quarter-window must be moved")

	deadline.Clear()
}

// A connection receiving periodic heartbeats at the half-window cadence (timeout/2)
// with jitter must never hit its read deadline.
func TestIdleReadDeadlineHeartbeatHalfWindowWithJitter(t *testing.T) {
	require := require.New(t)

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	const timeout = 60 * time.Millisecond
	deadline := NewIdleReadDeadline(server, timeout)
	defer deadline.Clear()

	// Simulate heartbeats at nominal timeout/2 (30ms), with jitter (+/- 3ms)
	// Over 10 iterations, the deadline must never be exceeded.
	jitterDeltas := []time.Duration{
		31 * time.Millisecond,
		29 * time.Millisecond,
		32 * time.Millisecond,
		28 * time.Millisecond,
		30 * time.Millisecond,
		31 * time.Millisecond,
		29 * time.Millisecond,
		30 * time.Millisecond,
		32 * time.Millisecond,
		29 * time.Millisecond,
	}

	for i, delta := range jitterDeltas {
		time.Sleep(delta)
		moved := deadline.Refresh()
		require.True(moved, "heartbeat at iteration %d should refresh deadline", i)
	}
}
