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
	"time"
)

// IdleReadDeadline keeps the read deadline of a connection as an idle
// timeout: the connection is torn down only when it stays silent for the
// whole timeout, not when it is busy.
//
// A read deadline is absolute, so arming it once would expire it even on a
// connection that keeps receiving data. Re-arming it on every message would
// restore the idle semantics, but every SetReadDeadline mutates the runtime
// poller, which is expensive on a high rate path. The deadline is therefore
// pushed forward only once it is at least half way to its expiry: the number
// of poller updates stays proportional to the timeout instead of to the
// message rate, and the effective idle window stays between half the timeout
// and the full timeout.
type IdleReadDeadline struct {
	conn    net.Conn
	timeout time.Duration
	current time.Time
}

// NewIdleReadDeadline arms the read deadline of conn with the given idle
// timeout.
func NewIdleReadDeadline(conn net.Conn, timeout time.Duration) *IdleReadDeadline {
	d := &IdleReadDeadline{
		conn:    conn,
		timeout: timeout,
	}
	d.current = time.Now().Add(timeout)
	_ = conn.SetReadDeadline(d.current)
	return d
}

// Refresh pushes the read deadline forward when it is at least half way to
// its expiry. It reports whether the deadline was moved.
func (d *IdleReadDeadline) Refresh() bool {
	now := time.Now()
	// Push the deadline forward once at least a quarter of the timeout has
	// elapsed (i.e. remaining time is <= 3/4 timeout). This ensures that
	// periodic heartbeats arriving near the half-window (timeout/2) do not
	// get skipped due to sub-millisecond scheduling or network jitter.
	if now.Add(d.timeout * 3 / 4).Before(d.current) {
		return false
	}
	d.current = now.Add(d.timeout)
	_ = d.conn.SetReadDeadline(d.current)
	return true
}

// Clear removes the read deadline.
func (d *IdleReadDeadline) Clear() {
	_ = d.conn.SetReadDeadline(time.Time{})
}
