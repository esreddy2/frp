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

package udp

import (
	"fmt"
	"sync"
	"time"
)

// DatagramSizeReportInterval is the minimum time between two reports of
// packets that do not fit into the datagram lane.
const DatagramSizeReportInterval = time.Minute

// SizeReporter throttles the reports of packets that do not fit into the
// datagram lane.
//
// A proxy that sends oversized packets continuously would otherwise write one
// log line per packet, and the log would hide the information the operator
// needs. The reporter emits the first event and then at most one event per
// interval, together with the number of events that were suppressed in
// between.
type SizeReporter struct {
	mu       sync.Mutex
	interval time.Duration
	reported bool
	last     time.Time
	pending  int
	now      func() time.Time
}

// NewSizeReporter returns a reporter that emits at most one event per
// interval.
func NewSizeReporter(interval time.Duration) *SizeReporter {
	return &SizeReporter{
		interval: interval,
		now:      time.Now,
	}
}

// ShouldReport reports whether an event should be logged now. The first
// event is always reported. The returned suffix mentions the number of
// events that were suppressed since the last report, and is empty when there
// were none.
//
// A nil reporter reports every event.
func (r *SizeReporter) ShouldReport() (bool, string) {
	if r == nil {
		return true, ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	if r.reported && now.Sub(r.last) < r.interval {
		r.pending++
		return false, ""
	}

	suppressed := r.pending
	r.pending = 0
	r.reported = true
	r.last = now
	return true, suppressedSuffix(suppressed)
}

func suppressedSuffix(suppressed int) string {
	if suppressed <= 0 {
		return ""
	}
	return fmt.Sprintf(" (%d more packets since the last report)", suppressed)
}
