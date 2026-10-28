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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A proxy that sends oversized packets continuously would write one log line
// per packet. The reporter emits the first event, suppresses the events that
// follow within the interval, and reports the number of suppressed events
// with the next one.
func TestSizeReporterThrottlesTheReports(t *testing.T) {
	now := time.Now()
	reporter := NewSizeReporter(time.Minute)
	reporter.now = func() time.Time { return now }

	report, suffix := reporter.ShouldReport()
	require.True(t, report)
	require.Empty(t, suffix)

	for range 3 {
		report, suffix = reporter.ShouldReport()
		require.False(t, report)
		require.Empty(t, suffix)
	}

	now = now.Add(time.Minute)
	report, suffix = reporter.ShouldReport()
	require.True(t, report)
	require.Contains(t, suffix, "3")
}

// A reporter that is not set up must not lose the report: the caller has to
// be able to log the event even when no throttle is configured.
func TestNilSizeReporterReportsEveryEvent(t *testing.T) {
	var reporter *SizeReporter

	report, suffix := reporter.ShouldReport()
	require.True(t, report)
	require.Empty(t, suffix)
}
