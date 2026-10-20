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

package limit

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNewBandwidthLimiterClampsBurstToTargetInt(t *testing.T) {
	const bytesPerSecond = int64(1 << 31)

	limiter := NewBandwidthLimiter(bytesPerSecond)
	require.NotNil(t, limiter)

	wantBurst := bytesPerSecond
	maxInt := int64(^uint(0) >> 1)
	if wantBurst > maxInt {
		wantBurst = maxInt
	}
	require.Equal(t, int(wantBurst), limiter.Burst())
	require.Equal(t, rate.Limit(float64(bytesPerSecond)), limiter.Limit())
}

func TestNewBandwidthLimiterDisablesNonPositiveLimit(t *testing.T) {
	require.Nil(t, NewBandwidthLimiter(0))
	require.Nil(t, NewBandwidthLimiter(-1))
}

func TestReaderAndWriterRejectInvalidBurst(t *testing.T) {
	for _, burst := range []int{0, -1} {
		t.Run("reader/"+strconv.Itoa(burst), func(t *testing.T) {
			reader := NewReader(strings.NewReader("payload"), rate.NewLimiter(rate.Limit(1), burst))
			n, err := reader.Read(make([]byte, 1))
			require.Zero(t, n)
			require.EqualError(t, err, "invalid limiter burst: "+strconv.Itoa(burst))
		})

		t.Run("writer/"+strconv.Itoa(burst), func(t *testing.T) {
			var dst bytes.Buffer
			writer := NewWriter(&dst, rate.NewLimiter(rate.Limit(1), burst))
			n, err := writer.Write([]byte("payload"))
			require.Zero(t, n)
			require.EqualError(t, err, "invalid limiter burst: "+strconv.Itoa(burst))
			require.Empty(t, dst.Bytes())
		})
	}
}

// A charge that is bigger than the burst must still be accounted for.
// rate.Limiter.WaitN rejects such a request outright, so without the split
// a single oversized datagram would bypass the limit completely.
func TestWaitNSplitsChargesLargerThanTheBurst(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(100), 100)

	start := time.Now()
	require.NoError(t, WaitN(context.Background(), limiter, 300))
	elapsed := time.Since(start)

	// 300 bytes at 100 bytes per second cannot be free: the first burst
	// is, the remaining 200 bytes have to wait for about two seconds.
	require.Greater(t, elapsed, 1500*time.Millisecond)
}

func TestWaitNIsANoOpWithoutLimiter(t *testing.T) {
	require.NoError(t, WaitN(context.Background(), nil, 1<<20))
	require.NoError(t, WaitN(context.Background(), nil, 0))
	require.NoError(t, WaitN(context.Background(), nil, -1))
}

// A canceled context must stop the wait instead of blocking forever.
func TestWaitNStopsOnACanceledContext(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.Error(t, WaitN(ctx, limiter, 10))
}

// A packet that is bigger than the burst must not be rejected every single
// time: it is charged one burst instead, so it is admitted at the rate of a
// burst-sized packet.
func TestAllowNChargesOversizedPacketsAsOneBurst(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(100), 100)
	now := time.Now()

	require.True(t, AllowN(limiter, now, 300), "an oversized packet must not be rejected outright")
	// The burst is used up, so the next packet has to wait.
	require.False(t, AllowN(limiter, now, 10))
}

func TestAllowNIsANoOpWithoutLimiter(t *testing.T) {
	require.True(t, AllowN(nil, time.Now(), 1<<20))
	require.True(t, AllowN(nil, time.Now(), 0))
	require.True(t, AllowN(nil, time.Now(), -1))
}
