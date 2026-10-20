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
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

// NewBandwidthLimiter creates a limiter whose rate preserves the configured
// byte limit while keeping the burst representable as an int on all targets.
func NewBandwidthLimiter(bytes int64) *rate.Limiter {
	if bytes <= 0 {
		return nil
	}

	maxInt := int64(^uint(0) >> 1)
	burst := min(bytes, maxInt)
	return rate.NewLimiter(rate.Limit(float64(bytes)), int(burst))
}

func invalidBurstError(burst int) error {
	return fmt.Errorf("invalid limiter burst: %d", burst)
}

// WaitN charges n bytes against the limiter and blocks until the tokens are
// available. A nil limiter means "no limit" and is a no-op.
//
// rate.Limiter.WaitN rejects a request that is larger than the burst, so the
// charge is split into burst-sized parts. This mirrors what limit.Writer does
// for payloads that are larger than the burst, and it keeps a single large
// datagram from bypassing the limit completely.
func WaitN(ctx context.Context, limiter *rate.Limiter, n int) error {
	if limiter == nil || n <= 0 {
		return nil
	}
	burst := limiter.Burst()
	if burst <= 0 {
		return invalidBurstError(burst)
	}
	for n > 0 {
		part := min(n, burst)
		if err := limiter.WaitN(ctx, part); err != nil {
			return err
		}
		n -= part
	}
	return nil
}

// AllowN charges n bytes against the limiter without blocking and reports
// whether the charge was allowed. A nil limiter means "no limit" and always
// allows the charge.
//
// A charge that is larger than the burst is rejected by the limiter no matter
// how many tokens are available, so a packet bigger than the burst would be
// dropped every single time. Such a packet is charged one burst instead: it
// is then admitted at the rate of a burst-sized packet instead of being
// rejected forever.
func AllowN(limiter *rate.Limiter, now time.Time, n int) bool {
	if limiter == nil || n <= 0 {
		return true
	}
	burst := limiter.Burst()
	if burst <= 0 {
		return false
	}
	if n > burst {
		n = burst
	}
	return limiter.AllowN(now, n)
}
