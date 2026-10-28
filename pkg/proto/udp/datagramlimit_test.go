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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// The relay header and the user address are part of every frame, so the
// largest UDP payload that fits into the datagram lane is smaller than the
// frame budget. The reported limit must match the encoder exactly: a
// payload of exactly the limit is relayed as a datagram, one byte more is
// not.
func TestMaxDatagramPayloadSizeMatchesTheEncoderBoundary(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 40000}
	limit := MaxDatagramPayloadSize("game", addr)
	require.Positive(t, limit)

	frame, err := encodeDgramFrame("game", addr, make([]byte, limit))
	require.NoError(t, err)
	require.Equal(t, DatagramFrameBudget, len(frame))

	_, err = encodeDgramFrame("game", addr, make([]byte, limit+1))
	require.ErrorIs(t, err, ErrDatagramTooLarge)
}

// A longer proxy name or a longer user address leaves less room for the
// payload, so the limit has to shrink with both of them.
func TestMaxDatagramPayloadSizeAccountsForNameAndAddress(t *testing.T) {
	shortAddr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 80}
	longAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 40000}

	shortName := MaxDatagramPayloadSize("p", shortAddr)
	longName := MaxDatagramPayloadSize("proxy-with-a-longer-name", shortAddr)
	require.Less(t, longName, shortName)

	shortAddress := MaxDatagramPayloadSize("p", shortAddr)
	longAddress := MaxDatagramPayloadSize("p", longAddr)
	require.Less(t, longAddress, shortAddress)

	// The address is written as a string, so an IPv6 address costs more
	// room than an IPv4 address of the same port.
	ipv6Addr := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 40000}
	require.Less(t, MaxDatagramPayloadSize("p", ipv6Addr), longAddress)
}

// The error has to carry the payload size and the limit, so the caller can
// report the limit that was reached instead of only reporting that the
// packet was too large.
func TestDatagramTooLargeErrorCarriesTheLimit(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 40000}
	limit := MaxDatagramPayloadSize("game", addr)
	payload := make([]byte, limit+1)

	_, err := encodeDgramFrame("game", addr, payload)
	require.ErrorIs(t, err, ErrDatagramTooLarge)
	require.NotErrorIs(t, err, ErrDatagramHeaderTooLong)

	var tooLarge *DatagramTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, limit, tooLarge.Limit)
	require.Equal(t, len(payload), tooLarge.PayloadSize)
	require.Contains(t, tooLarge.Error(), "quic")
}
