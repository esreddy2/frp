// Copyright 2025 The frp Authors
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

package msg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// MaxQuicDatagramSize is the maximum QUIC datagram payload size.
// quic-go default max_datagram_frame_size is ~1200 bytes (fits in min QUIC packet).
// We use a conservative limit to account for framing overhead.
const MaxQuicDatagramSize = 1200

// QuicDatagramPacket represents a UDP packet sent over QUIC datagrams.
// Format: [proxyNameLen:1][proxyName][localAddrLen:2][localAddr][remoteAddrLen:2][remoteAddr][contentLen:4][content]
// For localAddr/remoteAddr, we serialize as "ip:port" strings.

func EncodeQuicDatagramPacket(p *UDPPacket, proxyName string) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("nil UDP packet")
	}
	if p.RemoteAddr == nil {
		return nil, fmt.Errorf("missing remote address")
	}

	proxyNameBytes := []byte(proxyName)
	if len(proxyNameBytes) > 255 {
		return nil, ErrProxyNameTooLong
	}

	var localAddrStr, remoteAddrStr string
	if p.LocalAddr != nil {
		localAddrStr = p.LocalAddr.String()
	}
	if p.RemoteAddr != nil {
		remoteAddrStr = p.RemoteAddr.String()
	}

	localAddrBytes := []byte(localAddrStr)
	remoteAddrBytes := []byte(remoteAddrStr)
	contentBytes := p.Content

	// Validate address lengths fit in uint16
	if len(localAddrBytes) > 65535 {
		return nil, fmt.Errorf("local address too long: %d bytes", len(localAddrBytes))
	}
	if len(remoteAddrBytes) > 65535 {
		return nil, fmt.Errorf("remote address too long: %d bytes", len(remoteAddrBytes))
	}

	// Validate content length
	if len(contentBytes) > MaxUDPPayloadSize {
		return nil, fmt.Errorf("content too large: %d bytes (max %d)", len(contentBytes), MaxUDPPayloadSize)
	}

	// Calculate total size with overflow checks
	totalSize := 1 + len(proxyNameBytes) +
		2 + len(localAddrBytes) +
		2 + len(remoteAddrBytes) +
		4 + len(contentBytes)

	// Validate against QUIC datagram size limit
	if totalSize > MaxQuicDatagramSize {
		return nil, fmt.Errorf("encoded datagram too large: %d bytes (max %d)", totalSize, MaxQuicDatagramSize)
	}

	buf := make([]byte, totalSize)
	offset := 0

	buf[offset] = byte(len(proxyNameBytes))
	offset++
	copy(buf[offset:], proxyNameBytes)
	offset += len(proxyNameBytes)

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(localAddrBytes)))
	offset += 2
	copy(buf[offset:], localAddrBytes)
	offset += len(localAddrBytes)

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(remoteAddrBytes)))
	offset += 2
	copy(buf[offset:], remoteAddrBytes)
	offset += len(remoteAddrBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(contentBytes)))
	offset += 4
	copy(buf[offset:], contentBytes)

	return buf, nil
}

func DecodeQuicDatagramPacket(data []byte) (*UDPPacket, string, error) {
	if len(data) < 1 {
		return nil, "", ErrDatagramTooShort
	}

	offset := 0
	proxyNameLen := int(data[offset])
	offset++
	if len(data) < offset+proxyNameLen {
		return nil, "", ErrDatagramTooShort
	}
	proxyName := string(data[offset : offset+proxyNameLen])
	offset += proxyNameLen

	if len(data) < offset+2 {
		return nil, "", ErrDatagramTooShort
	}
	localAddrLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if len(data) < offset+localAddrLen {
		return nil, "", ErrDatagramTooShort
	}
	localAddrStr := string(data[offset : offset+localAddrLen])
	offset += localAddrLen

	if len(data) < offset+2 {
		return nil, "", ErrDatagramTooShort
	}
	remoteAddrLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if len(data) < offset+remoteAddrLen {
		return nil, "", ErrDatagramTooShort
	}
	remoteAddrStr := string(data[offset : offset+remoteAddrLen])
	offset += remoteAddrLen

	if len(data) < offset+4 {
		return nil, "", ErrDatagramTooShort
	}
	contentLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+contentLen {
		return nil, "", ErrDatagramTooShort
	}

	// Check for trailing data
	if offset+contentLen != len(data) {
		return nil, "", fmt.Errorf("trailing data after content: %d bytes", len(data)-offset-contentLen)
	}

	// Copy content to avoid aliasing input buffer
	content := make([]byte, contentLen)
	copy(content, data[offset:offset+contentLen])

	var localAddr, remoteAddr *net.UDPAddr
	if localAddrStr != "" {
		addr, err := net.ResolveUDPAddr("udp", localAddrStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid local address %q: %w", localAddrStr, err)
		}
		localAddr = addr
	}
	if remoteAddrStr != "" {
		addr, err := net.ResolveUDPAddr("udp", remoteAddrStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid remote address %q: %w", remoteAddrStr, err)
		}
		remoteAddr = addr
	}

	return &UDPPacket{
		Content:    content,
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}, proxyName, nil
}

var (
	ErrProxyNameTooLong = errors.New("proxy name too long (max 255 bytes)")
	ErrDatagramTooShort = errors.New("datagram too short")
)
