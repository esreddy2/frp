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

package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/proto/wire"
)

func TestMsgTypeMapsCoverAllMessageTypes(t *testing.T) {
	require.Len(t, msgTypeMap, len(v2MsgTypeMap))
	require.Len(t, v2MsgConstructors, len(v2MsgTypeMap))
	require.Len(t, v2MsgTypeIDMap, len(v2MsgTypeMap))
	for _, b := range []byte{
		TypeLogin, TypeLoginResp, TypeNewProxy, TypeNewProxyResp, TypeCloseProxy,
		TypeNewWorkConn, TypeReqWorkConn, TypeStartWorkConn, TypeNewVisitorConn,
		TypeNewVisitorConnResp, TypePing, TypePong, TypeUDPPacket, TypeNatHoleVisitor,
		TypeNatHoleClient, TypeNatHoleResp, TypeNatHoleSid, TypeNatHoleReport,
	} {
		_, ok := msgTypeMap[b]
		require.True(t, ok, "missing v1 type %d", b)
	}
	for i := uint16(1); i <= 18; i++ {
		_, ok := v2MsgConstructors[i]
		require.True(t, ok, "missing v2 constructor %d", i)
	}
}

func TestV2MessageFrameRoundTripAllTypes(t *testing.T) {
	cases := []Message{
		&Login{Version: "v1", Hostname: "h", User: "u", PoolCount: 2, Metas: map[string]string{"k": "v"}},
		&LoginResp{Version: "v1", RunID: "r", Error: "e"},
		&NewProxy{ProxyName: "p", ProxyType: "tcp", RemotePort: 80, CustomDomains: []string{"d"}, Metas: map[string]string{"k": "v"}},
		&NewProxyResp{ProxyName: "p", RemoteAddr: "a", Error: "e"},
		&CloseProxy{ProxyName: "p"},
		&NewWorkConn{RunID: "r", Timestamp: 1},
		&ReqWorkConn{},
		&StartWorkConn{ProxyName: "p", SrcAddr: "s", DstPort: 80},
		&NewVisitorConn{ProxyName: "p", UseCompression: true},
		&NewVisitorConnResp{ProxyName: "p", Error: "e"},
		&Ping{PrivilegeKey: "k", Timestamp: 1},
		&Pong{Error: "e"},
		&UDPPacket{Content: []byte("udp"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}},
		&NatHoleVisitor{TransactionID: "t", ProxyName: "p", MappedAddrs: []string{"a"}},
		&NatHoleClient{TransactionID: "t", Sid: "s"},
		&NatHoleResp{TransactionID: "t", Sid: "s", Error: "e"},
		&NatHoleSid{TransactionID: "t", Sid: "s", Response: true, Nonce: "n"},
		&NatHoleReport{Sid: "s", Success: true},
	}
	for _, in := range cases {
		t.Run(fmt.Sprintf("%T", in), func(t *testing.T) {
			frame, err := EncodeV2MessageFrame(in)
			require.NoError(t, err)
			require.Equal(t, wire.FrameTypeMessage, frame.Type)
			out, err := DecodeV2MessageFrame(frame)
			require.NoError(t, err)
			b1, _ := json.Marshal(in)
			b2, _ := json.Marshal(out)
			require.JSONEq(t, string(b1), string(b2))
		})
	}
}

func TestV2EncodeDecodeErrors(t *testing.T) {
	_, err := EncodeV2MessageFrame(nil)
	require.ErrorContains(t, err, "nil message")
	_, err = EncodeV2MessageFrame(struct{ Foo int }{})
	require.ErrorContains(t, err, "unknown v2 message type")

	frame := &wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0, 1}}
	_, err = DecodeV2MessageFrame(frame)
	require.Error(t, err)

	badType := &wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0xff, 0xff}}
	_, err = DecodeV2MessageFrame(badType)
	require.ErrorContains(t, err, "unknown v2 message type")

	wrongType := &wire.Frame{Type: 999, Payload: []byte{0, 1, '{', '}'}}
	_, err = DecodeV2MessageFrame(wrongType)
	require.ErrorContains(t, err, "unexpected frame type")

	// DecodeV2MessageFrameInto errors
	err = DecodeV2MessageFrameInto(wrongType, &Login{})
	require.ErrorContains(t, err, "unexpected frame type")
	err = DecodeV2MessageFrameInto(&wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0, 1}}, &Login{})
	require.Error(t, err)
	// non-pointer
	err = DecodeV2MessageFrameInto(&wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0, 1, '{', '}'}}, Login{})
	require.ErrorContains(t, err, "must be a pointer")
	// unknown type id
	err = DecodeV2MessageFrameInto(&wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0, 99}}, &Login{})
	require.ErrorContains(t, err, "unknown v2 message type")
	// mismatched type
	frame2, _ := EncodeV2MessageFrame(&Ping{})
	err = DecodeV2MessageFrameInto(frame2, &Login{})
	require.ErrorContains(t, err, "unexpected message type")
	// invalid json
	badJSON := &wire.Frame{Type: wire.FrameTypeMessage, Payload: append([]byte{0, byte(V2TypeLogin)}, []byte("{bad")...)}
	err = DecodeV2MessageFrameInto(badJSON, &Login{})
	require.Error(t, err)
}

func TestV2ReadWriterRoundTripMore(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV2ReadWriter(&buf)
	in := &Login{Version: "v", User: "u"}
	require.NoError(t, rw.WriteMsg(in))
	out, err := rw.ReadMsg()
	require.NoError(t, err)
	require.IsType(t, &Login{}, out)
	l := out.(*Login)
	require.Equal(t, in.Version, l.Version)
	require.Equal(t, in.User, l.User)

	// ReadMsgInto
	var buf2 bytes.Buffer
	rw2 := NewV2ReadWriter(&buf2)
	require.NoError(t, rw2.WriteMsg(in))
	var target Login
	require.NoError(t, rw2.ReadMsgInto(&target))
	require.Equal(t, in.Version, target.Version)
}

func TestV1ReadWriterRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV1ReadWriter(&buf)
	in := &Ping{Timestamp: 42}
	require.NoError(t, rw.WriteMsg(in))
	out, err := rw.ReadMsg()
	require.NoError(t, err)
	require.IsType(t, &Ping{}, out)
	require.Equal(t, int64(42), out.(*Ping).Timestamp)

	var buf2 bytes.Buffer
	rw2 := NewV1ReadWriter(&buf2)
	require.NoError(t, rw2.WriteMsg(in))
	var target Ping
	require.NoError(t, rw2.ReadMsgInto(&target))
	require.Equal(t, int64(42), target.Timestamp)
}

func TestNewReadWriterMore(t *testing.T) {
	require.IsType(t, &V1ReadWriter{}, NewReadWriter(&bytes.Buffer{}, ""))
	require.IsType(t, &V1ReadWriter{}, NewReadWriter(&bytes.Buffer{}, wire.ProtocolV1))
	require.IsType(t, &V1ReadWriter{}, NewReadWriter(&bytes.Buffer{}, "unknown"))
	require.IsType(t, &V2ReadWriter{}, NewReadWriter(&bytes.Buffer{}, wire.ProtocolV2))
}

func TestNewV2ReadWriterWithConn(t *testing.T) {
	var buf bytes.Buffer
	conn := wire.NewConn(&buf)
	rw := NewV2ReadWriterWithConn(conn)
	require.Same(t, conn, rw.WireConn())
}

func TestV2ReadWriterReadErrors(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV2ReadWriter(&buf)
	_, err := rw.ReadMsg()
	require.Error(t, err)
	var target Login
	err = rw.ReadMsgInto(&target)
	require.Error(t, err)
}

func TestEncodeV2FrameBinaryBigEndian(t *testing.T) {
	frame, err := EncodeV2MessageFrame(&Ping{})
	require.NoError(t, err)
	require.True(t, len(frame.Payload) >= 2)
	require.Equal(t, V2TypePing, binary.BigEndian.Uint16(frame.Payload[:2]))
}

func TestReadMsgLegacy(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteMsg(&buf, &Pong{Error: "e"}))
	msg, err := ReadMsg(&buf)
	require.NoError(t, err)
	require.IsType(t, &Pong{}, msg)
}

func TestReadMsgIntoLegacy(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteMsg(&buf, &Pong{Error: "e"}))
	var target Pong
	require.NoError(t, ReadMsgInto(&buf, &target))
	require.Equal(t, "e", target.Error)
}

func TestV2BinaryUDPPacketDecodeErrors(t *testing.T) {
	_, err := DecodeUDPPacketBinary([]byte{0, 0})
	require.ErrorContains(t, err, "too short")
	badFlags := []byte{0x80, 4, 203, 0, 113, 9, 0xd4, 0x31, 0, 0, 1}
	_, err = DecodeUDPPacketBinary(badFlags)
	require.ErrorContains(t, err, "reserved")
}

func TestPortsRangeAndNatHoleDetectBehavior(t *testing.T) {
	b, err := json.Marshal(NatHoleResp{DetectBehavior: NatHoleDetectBehavior{Role: "sender", CandidatePorts: []PortsRange{{From: 1, To: 2}}}})
	require.NoError(t, err)
	var out NatHoleResp
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, "sender", out.DetectBehavior.Role)
	require.Len(t, out.DetectBehavior.CandidatePorts, 1)
}

func TestUDPPacketJSONRoundTrip(t *testing.T) {
	in := &UDPPacket{
		Content:    []byte("x"),
		LocalAddr:  &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9},
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("127.0.0.2"), Port: 10},
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out UDPPacket
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in.Content, out.Content)
	require.Equal(t, in.LocalAddr.String(), out.LocalAddr.String())
}

func TestMsgTypeMapRegistration(t *testing.T) {
	require.Contains(t, msgTypeMap, TypeLogin)
	require.Contains(t, v2MsgTypeIDMap, reflect.TypeFor[Login]())
}
