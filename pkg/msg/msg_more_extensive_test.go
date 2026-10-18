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
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/proto/wire"
)

func TestV2WireFrameExactLayout(t *testing.T) {
	in := &Login{Version: "v1", User: "alice", PoolCount: 3}
	frame, err := EncodeV2MessageFrame(in)
	require.NoError(t, err)
	require.Equal(t, wire.FrameTypeMessage, frame.Type)
	require.Equal(t, V2TypeLogin, binary.BigEndian.Uint16(frame.Payload[:2]))
	var payload map[string]any
	require.NoError(t, json.Unmarshal(frame.Payload[2:], &payload))
	require.Equal(t, "v1", payload["version"])
	require.Equal(t, "alice", payload["user"])
	require.Equal(t, float64(3), payload["pool_count"])
}

func TestV2RoundTripEveryMessageTypeAllFields(t *testing.T) {
	cases := []Message{
		&Login{
			Version:      "1.2.3",
			Hostname:     "host",
			Os:           "linux",
			Arch:         "amd64",
			User:         "alice",
			PrivilegeKey: "key",
			Timestamp:    1700000000,
			RunID:        "run-1",
			ClientID:     "client-1",
			Metas:        map[string]string{"k": "v"},
			ClientSpec:   ClientSpec{Type: "ssh-tunnel", AlwaysAuthPass: true},
			PoolCount:    5,
		},
		&LoginResp{Version: "1.2.3", RunID: "run-1", Error: "err"},
		&NewProxy{
			ProxyName:          "p",
			ProxyType:          "tcp",
			UseEncryption:      true,
			UseCompression:     true,
			BandwidthLimit:     "10Mbps",
			BandwidthLimitMode: "tcp",
			Group:              "g",
			GroupKey:           "gk",
			Metas:              map[string]string{"m": "1"},
			Annotations:        map[string]string{"a": "1"},
			RemotePort:         8080,
			CustomDomains:      []string{"d.example.com"},
			SubDomain:          "sub",
			Locations:          []string{"/"},
			HTTPUser:           "u",
			HTTPPwd:            "p",
			HostHeaderRewrite:  "rewrite",
			Headers:            map[string]string{"h": "1"},
			ResponseHeaders:    map[string]string{"rh": "1"},
			RouteByHTTPUser:    "rbu",
			Sk:                 "sk",
			AllowUsers:         []string{"u1"},
			Multiplexer:        "mux",
		},
		&NewProxyResp{ProxyName: "p", RemoteAddr: "1.2.3.4:8080", Error: "e"},
		&CloseProxy{ProxyName: "p"},
		&NewWorkConn{RunID: "r", PrivilegeKey: "k", Timestamp: 1},
		&ReqWorkConn{},
		&StartWorkConn{ProxyName: "p", SrcAddr: "s", DstAddr: "d", SrcPort: 1000, DstPort: 2000, Error: "e"},
		&NewVisitorConn{RunID: "r", ProxyName: "p", SignKey: "s", Timestamp: 1, UseEncryption: true, UseCompression: true},
		&NewVisitorConnResp{ProxyName: "p", Error: "e"},
		&Ping{PrivilegeKey: "k", Timestamp: 2},
		&Pong{Error: "e"},
		&UDPPacket{
			Content:    []byte("hello"),
			LocalAddr:  &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1000, Zone: "en0"},
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321},
		},
		&NatHoleVisitor{
			TransactionID: "t", ProxyName: "p", PreCheck: true, Protocol: "tcp",
			SignKey: "s", Timestamp: 3, MappedAddrs: []string{"a1"}, AssistedAddrs: []string{"a2"},
		},
		&NatHoleClient{TransactionID: "t", ProxyName: "p", Sid: "sid", MappedAddrs: []string{"a1"}, AssistedAddrs: []string{"a2"}},
		&NatHoleResp{
			TransactionID:  "t",
			Sid:            "sid",
			Protocol:       "tcp",
			CandidateAddrs: []string{"c1"},
			AssistedAddrs:  []string{"a2"},
			DetectBehavior: NatHoleDetectBehavior{
				Role: "sender", Mode: 1, TTL: 10, SendDelayMs: 5, ReadTimeoutMs: 100,
				CandidatePorts:  []PortsRange{{From: 1, To: 2}},
				SendRandomPorts: 3, ListenRandomPorts: 4,
			},
		},
		&NatHoleSid{TransactionID: "t", Sid: "sid", Response: true, Nonce: "n"},
		&NatHoleReport{Sid: "sid", Success: true},
	}
	for _, in := range cases {
		t.Run(reflect.TypeOf(in).Elem().Name(), func(t *testing.T) {
			frame, err := EncodeV2MessageFrame(in)
			require.NoError(t, err)
			out, err := DecodeV2MessageFrame(frame)
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
}

func TestV2RoundTripPointerAndNonPointer(t *testing.T) {
	for _, m := range []Message{Login{Version: "v"}, &Login{Version: "v"}} {
		frame, err := EncodeV2MessageFrame(m)
		require.NoError(t, err)
		out, err := DecodeV2MessageFrame(frame)
		require.NoError(t, err)
		require.Equal(t, &Login{Version: "v"}, out)
	}
}

func TestV2EncodeRejectsNilAndUnknown(t *testing.T) {
	_, err := EncodeV2MessageFrame(nil)
	require.ErrorContains(t, err, "nil message")
	_, err = EncodeV2MessageFrame(&struct{ Foo int }{})
	require.ErrorContains(t, err, "unknown v2 message type")
	_, err = EncodeV2MessageFrame(struct{ Foo int }{})
	require.ErrorContains(t, err, "unknown v2 message type")
}

func TestV2DecodeRejectsInvalidFrames(t *testing.T) {
	_, err := DecodeV2MessageFrame(&wire.Frame{Type: wire.FrameTypeClientHello})
	require.ErrorContains(t, err, "unexpected frame type")

	_, err = DecodeV2MessageFrame(&wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0}})
	require.ErrorContains(t, err, "payload too short")

	_, err = DecodeV2MessageFrame(&wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0xff, 0xff}})
	require.ErrorContains(t, err, "unknown v2 message type")

	payload := make([]byte, 4)
	binary.BigEndian.PutUint16(payload[:2], V2TypeLogin)
	copy(payload[2:], "{not-json")
	_, err = DecodeV2MessageFrame(&wire.Frame{Type: wire.FrameTypeMessage, Payload: payload})
	require.Error(t, err)
}

func TestDecodeV2MessageFrameIntoRejectsInvalidTargets(t *testing.T) {
	frame, err := EncodeV2MessageFrame(&Login{Version: "v"})
	require.NoError(t, err)

	err = DecodeV2MessageFrameInto(frame, Login{})
	require.ErrorContains(t, err, "must be a pointer")

	err = DecodeV2MessageFrameInto(frame, &struct{}{})
	require.ErrorContains(t, err, "unknown v2 message type")

	err = DecodeV2MessageFrameInto(frame, &Ping{})
	require.ErrorContains(t, err, "unexpected message type")

	badJSON := &wire.Frame{Type: wire.FrameTypeMessage, Payload: append([]byte{0, byte(V2TypeLogin)}, []byte("{bad")...)}
	err = DecodeV2MessageFrameInto(badJSON, &Login{})
	require.Error(t, err)

	short := &wire.Frame{Type: wire.FrameTypeMessage, Payload: []byte{0}}
	err = DecodeV2MessageFrameInto(short, &Login{})
	require.ErrorContains(t, err, "payload too short")

	wrongFrame := &wire.Frame{Type: wire.FrameTypeClientHello, Payload: []byte{0, 1}}
	err = DecodeV2MessageFrameInto(wrongFrame, &Login{})
	require.ErrorContains(t, err, "unexpected frame type")
}

func TestDecodeV2MessageFrameIntoValidTargets(t *testing.T) {
	frame, err := EncodeV2MessageFrame(&StartWorkConn{ProxyName: "tcp", SrcAddr: "127.0.0.1", SrcPort: 1234})
	require.NoError(t, err)

	var out StartWorkConn
	require.NoError(t, DecodeV2MessageFrameInto(frame, &out))
	require.Equal(t, StartWorkConn{ProxyName: "tcp", SrcAddr: "127.0.0.1", SrcPort: 1234}, out)

	var outPtr *Login
	_ = outPtr
	frame2, err := EncodeV2MessageFrame(&Login{Version: "v"})
	require.NoError(t, err)
	var target Login
	require.NoError(t, DecodeV2MessageFrameInto(frame2, &target))
	require.Equal(t, "v", target.Version)
}

func TestV1RoundTripEveryMessageType(t *testing.T) {
	cases := []Message{
		&Login{Version: "v", User: "u", PoolCount: 1},
		&LoginResp{RunID: "r"},
		&NewProxy{ProxyName: "p"},
		&NewProxyResp{ProxyName: "p"},
		&CloseProxy{ProxyName: "p"},
		&NewWorkConn{RunID: "r"},
		&ReqWorkConn{},
		&StartWorkConn{ProxyName: "p"},
		&NewVisitorConn{ProxyName: "p"},
		&NewVisitorConnResp{ProxyName: "p"},
		&Ping{Timestamp: 1},
		&Pong{},
		&UDPPacket{Content: []byte("x"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}},
		&NatHoleVisitor{TransactionID: "t"},
		&NatHoleClient{TransactionID: "t"},
		&NatHoleResp{TransactionID: "t"},
		&NatHoleSid{TransactionID: "t"},
		&NatHoleReport{},
	}
	for _, in := range cases {
		t.Run(reflect.TypeOf(in).Elem().Name(), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, WriteMsg(&buf, in))
			out, err := ReadMsg(&buf)
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
}

func TestV1ReadWriterAllTypes(t *testing.T) {
	for _, in := range []Message{&Ping{Timestamp: 42}, &Pong{Error: "e"}, &Login{User: "u"}} {
		t.Run(reflect.TypeOf(in).Elem().Name(), func(t *testing.T) {
			var buf bytes.Buffer
			rw := NewV1ReadWriter(&buf)
			require.NoError(t, rw.WriteMsg(in))
			out, err := rw.ReadMsg()
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
}

func TestV1ReadWriterReadErrors(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV1ReadWriter(&buf)
	_, err := rw.ReadMsg()
	require.Error(t, err)
	var target Login
	err = rw.ReadMsgInto(&target)
	require.Error(t, err)
}

func TestMessageTypeByteAllTypes(t *testing.T) {
	require.Equal(t, TypeLogin, messageTypeByte(&Login{}))
	require.Equal(t, TypeLoginResp, messageTypeByte(&LoginResp{}))
	require.Equal(t, TypeNewProxy, messageTypeByte(&NewProxy{}))
	require.Equal(t, TypeNewProxyResp, messageTypeByte(&NewProxyResp{}))
	require.Equal(t, TypeCloseProxy, messageTypeByte(&CloseProxy{}))
	require.Equal(t, TypeNewWorkConn, messageTypeByte(&NewWorkConn{}))
	require.Equal(t, TypeReqWorkConn, messageTypeByte(&ReqWorkConn{}))
	require.Equal(t, TypeStartWorkConn, messageTypeByte(&StartWorkConn{}))
	require.Equal(t, TypeNewVisitorConn, messageTypeByte(&NewVisitorConn{}))
	require.Equal(t, TypeNewVisitorConnResp, messageTypeByte(&NewVisitorConnResp{}))
	require.Equal(t, TypePing, messageTypeByte(&Ping{}))
	require.Equal(t, TypePong, messageTypeByte(&Pong{}))
	require.Equal(t, TypeUDPPacket, messageTypeByte(&UDPPacket{}))
	require.Equal(t, TypeNatHoleVisitor, messageTypeByte(&NatHoleVisitor{}))
	require.Equal(t, TypeNatHoleClient, messageTypeByte(&NatHoleClient{}))
	require.Equal(t, TypeNatHoleResp, messageTypeByte(&NatHoleResp{}))
	require.Equal(t, TypeNatHoleSid, messageTypeByte(&NatHoleSid{}))
	require.Equal(t, TypeNatHoleReport, messageTypeByte(&NatHoleReport{}))
	require.Equal(t, byte(0), messageTypeByte(&struct{}{}))
	require.Equal(t, byte(0), messageTypeByte(struct{}{}))
	require.Equal(t, byte(0), messageTypeByte(nil))
}

func TestDispatcherDispatchesRegisteredHandler(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverConn := NewConn(server, NewV2ReadWriter(server))
	d := NewDispatcher(serverConn)
	received := make(chan Message, 1)
	d.RegisterHandler(&Ping{}, func(m Message) {
		received <- m
	})
	go d.Run()

	clientConn := NewConn(client, NewV2ReadWriter(client))
	require.NoError(t, clientConn.WriteMsg(&Ping{Timestamp: 9}))

	select {
	case m := <-received:
		require.Equal(t, int64(9), m.(*Ping).Timestamp)
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not invoked")
	}

	d.stop()
	select {
	case <-d.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed")
	}
}

func TestDispatcherSendAfterStopReturnsEOF(t *testing.T) {
	var buf bytes.Buffer
	d := NewDispatcher(NewV2ReadWriter(&buf))
	d.stop()
	err := d.Send(&Ping{})
	require.ErrorIs(t, err, io.EOF)
}

func TestDispatcherMultipleHandlers(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverConn := NewConn(server, NewV2ReadWriter(server))
	d := NewDispatcher(serverConn)
	gotPing := make(chan struct{}, 1)
	gotPong := make(chan struct{}, 1)
	d.RegisterHandler(&Ping{}, func(Message) { gotPing <- struct{}{} })
	d.RegisterHandler(&Pong{}, func(Message) { gotPong <- struct{}{} })
	go d.Run()

	clientConn := NewConn(client, NewV2ReadWriter(client))
	require.NoError(t, clientConn.WriteMsg(&Ping{}))
	require.NoError(t, clientConn.WriteMsg(&Pong{}))

	select {
	case <-gotPing:
	case <-time.After(2 * time.Second):
		t.Fatal("ping handler not invoked")
	}
	select {
	case <-gotPong:
	case <-time.After(2 * time.Second):
		t.Fatal("pong handler not invoked")
	}
	d.stop()
}

func TestAsyncHandlerRunsInGoroutine(t *testing.T) {
	started := make(chan struct{})
	done := make(chan struct{})
	h := AsyncHandler(func(Message) {
		close(started)
		<-done
	})
	go h(&Ping{})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("async handler did not start")
	}
	close(done)
}

func TestConnContextGetterSetter(t *testing.T) {
	client, _ := net.Pipe()
	defer client.Close()

	conn := NewConn(client, NewV2ReadWriter(client))
	require.Equal(t, context.Background(), conn.Context())

	ctx := context.WithValue(context.Background(), contextKey("k"), "v")
	cc := &contextConn{Conn: client, ctx: context.Background()}
	conn2 := NewConn(cc, NewV2ReadWriter(client))
	conn2.WithContext(ctx)
	require.Equal(t, "v", conn2.Context().Value(contextKey("k")))
}

type contextConn struct {
	net.Conn
	ctx context.Context
}

func (c *contextConn) Context() context.Context { return c.ctx }

func (c *contextConn) WithContext(ctx context.Context) { c.ctx = ctx }

func TestV2ReadWriterMixedMessageTypesInOrder(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV2ReadWriter(&buf)
	messages := []Message{
		&Ping{Timestamp: 1},
		&Login{User: "a"},
		&Pong{},
		&StartWorkConn{ProxyName: "p"},
	}
	for _, m := range messages {
		require.NoError(t, rw.WriteMsg(m))
	}
	for _, want := range messages {
		got, err := rw.ReadMsg()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestUDPPacketBinaryRoundTripAllFamilies(t *testing.T) {
	cases := []*UDPPacket{
		{
			Content:    []byte("ipv4-remote"),
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 12345},
		},
		{
			Content:    []byte("ipv4-both"),
			LocalAddr:  &net.UDPAddr{IP: net.ParseIP("192.0.2.2"), Port: 23456},
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 12345},
		},
		{
			Content:    []byte("ipv6-remote"),
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 12345},
		},
		{
			Content:    []byte("ipv6-both-zones"),
			LocalAddr:  &net.UDPAddr{IP: net.ParseIP("2001:db8::2"), Port: 23456, Zone: "en0"},
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 12345, Zone: "en1"},
		},
		{
			Content:    []byte("empty"),
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1},
		},
		{
			Content:    bytes.Repeat([]byte{0xff}, 65507),
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 65535},
		},
	}
	for _, in := range cases {
		t.Run("", func(t *testing.T) {
			body, err := EncodeUDPPacketBinary(in)
			require.NoError(t, err)
			out, err := DecodeUDPPacketBinary(body)
			require.NoError(t, err)
			require.Equal(t, in.Content, out.Content)
			if in.LocalAddr == nil {
				require.Nil(t, out.LocalAddr)
			} else {
				require.Equal(t, in.LocalAddr.String(), out.LocalAddr.String())
			}
			require.Equal(t, in.RemoteAddr.String(), out.RemoteAddr.String())
		})
	}
}

func TestUDPPacketBinaryNoLocalAddrFlag(t *testing.T) {
	in := &UDPPacket{Content: []byte("nolocal"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321}}
	body, err := EncodeUDPPacketBinary(in)
	require.NoError(t, err)
	require.Equal(t, udpPacketFlagRemoteAddr, body[0])
	out, err := DecodeUDPPacketBinary(body)
	require.NoError(t, err)
	require.Nil(t, out.LocalAddr)
	require.Equal(t, in.RemoteAddr.String(), out.RemoteAddr.String())
}

func TestUDPPacketBinaryContentDoesNotAliasInput(t *testing.T) {
	in := &UDPPacket{Content: []byte("payload"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321}}
	body, err := EncodeUDPPacketBinary(in)
	require.NoError(t, err)
	out, err := DecodeUDPPacketBinary(body)
	require.NoError(t, err)
	body[len(body)-1] ^= 0xff
	require.Equal(t, []byte("payload"), out.Content)
}

func TestUDPPacketBinaryDecodeTruncationVariants(t *testing.T) {
	valid, err := EncodeUDPPacketBinary(&UDPPacket{
		Content:    []byte("payload"),
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321},
	})
	require.NoError(t, err)

	// truncated at every position
	for i := 1; i < len(valid); i++ {
		_, err := DecodeUDPPacketBinary(valid[:i])
		require.Error(t, err, "truncated at %d", i)
	}

	// trailing bytes
	_, err = DecodeUDPPacketBinary(append(append([]byte(nil), valid...), 0))
	require.ErrorContains(t, err, "trailing")

	// truncated payload length field
	payloadLenOffset := len(valid) - len("payload") - 2
	truncated := append([]byte(nil), valid[:payloadLenOffset+2]...)
	binary.BigEndian.PutUint16(truncated[payloadLenOffset:payloadLenOffset+2], 1)
	_, err = DecodeUDPPacketBinary(truncated)
	require.ErrorContains(t, err, "truncated UDP payload")

	// inflated payload length
	invalidLen := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(invalidLen[payloadLenOffset:payloadLenOffset+2], 0xffff)
	_, err = DecodeUDPPacketBinary(invalidLen)
	require.ErrorContains(t, err, "payload length")
}

func TestUDPPacketBinaryDecodeMalformedBodies(t *testing.T) {
	_, err := DecodeUDPPacketBinary([]byte{})
	require.ErrorContains(t, err, "too short")
	_, err = DecodeUDPPacketBinary([]byte{0, 0, 0})
	require.ErrorContains(t, err, "missing remote address")
	_, err = DecodeUDPPacketBinary([]byte{0x80, 4, 203, 0, 113, 9, 0xd4, 0x31, 0, 0, 1})
	require.ErrorContains(t, err, "reserved")
	_, err = DecodeUDPPacketBinary([]byte{2, 9, 0, 0})
	require.ErrorContains(t, err, "unknown address family")
	badZone := []byte{2, 4, 203, 0, 113, 9, 0xd4, 0x31, 1, 'z', 0, 0}
	_, err = DecodeUDPPacketBinary(badZone)
	require.ErrorContains(t, err, "IPv4 zone")
	badUTF8 := make([]byte, 0, 24)
	badUTF8 = append(badUTF8, 2, 6)
	badUTF8 = append(badUTF8, make([]byte, 16)...)
	badUTF8 = append(badUTF8, 0, 1, 1, 0xff, 0, 0)
	_, err = DecodeUDPPacketBinary(badUTF8)
	require.ErrorContains(t, err, "UTF-8")
}

func TestUDPPacketBinaryEncodeRejectsInvalidPacketsExtensive(t *testing.T) {
	_, err := EncodeUDPPacketBinary(nil)
	require.ErrorContains(t, err, "nil UDP packet")

	_, err = EncodeUDPPacketBinary(&UDPPacket{})
	require.ErrorContains(t, err, "missing remote address")

	_, err = EncodeUDPPacketBinary(&UDPPacket{
		Content:    make([]byte, MaxUDPPayloadSize+1),
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321},
	})
	require.ErrorContains(t, err, "exceeds limit")

	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1, Zone: "bad"}})
	require.ErrorContains(t, err, "IPv4 zone")

	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1, Zone: string(bytes.Repeat([]byte{'z'}, 256))}})
	require.ErrorContains(t, err, "zone exceeds")

	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1, Zone: string([]byte{0xff})}})
	require.ErrorContains(t, err, "UTF-8")

	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{Port: -1}})
	require.ErrorContains(t, err, "port out of range")
	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{Port: 65536}})
	require.ErrorContains(t, err, "port out of range")
	_, err = EncodeUDPPacketBinary(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.IP{1, 2, 3}}})
	require.ErrorContains(t, err, "invalid IP")

	// content exactly at the limit is accepted
	_, err = EncodeUDPPacketBinary(&UDPPacket{
		Content:    make([]byte, MaxUDPPayloadSize),
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321},
	})
	require.NoError(t, err)
}

func TestUDPPacketBinaryEncodeFramePayloadLimit(t *testing.T) {
	// Both addresses with maximum zones plus large content exceeds the frame payload limit.
	zone255 := string(bytes.Repeat([]byte{'z'}, 255))
	in := &UDPPacket{
		Content:    make([]byte, 65000),
		LocalAddr:  &net.UDPAddr{IP: net.ParseIP("2001:db8::2"), Port: 23456, Zone: zone255},
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 12345, Zone: zone255},
	}
	_, err := EncodeUDPPacketBinary(in)
	require.ErrorContains(t, err, "frame payload length")

	// Just under the limit succeeds.
	in.Content = make([]byte, 64900)
	_, err = EncodeUDPPacketBinary(in)
	require.NoError(t, err)
}

func TestV2BinaryUDPPacketReadWriterReadMsgInto(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV2BinaryUDPPacketReadWriter(&buf)
	in := &UDPPacket{Content: []byte("binary"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321}}
	require.NoError(t, rw.WriteMsg(in))

	var out UDPPacket
	require.NoError(t, rw.ReadMsgInto(&out))
	require.Equal(t, in.Content, out.Content)
	require.Equal(t, in.RemoteAddr.String(), out.RemoteAddr.String())
}

func TestV2BinaryUDPPacketReadWriterMixedMessages(t *testing.T) {
	var buf bytes.Buffer
	rw := NewV2BinaryUDPPacketReadWriter(&buf)
	require.NoError(t, rw.WriteMsg(&UDPPacket{Content: []byte("udp"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321}}))
	require.NoError(t, rw.WriteMsg(&Ping{Timestamp: 7}))

	frameConn := wire.NewConn(&buf)
	frame, err := frameConn.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, V2TypeUDPPacketBinary, binary.BigEndian.Uint16(frame.Payload[:2]))

	frame, err = frameConn.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, V2TypePing, binary.BigEndian.Uint16(frame.Payload[:2]))
}

func TestV2BinaryUDPPacketReadWriterRejectsJSONAfterBinaryNegotiation(t *testing.T) {
	newJSONStream := func() *bytes.Buffer {
		s := &bytes.Buffer{}
		require.NoError(t, NewReadWriter(s, wire.ProtocolV2).WriteMsg(&UDPPacket{
			Content:    []byte("x"),
			RemoteAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321},
		}))
		return s
	}

	stream1 := newJSONStream()
	negotiated1, err := NewUDPPacketReadWriter(stream1, wire.ProtocolV2, wire.UDPPacketCodecBinary)
	require.NoError(t, err)
	_, err = negotiated1.ReadMsg()
	require.ErrorContains(t, err, "JSON UDP packet after binary codec negotiation")

	stream2 := newJSONStream()
	negotiated2, err := NewUDPPacketReadWriter(stream2, wire.ProtocolV2, wire.UDPPacketCodecBinary)
	require.NoError(t, err)
	var target UDPPacket
	err = negotiated2.ReadMsgInto(&target)
	require.ErrorContains(t, err, "JSON UDP packet after binary codec negotiation")
}

func TestNewUDPPacketReadWriterMatrix(t *testing.T) {
	// v2 + binary
	rw, err := NewUDPPacketReadWriter(&bytes.Buffer{}, wire.ProtocolV2, wire.UDPPacketCodecBinary)
	require.NoError(t, err)
	require.IsType(t, &V2BinaryUDPPacketReadWriter{}, rw)

	// v2 + json (empty codec)
	rw, err = NewUDPPacketReadWriter(&bytes.Buffer{}, wire.ProtocolV2, "")
	require.NoError(t, err)
	require.IsType(t, &V2ReadWriter{}, rw)

	// default protocol -> v1
	rw, err = NewUDPPacketReadWriter(&bytes.Buffer{}, "", "")
	require.NoError(t, err)
	require.IsType(t, &V1ReadWriter{}, rw)

	// invalid combos
	for _, tc := range []struct {
		name, wireProtocol, udpPacketCodec, want string
	}{
		{"binary over v1", wire.ProtocolV1, wire.UDPPacketCodecBinary, "requires wire protocol v2"},
		{"binary over default", "", wire.UDPPacketCodecBinary, "requires wire protocol v2"},
		{"unknown v2 codec", wire.ProtocolV2, "unknown", "unsupported UDP packet codec"},
		{"unknown wire protocol", "unknown", "", "unsupported wire protocol"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewUDPPacketReadWriter(&bytes.Buffer{}, tc.wireProtocol, tc.udpPacketCodec)
			require.Nil(t, got)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestQuicDatagramRoundTrip(t *testing.T) {
	packet := &UDPPacket{
		Content:    []byte("hello-quic"),
		LocalAddr:  &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1000},
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 54321},
	}
	data, err := EncodeQuicDatagramPacket(packet, "my-proxy")
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), MaxQuicDatagramSize)

	out, name, err := DecodeQuicDatagramPacket(data)
	require.NoError(t, err)
	require.Equal(t, "my-proxy", name)
	require.Equal(t, packet.Content, out.Content)
	require.Equal(t, packet.LocalAddr.String(), out.LocalAddr.String())
	require.Equal(t, packet.RemoteAddr.String(), out.RemoteAddr.String())
}

func TestQuicDatagramRoundTripNoLocalAddr(t *testing.T) {
	packet := &UDPPacket{Content: []byte("x"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}
	data, err := EncodeQuicDatagramPacket(packet, "p")
	require.NoError(t, err)
	out, name, err := DecodeQuicDatagramPacket(data)
	require.NoError(t, err)
	require.Equal(t, "p", name)
	require.Nil(t, out.LocalAddr)
	require.Equal(t, "10.0.0.1:9", out.RemoteAddr.String())
	require.Equal(t, packet.Content, out.Content)
}

func TestQuicDatagramRoundTripEmptyContent(t *testing.T) {
	packet := &UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}
	data, err := EncodeQuicDatagramPacket(packet, "")
	require.NoError(t, err)
	out, name, err := DecodeQuicDatagramPacket(data)
	require.NoError(t, err)
	require.Equal(t, "", name)
	require.Empty(t, out.Content)
	require.Equal(t, "10.0.0.1:9", out.RemoteAddr.String())
}

func TestQuicDatagramEncodeErrors(t *testing.T) {
	_, err := EncodeQuicDatagramPacket(nil, "p")
	require.ErrorContains(t, err, "nil UDP packet")

	_, err = EncodeQuicDatagramPacket(&UDPPacket{Content: []byte("x")}, "p")
	require.ErrorContains(t, err, "missing remote address")

	longName := string(bytes.Repeat([]byte{'a'}, 256))
	_, err = EncodeQuicDatagramPacket(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}, longName)
	require.ErrorContains(t, err, "proxy name too long")

	// exactly 255 bytes is accepted
	name255 := string(bytes.Repeat([]byte{'a'}, 255))
	_, err = EncodeQuicDatagramPacket(&UDPPacket{RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}, name255)
	require.NoError(t, err)

	// content too large
	_, err = EncodeQuicDatagramPacket(&UDPPacket{Content: make([]byte, MaxUDPPayloadSize+1), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}, "p")
	require.ErrorContains(t, err, "too large")

	// total datagram too large (> 1200)
	bigAddr := string(bytes.Repeat([]byte{'a'}, 600))
	bigIP := net.ParseIP("2001:db8::1")
	_, err = EncodeQuicDatagramPacket(&UDPPacket{
		LocalAddr:  &net.UDPAddr{IP: bigIP, Port: 1, Zone: bigAddr},
		RemoteAddr: &net.UDPAddr{IP: bigIP, Port: 2, Zone: bigAddr},
		Content:    []byte("x"),
	}, "p")
	require.ErrorContains(t, err, "too large")
}

func TestQuicDatagramDecodeErrors(t *testing.T) {
	_, _, err := DecodeQuicDatagramPacket(nil)
	require.ErrorIs(t, err, ErrDatagramTooShort)
	_, _, err = DecodeQuicDatagramPacket([]byte{})
	require.ErrorIs(t, err, ErrDatagramTooShort)

	// truncated proxy name
	_, _, err = DecodeQuicDatagramPacket([]byte{5, 'h', 'e', 'l'})
	require.ErrorIs(t, err, ErrDatagramTooShort)

	valid, err := EncodeQuicDatagramPacket(&UDPPacket{
		Content:    []byte("ok"),
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9},
	}, "p")
	require.NoError(t, err)

	// trailing data
	_, _, err = DecodeQuicDatagramPacket(append(append([]byte(nil), valid...), 0))
	require.ErrorContains(t, err, "trailing data")

	// truncated local address length
	trunc := append([]byte(nil), valid[:3]...)
	_, _, err = DecodeQuicDatagramPacket(trunc)
	require.ErrorIs(t, err, ErrDatagramTooShort)

	// invalid remote address string
	badRemote := []byte{0, 0, 0, 0, 12, 'i', 'n', 'v', 'a', 'l', 'i', 'd', ':', 'a', 'd', 'd', 'r', 0, 0, 0, 0}
	_, _, err = DecodeQuicDatagramPacket(badRemote)
	require.ErrorContains(t, err, "invalid remote address")

	// invalid local address string
	badLocal := []byte{0, 0, 12, 'i', 'n', 'v', 'a', 'l', 'i', 'd', ':', 'a', 'd', 'd', 'r', 0, 10, '1', '0', '.', '0', '.', '0', '.', '1', ':', '9', 0, 0, 0, 0}
	_, _, err = DecodeQuicDatagramPacket(badLocal)
	require.ErrorContains(t, err, "invalid local address")
}

func TestQuicDatagramContentDoesNotAliasInput(t *testing.T) {
	packet := &UDPPacket{Content: []byte("data"), RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9}}
	data, err := EncodeQuicDatagramPacket(packet, "p")
	require.NoError(t, err)
	out, _, err := DecodeQuicDatagramPacket(data)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.Equal(t, byte('a'), out.Content[len(out.Content)-1])
}

func TestQuicDatagramMaxSizeBoundary(t *testing.T) {
	// Largest content that still fits in a 1200-byte datagram with a short proxy name and small addresses.
	// Format: 1 + 1 + 2 + 9 + 2 + 13 + 4 + content ("10.0.0.1:9"=10, "10.0.0.2:9"=11) => 42 + content
	packet := &UDPPacket{
		LocalAddr:  &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9},
		RemoteAddr: &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 9},
		Content:    make([]byte, MaxQuicDatagramSize-42),
	}
	data, err := EncodeQuicDatagramPacket(packet, "p")
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), MaxQuicDatagramSize)
	out, _, err := DecodeQuicDatagramPacket(data)
	require.NoError(t, err)
	require.Len(t, out.Content, MaxQuicDatagramSize-42)
}

type contextKey string

func (k contextKey) String() string { return string(k) }
