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

package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"

	libnet "github.com/fatedier/golib/net"
	"github.com/stretchr/testify/require"
)

func TestFrameStruct(t *testing.T) {
	f := &Frame{
		Type:    FrameTypeMessage,
		Flags:   0,
		Payload: []byte("hello"),
	}
	require.Equal(t, uint16(16), f.Type)
	require.Equal(t, uint16(0), f.Flags)
	require.Equal(t, []byte("hello"), f.Payload)
}

func TestNewConn(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	require.NotNil(t, conn)
}

func TestWriteFrameRejectsNonZeroFlags(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	err := conn.WriteFrame(&Frame{Type: FrameTypeMessage, Flags: 1, Payload: []byte{1}})
	require.ErrorContains(t, err, "unsupported frame flags")
}

func TestWriteFrameRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	large := make([]byte, DefaultMaxFramePayloadSize+1)
	err := conn.WriteFrame(&Frame{Type: FrameTypeMessage, Payload: large})
	require.ErrorContains(t, err, "exceeds limit")
}

func TestWriteFrameWithExactMaxPayload(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	payload := make([]byte, DefaultMaxFramePayloadSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	err := conn.WriteFrame(&Frame{Type: FrameTypeMessage, Payload: payload})
	require.NoError(t, err)
	require.Equal(t, 8+DefaultMaxFramePayloadSize, len(buf.Bytes()))
}

func TestWriteFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	err := conn.WriteFrame(&Frame{Type: FrameTypeClientHello, Payload: []byte{}})
	require.NoError(t, err)
	require.Equal(t, 8, len(buf.Bytes()))

	// Verify header
	header := buf.Bytes()
	require.Equal(t, FrameTypeClientHello, binary.BigEndian.Uint16(header[0:2]))
	require.Equal(t, uint16(0), binary.BigEndian.Uint16(header[2:4]))
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(header[4:8]))
}

func TestReadFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], FrameTypeServerHello)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], 0)
	buf.Write(header)

	frame, err := NewConn(&buf).ReadFrame()
	require.NoError(t, err)
	require.Equal(t, FrameTypeServerHello, frame.Type)
	require.Equal(t, uint16(0), frame.Flags)
	require.Empty(t, frame.Payload)
}

func TestReadFrameTruncatesOnShortHeader(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{1, 2, 3}) // incomplete header
	_, err := NewConn(&buf).ReadFrame()
	require.Error(t, err)
}

func TestReadFrameTruncatesOnShortPayload(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], FrameTypeMessage)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], 10)
	buf.Write(header)
	buf.Write([]byte{1, 2, 3}) // incomplete payload
	_, err := NewConn(&buf).ReadFrame()
	require.Error(t, err)
}

func TestNewJSONFrame(t *testing.T) {
	in := map[string]string{"key": "value"}
	f, err := NewJSONFrame(FrameTypeMessage, in)
	require.NoError(t, err)
	require.Equal(t, FrameTypeMessage, f.Type)
	require.Equal(t, uint16(0), f.Flags)
	require.NotEmpty(t, f.Payload)

	var out map[string]string
	err = json.Unmarshal(f.Payload, &out)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestNewJSONFrameInvalidInput(t *testing.T) {
	_, err := NewJSONFrame(FrameTypeMessage, make(chan int))
	require.Error(t, err)
}

func TestUnmarshalFrame(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	var out struct {
		Name string `json:"name"`
	}
	f := &Frame{Type: FrameTypeMessage, Payload: []byte(`{"name":"test"}`)}
	err := conn.UnmarshalFrame(f, &out)
	require.NoError(t, err)
	require.Equal(t, "test", out.Name)
}

func TestReadJSONFrameTypeMismatch(t *testing.T) {
	var buf bytes.Buffer
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], FrameTypeClientHello) // actual type
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], 2)
	buf.Write(header)
	buf.Write([]byte("{}"))

	var out ClientHello
	err := NewConn(&buf).ReadJSONFrame(FrameTypeServerHello, &out) // expected different
	require.ErrorContains(t, err, "unexpected frame type")
}

func TestReadJSONFrameReadError(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{1}) // incomplete
	var out ClientHello
	err := NewConn(&buf).ReadJSONFrame(FrameTypeClientHello, &out)
	require.Error(t, err)
}

func TestWriteJSONFrameInvalidInput(t *testing.T) {
	var buf bytes.Buffer
	conn := NewConn(&buf)
	err := conn.WriteJSONFrame(FrameTypeMessage, make(chan int))
	require.Error(t, err)
}

func TestWriteMagic(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMagic(&buf)
	require.NoError(t, err)
	require.Equal(t, []byte(MagicV2), buf.Bytes())
}

func TestWriteMagicIfV2ProtocolV1(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMagicIfV2(&buf, ProtocolV1)
	require.NoError(t, err)
	require.Empty(t, buf.Bytes())
}

func TestWriteMagicIfV2ProtocolV2(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMagicIfV2(&buf, ProtocolV2)
	require.NoError(t, err)
	require.Equal(t, []byte(MagicV2), buf.Bytes())
}

func TestWriteMagicIfV2OtherProtocol(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMagicIfV2(&buf, "v3")
	require.NoError(t, err)
	require.Empty(t, buf.Bytes())
}

func TestWriteMagicIfV2Empty(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMagicIfV2(&buf, "")
	require.NoError(t, err)
	require.Empty(t, buf.Bytes())
}

func TestCheckMagicMismatchReturnsSharedConnWithRemainingBytes(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	want := []byte("notfrp...")
	go func() {
		defer client.Close()
		_, _ = client.Write(want)
	}()

	out, isV2, err := CheckMagic(server)
	require.NoError(t, err)
	require.False(t, isV2)

	got, err := io.ReadAll(out)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCheckMagicPartialRead(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	go func() {
		defer client.Close()
		_, _ = client.Write([]byte("FRP")) // partial magic
	}()

	out, isV2, err := CheckMagic(server)
	require.Error(t, err)
	require.False(t, isV2)
	_ = out
}

func TestCheckMagicV2WithExtraDataPreserved(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	want := []byte("extra data here")
	go func() {
		defer client.Close()
		_, _ = client.Write(append([]byte(MagicV2), want...))
	}()

	out, isV2, err := CheckMagic(server)
	require.NoError(t, err)
	require.True(t, isV2)

	got := make([]byte, len(want))
	n, err := io.ReadFull(out, got)
	require.NoError(t, err)
	require.Equal(t, len(want), n)
	require.Equal(t, want, got)
}

func TestCheckMagicWithLibnetSharedConn(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	want := []byte("payload")
	go func() {
		defer client.Close()
		_, _ = client.Write(append([]byte(MagicV2), want...))
	}()

	out, isV2, err := CheckMagic(server)
	require.NoError(t, err)
	require.True(t, isV2)
	require.Equal(t, connType(out), connType(server)) // same underlying
	_ = out
}

func connType(c net.Conn) string {
	switch c.(type) {
	case *net.TCPConn:
		return "*net.TCPConn"
	case *net.UnixConn:
		return "*net.UnixConn"
	case *libnet.SharedConn:
		return "*libnet.SharedConn"
	default:
		return "other"
	}
}

func TestBootstrapInfo(t *testing.T) {
	b := BootstrapInfo{Transport: "tcp", TLS: true, TCPMux: true}
	require.Equal(t, "tcp", b.Transport)
	require.True(t, b.TLS)
	require.True(t, b.TCPMux)

	b2 := BootstrapInfo{}
	require.Equal(t, "", b2.Transport)
	require.False(t, b2.TLS)
	require.False(t, b2.TCPMux)
}

func TestClientCapabilitiesStruct(t *testing.T) {
	cc := ClientCapabilities{
		Message: MessageCapabilities{
			Codecs:          []string{MessageCodecJSON},
			UDPPacketCodecs: []string{UDPPacketCodecBinary},
		},
		Crypto: CryptoCapabilities{
			Algorithms:   []string{AEADAlgorithmAES256GCM},
			ClientRandom: make([]byte, CryptoRandomSize),
		},
	}
	require.Len(t, cc.Message.Codecs, 1)
	require.Len(t, cc.Message.UDPPacketCodecs, 1)
	require.Len(t, cc.Crypto.Algorithms, 1)
	require.Len(t, cc.Crypto.ClientRandom, CryptoRandomSize)
}

func TestServerHelloStructs(t *testing.T) {
	sh := DefaultServerHello()
	require.Equal(t, MessageCodecJSON, sh.Selected.Message.Codec)
	require.Equal(t, "", sh.Selected.Message.UDPPacketCodec)
	require.Equal(t, "", sh.Selected.Crypto.Algorithm)
	require.Nil(t, sh.Selected.Crypto.ServerRandom)
	require.Equal(t, "", sh.Error)

	sh2 := ServerHello{
		Selected: ServerSelection{
			Message: MessageSelection{
				Codec:          MessageCodecJSON,
				UDPPacketCodec: UDPPacketCodecBinary,
			},
			Crypto: CryptoSelection{
				Algorithm:    AEADAlgorithmAES256GCM,
				ServerRandom: make([]byte, CryptoRandomSize),
			},
		},
		Error: "test error",
	}
	require.Equal(t, UDPPacketCodecBinary, sh2.Selected.Message.UDPPacketCodec)
	require.Equal(t, AEADAlgorithmAES256GCM, sh2.Selected.Crypto.Algorithm)
	require.Len(t, sh2.Selected.Crypto.ServerRandom, CryptoRandomSize)
	require.Equal(t, "test error", sh2.Error)
}

func TestSupports(t *testing.T) {
	require.True(t, Supports([]string{"a", "b", "c"}, "a"))
	require.True(t, Supports([]string{"a", "b", "c"}, "c"))
	require.False(t, Supports([]string{"a", "b", "c"}, "d"))
	require.False(t, Supports([]string{}, "a"))
	require.False(t, Supports(nil, "a"))
}

func TestValidateClientHelloWithAdditionalCodecs(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Message.Codecs = []string{MessageCodecJSON, "extra"}
	require.NoError(t, ValidateClientHello(hello))
}

func TestValidateClientHelloMissingJSONCodec(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Message.Codecs = []string{"protobuf"}
	require.ErrorContains(t, ValidateClientHello(hello), "unsupported message codec")
}

func TestValidateClientHelloEmptyCodecs(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Message.Codecs = []string{}
	require.ErrorContains(t, ValidateClientHello(hello), "unsupported message codec")
}

func TestValidateClientHelloNilCodecs(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Message.Codecs = nil
	require.ErrorContains(t, ValidateClientHello(hello), "unsupported message codec")
}

func TestClientHelloWithCryptoRandom(t *testing.T) {
	clientRandom := make([]byte, CryptoRandomSize)
	for i := range clientRandom {
		clientRandom[i] = byte(i)
	}
	hello := clientHelloWithCryptoRandom(BootstrapInfo{Transport: "kcp"}, clientRandom)
	require.Equal(t, "kcp", hello.Bootstrap.Transport)
	require.Equal(t, MessageCodecJSON, hello.Capabilities.Message.Codecs[0])
	require.Equal(t, UDPPacketCodecBinary, hello.Capabilities.Message.UDPPacketCodecs[0])
	require.Equal(t, AEADAlgorithmAES256GCM, hello.Capabilities.Crypto.Algorithms[0])
	require.Equal(t, clientRandom, hello.Capabilities.Crypto.ClientRandom)
}

func TestNewClientHello(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{Transport: "tcp", TLS: false})
	require.NoError(t, err)
	require.Equal(t, "tcp", hello.Bootstrap.Transport)
	require.Len(t, hello.Capabilities.Crypto.ClientRandom, CryptoRandomSize)
	require.NotEmpty(t, hello.Capabilities.Crypto.Algorithms)
}

func TestNewClientHelloMultipleCallsProduceDifferentRandoms(t *testing.T) {
	hello1, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello2, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	require.NotEqual(t, hello1.Capabilities.Crypto.ClientRandom, hello2.Capabilities.Crypto.ClientRandom)
}

func TestNewServerHelloInvalidClientHello(t *testing.T) {
	badHello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	badHello.Capabilities.Message.Codecs = []string{"bad"}
	_, _, err = NewServerHello(badHello)
	require.Error(t, err)
}

func TestNewServerHelloSelectsBestAvailable(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	// Prefer order in client algorithms
	serverHello, _, err := NewServerHello(hello)
	require.NoError(t, err)
	algos := PreferredAEADAlgorithms()
	require.Equal(t, algos[0], serverHello.Selected.Crypto.Algorithm)
}

func TestNewServerHelloWithMultipleAlgorithmOptions(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Crypto.Algorithms = []string{AEADAlgorithmXChaCha20Poly1305, AEADAlgorithmAES256GCM}
	serverHello, _, err := NewServerHello(hello)
	require.NoError(t, err)
	require.Equal(t, AEADAlgorithmXChaCha20Poly1305, serverHello.Selected.Crypto.Algorithm)
}

func TestNewServerHelloWithOnlyXChaCha(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Crypto.Algorithms = []string{AEADAlgorithmXChaCha20Poly1305}
	serverHello, _, err := NewServerHello(hello)
	require.NoError(t, err)
	require.Equal(t, AEADAlgorithmXChaCha20Poly1305, serverHello.Selected.Crypto.Algorithm)
}

func TestNewServerHelloWithOnlyAESGCM(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Crypto.Algorithms = []string{AEADAlgorithmAES256GCM}
	serverHello, _, err := NewServerHello(hello)
	require.NoError(t, err)
	require.Equal(t, AEADAlgorithmAES256GCM, serverHello.Selected.Crypto.Algorithm)
}

func TestNewServerHelloNoSupportedAlgorithm(t *testing.T) {
	hello, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	hello.Capabilities.Crypto.Algorithms = []string{"unknown1", "unknown2"}
	_, _, err = NewServerHello(hello)
	require.ErrorContains(t, err, "no supported crypto algorithm")
}

func TestValidateCryptoCapabilities(t *testing.T) {
	c := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize),
		Algorithms:   []string{AEADAlgorithmAES256GCM},
	}
	require.NoError(t, ValidateCryptoCapabilities(c))
}

func TestValidateCryptoCapabilitiesInvalidRandomLength(t *testing.T) {
	c := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize-1),
		Algorithms:   []string{AEADAlgorithmAES256GCM},
	}
	err := ValidateCryptoCapabilities(c)
	require.ErrorContains(t, err, "invalid crypto client random length")

	c2 := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize+1),
		Algorithms:   []string{AEADAlgorithmAES256GCM},
	}
	err = ValidateCryptoCapabilities(c2)
	require.ErrorContains(t, err, "invalid crypto client random length")
}

func TestValidateCryptoCapabilitiesNoSupportedAlgorithm(t *testing.T) {
	c := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize),
		Algorithms:   []string{"bad"},
	}
	err := ValidateCryptoCapabilities(c)
	require.ErrorContains(t, err, "no supported crypto algorithm")

	c2 := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize),
		Algorithms:   []string{},
	}
	err = ValidateCryptoCapabilities(c2)
	require.ErrorContains(t, err, "no supported crypto algorithm")
}

func TestValidateCryptoCapabilitiesNilAlgorithms(t *testing.T) {
	c := CryptoCapabilities{
		ClientRandom: make([]byte, CryptoRandomSize),
		Algorithms:   nil,
	}
	err := ValidateCryptoCapabilities(c)
	require.ErrorContains(t, err, "no supported crypto algorithm")
}

func TestValidateServerHelloForClient(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	require.NoError(t, ValidateServerHelloForClient(client, server))
}

func TestValidateServerHelloForClientUnsupportedMessageCodec(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Message.Codec = "unknown"
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "unsupported selected message codec")
}

func TestValidateServerHelloForClientUnsupportedUDPPacketCodec(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Message.UDPPacketCodec = "unknown"
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "unsupported selected UDP packet codec")
}

func TestValidateServerHelloForClientUnadvertisedUDPPacketCodec(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	// Client doesn't advertise UDP packet codec
	client.Capabilities.Message.UDPPacketCodecs = []string{}
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	// Server selects binary even though not advertised
	server.Selected.Message.UDPPacketCodec = UDPPacketCodecBinary
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "was not advertised by client")
}

func TestValidateServerHelloForClientEmptyUDPPacketCodecOk(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	client.Capabilities.Message.UDPPacketCodecs = []string{}
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	require.NoError(t, ValidateServerHelloForClient(client, server))
}

func TestValidateServerHelloForClientNilUDPPacketCodecsOk(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	client.Capabilities.Message.UDPPacketCodecs = nil
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	require.NoError(t, ValidateServerHelloForClient(client, server))
}

func TestValidateServerHelloForClientUnknownCryptoAlgorithm(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Crypto.Algorithm = "unknown"
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "unknown selected crypto algorithm")
}

func TestValidateServerHelloForClientUnadvertisedCryptoAlgorithm(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	client.Capabilities.Crypto.Algorithms = []string{AEADAlgorithmAES256GCM}
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Crypto.Algorithm = AEADAlgorithmXChaCha20Poly1305
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "selected crypto algorithm was not advertised by client")
}

func TestValidateServerHelloForClientInvalidServerRandomLength(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Crypto.ServerRandom = make([]byte, CryptoRandomSize-1)
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "invalid crypto server random length")

	server.Selected.Crypto.ServerRandom = make([]byte, CryptoRandomSize+1)
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "invalid crypto server random length")

	server.Selected.Crypto.ServerRandom = nil
	err = ValidateServerHelloForClient(client, server)
	require.ErrorContains(t, err, "invalid crypto server random length")
}

func TestSelectUDPPacketCodec(t *testing.T) {
	require.Equal(t, UDPPacketCodecBinary, selectUDPPacketCodec([]string{UDPPacketCodecBinary}))
	require.Equal(t, UDPPacketCodecBinary, selectUDPPacketCodec([]string{"other", UDPPacketCodecBinary}))
	require.Equal(t, "", selectUDPPacketCodec([]string{}))
	require.Equal(t, "", selectUDPPacketCodec(nil))
	require.Equal(t, "", selectUDPPacketCodec([]string{"unknown"}))
}

func TestNewCryptoContext(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{Transport: "tcp"})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)

	clientFrame, err := NewJSONFrame(FrameTypeClientHello, client)
	require.NoError(t, err)
	serverFrame, err := NewJSONFrame(FrameTypeServerHello, server)
	require.NoError(t, err)

	ctx := NewCryptoContext(server.Selected.Crypto.Algorithm, nil, clientFrame.Payload, serverFrame.Payload)
	require.NotNil(t, ctx)
	require.Equal(t, server.Selected.Crypto.Algorithm, ctx.Algorithm)
	require.Len(t, ctx.TranscriptHash, 32) // SHA256
}

func TestNewCryptoContextDeterministic(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)

	clientFrame, _ := NewJSONFrame(FrameTypeClientHello, client)
	serverFrame, _ := NewJSONFrame(FrameTypeServerHello, server)

	ctx1 := NewCryptoContext(server.Selected.Crypto.Algorithm, nil, clientFrame.Payload, serverFrame.Payload)
	ctx2 := NewCryptoContext(server.Selected.Crypto.Algorithm, nil, clientFrame.Payload, serverFrame.Payload)
	require.Equal(t, ctx1.Algorithm, ctx2.Algorithm)
	require.Equal(t, ctx1.TranscriptHash, ctx2.TranscriptHash)
}

func TestHashCryptoTranscript(t *testing.T) {
	hash := HashCryptoTranscript([]byte("client"), []byte("server"))
	require.Len(t, hash, 32)
	hash2 := HashCryptoTranscript([]byte("client"), []byte("server"))
	require.Equal(t, hash, hash2)

	hash3 := HashCryptoTranscript([]byte("client2"), []byte("server"))
	require.NotEqual(t, hash, hash3)

	hash4 := HashCryptoTranscript([]byte("client"), []byte("server2"))
	require.NotEqual(t, hash, hash4)
}

func TestHashCryptoTranscriptEmptyPayloads(t *testing.T) {
	hash := HashCryptoTranscript([]byte{}, []byte{})
	require.Len(t, hash, 32)

	hash2 := HashCryptoTranscript(nil, nil)
	require.Len(t, hash2, 32)
	require.Equal(t, hash, hash2)
}

func TestNewClientCryptoContext(t *testing.T) {
	client, priv, err := NewClientHello(BootstrapInfo{Transport: "tcp", TLS: true})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)

	clientFrame, _ := NewJSONFrame(FrameTypeClientHello, client)
	serverFrame, _ := NewJSONFrame(FrameTypeServerHello, server)

	ctx, err := NewClientCryptoContext(priv, clientFrame.Payload, serverFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, server.Selected.Crypto.Algorithm, ctx.Algorithm)
	require.Len(t, ctx.TranscriptHash, 32)
}

func TestNewClientCryptoContextInvalidClientHelloJSON(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)

	serverFrame, _ := NewJSONFrame(FrameTypeServerHello, server)
	_, err = NewClientCryptoContext(nil, []byte("not json"), serverFrame.Payload)
	require.ErrorContains(t, err, "decode ClientHello transcript")
}

func TestNewClientCryptoContextInvalidServerHelloJSON(t *testing.T) {
	client, _, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	clientFrame, _ := NewJSONFrame(FrameTypeClientHello, client)
	_, err = NewClientCryptoContext(nil, clientFrame.Payload, []byte("not json"))
	require.ErrorContains(t, err, "decode ServerHello transcript")
}

func TestNewClientCryptoContextValidationFails(t *testing.T) {
	client, priv, err := NewClientHello(BootstrapInfo{})
	require.NoError(t, err)
	server, _, err := NewServerHello(client)
	require.NoError(t, err)
	server.Selected.Crypto.Algorithm = "unknown"

	clientFrame, _ := NewJSONFrame(FrameTypeClientHello, client)
	serverFrame, _ := NewJSONFrame(FrameTypeServerHello, server)
	_, err = NewClientCryptoContext(priv, clientFrame.Payload, serverFrame.Payload)
	require.Error(t, err)
}

func TestPreferredAEADAlgorithmsStructure(t *testing.T) {
	algs := PreferredAEADAlgorithms()
	require.Len(t, algs, 2)
	require.Contains(t, algs, AEADAlgorithmAES256GCM)
	require.Contains(t, algs, AEADAlgorithmXChaCha20Poly1305)
}

func TestSelectAEADAlgorithm(t *testing.T) {
	selected, ok := SelectAEADAlgorithm([]string{AEADAlgorithmAES256GCM})
	require.True(t, ok)
	require.Equal(t, AEADAlgorithmAES256GCM, selected)

	selected, ok = SelectAEADAlgorithm([]string{AEADAlgorithmXChaCha20Poly1305})
	require.True(t, ok)
	require.Equal(t, AEADAlgorithmXChaCha20Poly1305, selected)

	selected, ok = SelectAEADAlgorithm([]string{AEADAlgorithmXChaCha20Poly1305, AEADAlgorithmAES256GCM})
	require.True(t, ok)
	require.Equal(t, AEADAlgorithmXChaCha20Poly1305, selected) // first match in client list that is supported

	selected, ok = SelectAEADAlgorithm([]string{"unknown", AEADAlgorithmAES256GCM})
	require.True(t, ok)
	require.Equal(t, AEADAlgorithmAES256GCM, selected)

	selected, ok = SelectAEADAlgorithm([]string{"unknown1", "unknown2"})
	require.False(t, ok)
	require.Equal(t, "", selected)

	selected, ok = SelectAEADAlgorithm([]string{})
	require.False(t, ok)
	require.Equal(t, "", selected)

	selected, ok = SelectAEADAlgorithm(nil)
	require.False(t, ok)
	require.Equal(t, "", selected)
}

func TestIsSupportedAEADAlgorithm(t *testing.T) {
	require.True(t, IsSupportedAEADAlgorithm(AEADAlgorithmAES256GCM))
	require.True(t, IsSupportedAEADAlgorithm(AEADAlgorithmXChaCha20Poly1305))
	require.False(t, IsSupportedAEADAlgorithm("chacha20-poly1305"))
	require.False(t, IsSupportedAEADAlgorithm(""))
	require.False(t, IsSupportedAEADAlgorithm("unknown"))
}

func TestNewCryptoRandom(t *testing.T) {
	r1, err := newCryptoRandom()
	require.NoError(t, err)
	require.Len(t, r1, CryptoRandomSize)

	r2, err := newCryptoRandom()
	require.NoError(t, err)
	require.Len(t, r2, CryptoRandomSize)
	require.NotEqual(t, r1, r2)
}

func TestHasFastAESGCM(t *testing.T) {
	// Just ensure it returns bool without panic
	res := hasFastAESGCM()
	require.True(t, res == true || res == false)
}

func TestConstants(t *testing.T) {
	require.Equal(t, "v1", ProtocolV1)
	require.Equal(t, "v2", ProtocolV2)
	require.Equal(t, uint16(2), uint16(WireVersionV2))
	require.Equal(t, uint16(1), FrameTypeClientHello)
	require.Equal(t, uint16(2), FrameTypeServerHello)
	require.Equal(t, uint16(16), FrameTypeMessage)
	require.Equal(t, "json", MessageCodecJSON)
	require.Equal(t, "binary-v1", UDPPacketCodecBinary)
	require.Equal(t, int(DefaultMaxFramePayloadSize), 65536)
	require.Equal(t, "FRP\x00\x02\r\n", MagicV2)
	require.Equal(t, 32, int(CryptoRandomSize))
	require.Equal(t, "aes-256-gcm", AEADAlgorithmAES256GCM)
	require.Equal(t, "xchacha20-poly1305", AEADAlgorithmXChaCha20Poly1305)
	require.Equal(t, "frp wire v2 crypto transcript", cryptoTranscriptLabel)
}
