// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msg

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/fatedier/frp/pkg/proto/wire"
)

// TestV2MessageConstructors verifies that the precomputed constructor table
// produces the same message types as the old reflect.New path, for every
// registered v2 message type. This guards the optimization against drift
// between the constructor table and the type registry.
func TestV2MessageConstructors(t *testing.T) {
	for typeID, m := range v2MsgTypeMap {
		// Build the payload the way EncodeV2MessageFrame does.
		content, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal %d: %v", typeID, err)
		}
		payload := make([]byte, 2+len(content))
		payload[0] = byte(typeID >> 8)
		payload[1] = byte(typeID)
		copy(payload[2:], content)

		frame := &wire.Frame{Type: wire.FrameTypeMessage, Payload: payload}

		decoded, err := DecodeV2MessageFrame(frame)
		if err != nil {
			t.Fatalf("type %d decode error: %v", typeID, err)
		}
		if decoded == nil {
			t.Fatalf("type %d: nil message", typeID)
		}
		// The decoded message must be a non-nil pointer to the same concrete
		// struct type as the registered value. Both the old reflect.New path
		// and the new constructor table return pointers, so json.Unmarshal
		// can populate the fields directly.
		decodedPtrType := reflect.TypeOf(decoded)
		if decodedPtrType == nil || decodedPtrType.Kind() != reflect.Pointer {
			t.Fatalf("type %d: got non-pointer %T", typeID, decoded)
		}
		if decodedPtrType.Elem() != reflect.TypeOf(m) {
			t.Fatalf("type %d: got *%s want *%s", typeID, decodedPtrType.Elem(), reflect.TypeOf(m))
		}
	}
}
