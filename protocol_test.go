package squirreldb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"
)

func TestProtocolConstants(t *testing.T) {
	t.Run("Magic bytes", func(t *testing.T) {
		expected := []byte{'S', 'Q', 'R', 'L'}
		if !bytes.Equal(Magic, expected) {
			t.Errorf("Magic = %v, want %v", Magic, expected)
		}
		if string(Magic) != "SQRL" {
			t.Errorf("Magic as string = %s, want SQRL", string(Magic))
		}
	})

	t.Run("ProtocolVersion", func(t *testing.T) {
		if ProtocolVersion != 0x01 {
			t.Errorf("ProtocolVersion = %d, want %d", ProtocolVersion, 0x01)
		}
	})

	t.Run("MaxMessageSize", func(t *testing.T) {
		expected := 16 * 1024 * 1024
		if MaxMessageSize != expected {
			t.Errorf("MaxMessageSize = %d, want %d", MaxMessageSize, expected)
		}
	})
}

func TestHandshakeStatus(t *testing.T) {
	tests := []struct {
		name   string
		status HandshakeStatus
		want   byte
	}{
		{"Success", HandshakeSuccess, 0x00},
		{"VersionMismatch", HandshakeVersionMismatch, 0x01},
		{"AuthFailed", HandshakeAuthFailed, 0x02},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.status) != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, byte(tt.status), tt.want)
			}
		})
	}
}

func TestMessageType(t *testing.T) {
	tests := []struct {
		name    string
		msgType MessageType
		want    byte
	}{
		{"Request", MessageTypeRequest, 0x01},
		{"Response", MessageTypeResponse, 0x02},
		{"Notification", MessageTypeNotification, 0x03},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.msgType) != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, byte(tt.msgType), tt.want)
			}
		})
	}
}

func TestEncoding(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		want     byte
	}{
		{"MessagePack", EncodingMessagePack, 0x01},
		{"JSON", EncodingJSON, 0x02},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.encoding) != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, byte(tt.encoding), tt.want)
			}
		})
	}
}

func TestProtocolFlags(t *testing.T) {
	t.Run("ToByte with both false", func(t *testing.T) {
		flags := ProtocolFlags{MessagePack: false, JSONFallback: false}
		if flags.ToByte() != 0x00 {
			t.Errorf("ToByte() = %d, want %d", flags.ToByte(), 0x00)
		}
	})

	t.Run("ToByte with MessagePack only", func(t *testing.T) {
		flags := ProtocolFlags{MessagePack: true, JSONFallback: false}
		if flags.ToByte() != 0x01 {
			t.Errorf("ToByte() = %d, want %d", flags.ToByte(), 0x01)
		}
	})

	t.Run("ToByte with JSONFallback only", func(t *testing.T) {
		flags := ProtocolFlags{MessagePack: false, JSONFallback: true}
		if flags.ToByte() != 0x02 {
			t.Errorf("ToByte() = %d, want %d", flags.ToByte(), 0x02)
		}
	})

	t.Run("ToByte with both true", func(t *testing.T) {
		flags := ProtocolFlags{MessagePack: true, JSONFallback: true}
		if flags.ToByte() != 0x03 {
			t.Errorf("ToByte() = %d, want %d", flags.ToByte(), 0x03)
		}
	})

	t.Run("FlagsFromByte 0x00", func(t *testing.T) {
		flags := FlagsFromByte(0x00)
		if flags.MessagePack || flags.JSONFallback {
			t.Errorf("FlagsFromByte(0x00) = %+v, want both false", flags)
		}
	})

	t.Run("FlagsFromByte 0x01", func(t *testing.T) {
		flags := FlagsFromByte(0x01)
		if !flags.MessagePack || flags.JSONFallback {
			t.Errorf("FlagsFromByte(0x01) = %+v, want MessagePack=true, JSONFallback=false", flags)
		}
	})

	t.Run("FlagsFromByte 0x02", func(t *testing.T) {
		flags := FlagsFromByte(0x02)
		if flags.MessagePack || !flags.JSONFallback {
			t.Errorf("FlagsFromByte(0x02) = %+v, want MessagePack=false, JSONFallback=true", flags)
		}
	})

	t.Run("FlagsFromByte 0x03", func(t *testing.T) {
		flags := FlagsFromByte(0x03)
		if !flags.MessagePack || !flags.JSONFallback {
			t.Errorf("FlagsFromByte(0x03) = %+v, want both true", flags)
		}
	})

	t.Run("Roundtrip", func(t *testing.T) {
		for _, mp := range []bool{true, false} {
			for _, jf := range []bool{true, false} {
				flags := ProtocolFlags{MessagePack: mp, JSONFallback: jf}
				b := flags.ToByte()
				restored := FlagsFromByte(b)
				if restored.MessagePack != mp || restored.JSONFallback != jf {
					t.Errorf("Roundtrip failed: %+v -> %d -> %+v", flags, b, restored)
				}
			}
		}
	})
}

func TestBuildHandshake(t *testing.T) {
	t.Run("without auth", func(t *testing.T) {
		data := BuildHandshake("", ProtocolFlags{MessagePack: true, JSONFallback: true})

		if !bytes.Equal(data[0:4], Magic) {
			t.Errorf("Magic bytes = %v, want %v", data[0:4], Magic)
		}
		if data[4] != ProtocolVersion {
			t.Errorf("Version = %d, want %d", data[4], ProtocolVersion)
		}
		if data[5] != 0x03 {
			t.Errorf("Flags = %d, want %d", data[5], 0x03)
		}
		tokenLen := binary.BigEndian.Uint16(data[6:8])
		if tokenLen != 0 {
			t.Errorf("Token length = %d, want %d", tokenLen, 0)
		}
		if len(data) != 8 {
			t.Errorf("Total length = %d, want %d", len(data), 8)
		}
	})

	t.Run("with auth", func(t *testing.T) {
		token := "my-secret-token"
		data := BuildHandshake(token, ProtocolFlags{MessagePack: true, JSONFallback: true})

		tokenLen := binary.BigEndian.Uint16(data[6:8])
		if int(tokenLen) != len(token) {
			t.Errorf("Token length = %d, want %d", tokenLen, len(token))
		}
		if string(data[8:]) != token {
			t.Errorf("Token = %s, want %s", string(data[8:]), token)
		}
	})

	t.Run("with custom flags", func(t *testing.T) {
		data := BuildHandshake("", ProtocolFlags{MessagePack: true, JSONFallback: false})
		if data[5] != 0x01 {
			t.Errorf("Flags = %d, want %d", data[5], 0x01)
		}
	})
}

func TestParseHandshakeResponse(t *testing.T) {
	t.Run("success response", func(t *testing.T) {
		sessionID := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		response := make([]byte, 19)
		response[0] = byte(HandshakeSuccess)
		response[1] = 0x01
		response[2] = 0x03
		copy(response[3:], sessionID[:])

		result, err := ParseHandshakeResponse(response)
		if err != nil {
			t.Fatalf("ParseHandshakeResponse() error = %v", err)
		}

		if result.Status != HandshakeSuccess {
			t.Errorf("Status = %d, want %d", result.Status, HandshakeSuccess)
		}
		if result.Version != 0x01 {
			t.Errorf("Version = %d, want %d", result.Version, 0x01)
		}
		if !result.Flags.MessagePack || !result.Flags.JSONFallback {
			t.Errorf("Flags = %+v, want both true", result.Flags)
		}
		if result.SessionID != sessionID {
			t.Errorf("SessionID = %v, want %v", result.SessionID, sessionID)
		}
	})

	t.Run("version mismatch response", func(t *testing.T) {
		response := make([]byte, 19)
		response[0] = byte(HandshakeVersionMismatch)
		response[1] = 0x02

		result, err := ParseHandshakeResponse(response)
		if err != nil {
			t.Fatalf("ParseHandshakeResponse() error = %v", err)
		}

		if result.Status != HandshakeVersionMismatch {
			t.Errorf("Status = %d, want %d", result.Status, HandshakeVersionMismatch)
		}
		if result.Version != 0x02 {
			t.Errorf("Version = %d, want %d", result.Version, 0x02)
		}
	})

	t.Run("auth failed response", func(t *testing.T) {
		response := make([]byte, 19)
		response[0] = byte(HandshakeAuthFailed)

		result, err := ParseHandshakeResponse(response)
		if err != nil {
			t.Fatalf("ParseHandshakeResponse() error = %v", err)
		}

		if result.Status != HandshakeAuthFailed {
			t.Errorf("Status = %d, want %d", result.Status, HandshakeAuthFailed)
		}
	})

	t.Run("too short response", func(t *testing.T) {
		_, err := ParseHandshakeResponse([]byte{0x00, 0x01})
		if err == nil {
			t.Error("Expected error for too short response")
		}
	})
}

func TestEncodeDecodeMessage(t *testing.T) {
	msg := map[string]interface{}{
		"type":  "query",
		"id":    "123",
		"query": "test",
	}

	t.Run("JSON roundtrip", func(t *testing.T) {
		data, err := EncodeMessage(msg, EncodingJSON)
		if err != nil {
			t.Fatalf("EncodeMessage() error = %v", err)
		}

		var decoded map[string]interface{}
		err = DecodeMessage(data, EncodingJSON, &decoded)
		if err != nil {
			t.Fatalf("DecodeMessage() error = %v", err)
		}

		if decoded["type"] != msg["type"] {
			t.Errorf("type = %v, want %v", decoded["type"], msg["type"])
		}
		if decoded["id"] != msg["id"] {
			t.Errorf("id = %v, want %v", decoded["id"], msg["id"])
		}
	})

	t.Run("MessagePack roundtrip", func(t *testing.T) {
		data, err := EncodeMessage(msg, EncodingMessagePack)
		if err != nil {
			t.Fatalf("EncodeMessage() error = %v", err)
		}

		var decoded map[string]interface{}
		err = DecodeMessage(data, EncodingMessagePack, &decoded)
		if err != nil {
			t.Fatalf("DecodeMessage() error = %v", err)
		}

		if decoded["type"] != msg["type"] {
			t.Errorf("type = %v, want %v", decoded["type"], msg["type"])
		}
		if decoded["id"] != msg["id"] {
			t.Errorf("id = %v, want %v", decoded["id"], msg["id"])
		}
	})
}

func TestBuildFrame(t *testing.T) {
	t.Run("frame structure", func(t *testing.T) {
		payload := []byte("test payload")
		frame := BuildFrame(MessageTypeRequest, EncodingMessagePack, payload)

		// Length should be payload + 2
		length := binary.BigEndian.Uint32(frame[0:4])
		if length != uint32(len(payload)+2) {
			t.Errorf("Length = %d, want %d", length, len(payload)+2)
		}

		// Message type
		if frame[4] != byte(MessageTypeRequest) {
			t.Errorf("MsgType = %d, want %d", frame[4], MessageTypeRequest)
		}

		// Encoding
		if frame[5] != byte(EncodingMessagePack) {
			t.Errorf("Encoding = %d, want %d", frame[5], EncodingMessagePack)
		}

		// Payload
		if !bytes.Equal(frame[6:], payload) {
			t.Errorf("Payload = %v, want %v", frame[6:], payload)
		}
	})

	t.Run("response frame", func(t *testing.T) {
		payload := []byte("response data")
		frame := BuildFrame(MessageTypeResponse, EncodingJSON, payload)

		if frame[4] != byte(MessageTypeResponse) {
			t.Errorf("MsgType = %d, want %d", frame[4], MessageTypeResponse)
		}
		if frame[5] != byte(EncodingJSON) {
			t.Errorf("Encoding = %d, want %d", frame[5], EncodingJSON)
		}
	})
}

func TestParseFrameHeader(t *testing.T) {
	t.Run("request header", func(t *testing.T) {
		// Length=14 (12 payload + 2), type=REQUEST, encoding=MESSAGEPACK
		header := []byte{0x00, 0x00, 0x00, 0x0e, 0x01, 0x01}
		result, err := ParseFrameHeader(header)
		if err != nil {
			t.Fatalf("ParseFrameHeader() error = %v", err)
		}

		if result.PayloadLength != 12 {
			t.Errorf("PayloadLength = %d, want %d", result.PayloadLength, 12)
		}
		if result.MsgType != MessageTypeRequest {
			t.Errorf("MsgType = %d, want %d", result.MsgType, MessageTypeRequest)
		}
		if result.Encoding != EncodingMessagePack {
			t.Errorf("Encoding = %d, want %d", result.Encoding, EncodingMessagePack)
		}
	})

	t.Run("response header", func(t *testing.T) {
		// Length=34, type=RESPONSE, encoding=JSON
		header := []byte{0x00, 0x00, 0x00, 0x22, 0x02, 0x02}
		result, err := ParseFrameHeader(header)
		if err != nil {
			t.Fatalf("ParseFrameHeader() error = %v", err)
		}

		if result.PayloadLength != 32 {
			t.Errorf("PayloadLength = %d, want %d", result.PayloadLength, 32)
		}
		if result.MsgType != MessageTypeResponse {
			t.Errorf("MsgType = %d, want %d", result.MsgType, MessageTypeResponse)
		}
		if result.Encoding != EncodingJSON {
			t.Errorf("Encoding = %d, want %d", result.Encoding, EncodingJSON)
		}
	})

	t.Run("too short header", func(t *testing.T) {
		_, err := ParseFrameHeader([]byte{0x00, 0x00, 0x00})
		if err == nil {
			t.Error("Expected error for too short header")
		}
	})
}

func TestUUIDToString(t *testing.T) {
	t.Run("standard UUID", func(t *testing.T) {
		b := [16]byte{
			0x55, 0x0e, 0x84, 0x00,
			0xe2, 0x9b,
			0x41, 0xd4,
			0xa7, 0x16,
			0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		}
		uuid := UUIDToString(b)
		if uuid != "550e8400-e29b-41d4-a716-446655440000" {
			t.Errorf("UUID = %s, want 550e8400-e29b-41d4-a716-446655440000", uuid)
		}
	})

	t.Run("all zeros", func(t *testing.T) {
		b := [16]byte{}
		uuid := UUIDToString(b)
		if uuid != "00000000-0000-0000-0000-000000000000" {
			t.Errorf("UUID = %s, want 00000000-0000-0000-0000-000000000000", uuid)
		}
	})

	t.Run("all 0xff", func(t *testing.T) {
		b := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		uuid := UUIDToString(b)
		if uuid != "ffffffff-ffff-ffff-ffff-ffffffffffff" {
			t.Errorf("UUID = %s, want ffffffff-ffff-ffff-ffff-ffffffffffff", uuid)
		}
	})
}

func TestFullFrameRoundtrip(t *testing.T) {
	t.Run("JSON roundtrip", func(t *testing.T) {
		msg := map[string]interface{}{
			"type":  "query",
			"id":    "test-123",
			"query": "db.table(\"users\").run()",
		}

		// Encode message
		payload, err := EncodeMessage(msg, EncodingJSON)
		if err != nil {
			t.Fatalf("EncodeMessage() error = %v", err)
		}

		// Build frame
		frame := BuildFrame(MessageTypeRequest, EncodingJSON, payload)

		// Parse header
		header, err := ParseFrameHeader(frame[:6])
		if err != nil {
			t.Fatalf("ParseFrameHeader() error = %v", err)
		}

		// Extract and decode payload
		extractedPayload := frame[6 : 6+header.PayloadLength]
		var decoded map[string]interface{}
		err = DecodeMessage(extractedPayload, header.Encoding, &decoded)
		if err != nil {
			t.Fatalf("DecodeMessage() error = %v", err)
		}

		if header.MsgType != MessageTypeRequest {
			t.Errorf("MsgType = %d, want %d", header.MsgType, MessageTypeRequest)
		}
		if header.Encoding != EncodingJSON {
			t.Errorf("Encoding = %d, want %d", header.Encoding, EncodingJSON)
		}
		if decoded["type"] != msg["type"] {
			t.Errorf("type = %v, want %v", decoded["type"], msg["type"])
		}
	})

	t.Run("MessagePack roundtrip", func(t *testing.T) {
		msg := map[string]interface{}{
			"type":  "result",
			"id":    "resp-456",
			"data":  map[string]interface{}{"count": float64(42)},
		}

		payload, err := EncodeMessage(msg, EncodingMessagePack)
		if err != nil {
			t.Fatalf("EncodeMessage() error = %v", err)
		}

		frame := BuildFrame(MessageTypeResponse, EncodingMessagePack, payload)
		header, err := ParseFrameHeader(frame[:6])
		if err != nil {
			t.Fatalf("ParseFrameHeader() error = %v", err)
		}

		extractedPayload := frame[6 : 6+header.PayloadLength]
		var decoded map[string]interface{}
		err = DecodeMessage(extractedPayload, header.Encoding, &decoded)
		if err != nil {
			t.Fatalf("DecodeMessage() error = %v", err)
		}

		if header.MsgType != MessageTypeResponse {
			t.Errorf("MsgType = %d, want %d", header.MsgType, MessageTypeResponse)
		}
	})
}

func TestClientMessageSerialization(t *testing.T) {
	t.Run("Query message", func(t *testing.T) {
		msg := ClientMessage{
			Type:  "query",
			ID:    "123",
			Query: "db.table(\"users\").run()",
		}

		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		var decoded ClientMessage
		err = json.Unmarshal(data, &decoded)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.Type != msg.Type {
			t.Errorf("Type = %s, want %s", decoded.Type, msg.Type)
		}
		if decoded.ID != msg.ID {
			t.Errorf("ID = %s, want %s", decoded.ID, msg.ID)
		}
		if decoded.Query != msg.Query {
			t.Errorf("Query = %s, want %s", decoded.Query, msg.Query)
		}
	})

	t.Run("Insert message", func(t *testing.T) {
		msg := ClientMessage{
			Type:       "insert",
			ID:         "456",
			Collection: "users",
			Data:       map[string]interface{}{"name": "Alice"},
		}

		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}

		var decoded ClientMessage
		err = json.Unmarshal(data, &decoded)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.Collection != msg.Collection {
			t.Errorf("Collection = %s, want %s", decoded.Collection, msg.Collection)
		}
	})
}

func TestServerMessageSerialization(t *testing.T) {
	t.Run("Result message", func(t *testing.T) {
		jsonData := `{"type":"result","id":"123","data":[{"id":"doc1"}]}`

		var msg ServerMessage
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if msg.Type != "result" {
			t.Errorf("Type = %s, want result", msg.Type)
		}
		if msg.ID != "123" {
			t.Errorf("ID = %s, want 123", msg.ID)
		}
		if msg.Data == nil {
			t.Error("Data should not be nil")
		}
	})

	t.Run("Error message", func(t *testing.T) {
		jsonData := `{"type":"error","id":"123","error":"something went wrong"}`

		var msg ServerMessage
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if msg.Type != "error" {
			t.Errorf("Type = %s, want error", msg.Type)
		}
		if msg.Error != "something went wrong" {
			t.Errorf("Error = %s, want 'something went wrong'", msg.Error)
		}
	})
}
