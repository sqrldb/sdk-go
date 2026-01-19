package squirreldb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Protocol constants
const (
	ProtocolVersion = 0x01
	MaxMessageSize  = 16 * 1024 * 1024 // 16MB
)

// Magic bytes for handshake
var Magic = []byte{'S', 'Q', 'R', 'L'}

// HandshakeStatus represents handshake response status codes.
type HandshakeStatus byte

const (
	HandshakeSuccess        HandshakeStatus = 0x00
	HandshakeVersionMismatch HandshakeStatus = 0x01
	HandshakeAuthFailed     HandshakeStatus = 0x02
)

// MessageType represents message type codes.
type MessageType byte

const (
	MessageTypeRequest      MessageType = 0x01
	MessageTypeResponse     MessageType = 0x02
	MessageTypeNotification MessageType = 0x03
)

// Encoding represents serialization encoding codes.
type Encoding byte

const (
	EncodingMessagePack Encoding = 0x01
	EncodingJSON        Encoding = 0x02
)

// ProtocolFlags represents handshake protocol flags.
type ProtocolFlags struct {
	MessagePack  bool
	JSONFallback bool
}

// ToByte converts flags to a byte.
func (f ProtocolFlags) ToByte() byte {
	var b byte
	if f.MessagePack {
		b |= 0x01
	}
	if f.JSONFallback {
		b |= 0x02
	}
	return b
}

// FlagsFromByte creates flags from a byte.
func FlagsFromByte(b byte) ProtocolFlags {
	return ProtocolFlags{
		MessagePack:  b&0x01 != 0,
		JSONFallback: b&0x02 != 0,
	}
}

// BuildHandshake builds a handshake packet to send to server.
func BuildHandshake(authToken string, flags ProtocolFlags) []byte {
	tokenBytes := []byte(authToken)
	buf := make([]byte, 8+len(tokenBytes))

	// Magic
	copy(buf[0:4], Magic)
	// Version
	buf[4] = ProtocolVersion
	// Flags
	buf[5] = flags.ToByte()
	// Token length (big-endian)
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(tokenBytes)))
	// Token
	copy(buf[8:], tokenBytes)

	return buf
}

// HandshakeResponse represents parsed handshake response.
type HandshakeResponse struct {
	Status    HandshakeStatus
	Version   byte
	Flags     ProtocolFlags
	SessionID [16]byte
}

// ParseHandshakeResponse parses handshake response from server.
func ParseHandshakeResponse(data []byte) (*HandshakeResponse, error) {
	if len(data) < 19 {
		return nil, fmt.Errorf("handshake response too short: %d bytes", len(data))
	}

	var sessionID [16]byte
	copy(sessionID[:], data[3:19])

	return &HandshakeResponse{
		Status:    HandshakeStatus(data[0]),
		Version:   data[1],
		Flags:     FlagsFromByte(data[2]),
		SessionID: sessionID,
	}, nil
}

// EncodeMessage encodes a message using the specified encoding.
func EncodeMessage(msg interface{}, encoding Encoding) ([]byte, error) {
	if encoding == EncodingMessagePack {
		return msgpack.Marshal(msg)
	}
	return json.Marshal(msg)
}

// DecodeMessage decodes a message using the specified encoding.
func DecodeMessage(data []byte, encoding Encoding, v interface{}) error {
	if encoding == EncodingMessagePack {
		return msgpack.Unmarshal(data, v)
	}
	return json.Unmarshal(data, v)
}

// BuildFrame builds a framed message.
func BuildFrame(msgType MessageType, encoding Encoding, payload []byte) []byte {
	length := uint32(len(payload) + 2) // +2 for type and encoding bytes

	buf := make([]byte, 6+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(msgType)
	buf[5] = byte(encoding)
	copy(buf[6:], payload)

	return buf
}

// FrameHeader represents parsed frame header.
type FrameHeader struct {
	PayloadLength uint32
	MsgType       MessageType
	Encoding      Encoding
}

// ParseFrameHeader parses frame header.
func ParseFrameHeader(data []byte) (*FrameHeader, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("frame header too short: %d bytes", len(data))
	}

	length := binary.BigEndian.Uint32(data[0:4])
	payloadLength := length - 2

	return &FrameHeader{
		PayloadLength: payloadLength,
		MsgType:       MessageType(data[4]),
		Encoding:      Encoding(data[5]),
	}, nil
}

// UUIDToString converts UUID bytes to string.
func UUIDToString(b [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
