package squirreldb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RESP protocol errors
var (
	ErrInvalidResp     = errors.New("invalid RESP response")
	ErrUnexpectedType  = errors.New("unexpected RESP type")
	ErrNilResponse     = errors.New("nil response")
	ErrProtocolError   = errors.New("RESP protocol error")
)

// RESP type prefixes
const (
	respSimpleString = '+'
	respError        = '-'
	respInteger      = ':'
	respBulkString   = '$'
	respArray        = '*'
)

// RespValue represents a parsed RESP value
type RespValue struct {
	Type    byte
	Str     string
	Int     int64
	Array   []RespValue
	IsNull  bool
	Err     error
}

// encodeCommand encodes a RESP array command
func encodeCommand(args ...string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, '*')
	buf = append(buf, []byte(strconv.Itoa(len(args)))...)
	buf = append(buf, '\r', '\n')

	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, []byte(strconv.Itoa(len(arg)))...)
		buf = append(buf, '\r', '\n')
		buf = append(buf, []byte(arg)...)
		buf = append(buf, '\r', '\n')
	}

	return buf
}

// readResp reads and parses a RESP response
func readResp(r *bufio.Reader) (RespValue, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return RespValue{}, fmt.Errorf("read line: %w", err)
	}

	if len(line) < 3 {
		return RespValue{}, ErrInvalidResp
	}

	// Remove \r\n
	line = line[:len(line)-2]

	prefix := line[0]
	data := line[1:]

	switch prefix {
	case respSimpleString:
		return RespValue{Type: respSimpleString, Str: data}, nil

	case respError:
		return RespValue{Type: respError, Err: errors.New(data)}, nil

	case respInteger:
		n, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return RespValue{}, fmt.Errorf("parse integer: %w", err)
		}
		return RespValue{Type: respInteger, Int: n}, nil

	case respBulkString:
		length, err := strconv.Atoi(data)
		if err != nil {
			return RespValue{}, fmt.Errorf("parse bulk length: %w", err)
		}

		if length == -1 {
			return RespValue{Type: respBulkString, IsNull: true}, nil
		}

		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return RespValue{}, fmt.Errorf("read bulk string: %w", err)
		}

		return RespValue{Type: respBulkString, Str: string(buf[:length])}, nil

	case respArray:
		count, err := strconv.Atoi(data)
		if err != nil {
			return RespValue{}, fmt.Errorf("parse array count: %w", err)
		}

		if count == -1 {
			return RespValue{Type: respArray, IsNull: true}, nil
		}

		arr := make([]RespValue, count)
		for i := 0; i < count; i++ {
			val, err := readResp(r)
			if err != nil {
				return RespValue{}, fmt.Errorf("read array element %d: %w", i, err)
			}
			arr[i] = val
		}

		return RespValue{Type: respArray, Array: arr}, nil

	default:
		return RespValue{}, fmt.Errorf("%w: unknown prefix %c", ErrInvalidResp, prefix)
	}
}

// asString extracts string value from RespValue
func (v RespValue) asString() (string, error) {
	if v.Err != nil {
		return "", v.Err
	}
	if v.IsNull {
		return "", ErrNilResponse
	}
	switch v.Type {
	case respSimpleString, respBulkString:
		return v.Str, nil
	default:
		return "", fmt.Errorf("%w: expected string, got %c", ErrUnexpectedType, v.Type)
	}
}

// asInt extracts integer value from RespValue
func (v RespValue) asInt() (int64, error) {
	if v.Err != nil {
		return 0, v.Err
	}
	if v.Type != respInteger {
		return 0, fmt.Errorf("%w: expected integer, got %c", ErrUnexpectedType, v.Type)
	}
	return v.Int, nil
}

// asBool extracts boolean from integer RespValue (0 = false, 1 = true)
func (v RespValue) asBool() (bool, error) {
	n, err := v.asInt()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// asStringSlice extracts string slice from array RespValue
func (v RespValue) asStringSlice() ([]string, error) {
	if v.Err != nil {
		return nil, v.Err
	}
	if v.IsNull {
		return nil, nil
	}
	if v.Type != respArray {
		return nil, fmt.Errorf("%w: expected array, got %c", ErrUnexpectedType, v.Type)
	}

	result := make([]string, len(v.Array))
	for i, elem := range v.Array {
		if elem.IsNull {
			result[i] = ""
			continue
		}
		s, err := elem.asString()
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		result[i] = s
	}
	return result, nil
}

// asNullableStringSlice extracts string slice with nil markers for null values
func (v RespValue) asNullableStringSlice() ([]string, []bool, error) {
	if v.Err != nil {
		return nil, nil, v.Err
	}
	if v.IsNull {
		return nil, nil, nil
	}
	if v.Type != respArray {
		return nil, nil, fmt.Errorf("%w: expected array, got %c", ErrUnexpectedType, v.Type)
	}

	result := make([]string, len(v.Array))
	isNull := make([]bool, len(v.Array))
	for i, elem := range v.Array {
		if elem.IsNull {
			isNull[i] = true
			continue
		}
		s, err := elem.asString()
		if err != nil {
			return nil, nil, fmt.Errorf("element %d: %w", i, err)
		}
		result[i] = s
	}
	return result, isNull, nil
}

// asOK checks if response is OK
func (v RespValue) asOK() error {
	if v.Err != nil {
		return v.Err
	}
	s, err := v.asString()
	if err != nil {
		return err
	}
	if s != "OK" {
		return fmt.Errorf("%w: expected OK, got %s", ErrUnexpectedType, s)
	}
	return nil
}
