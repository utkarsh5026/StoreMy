package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// ParseField reads and parses a field from the given reader based on the specified field type.
// This function acts as a dispatcher to the appropriate type-specific parsing function.
//
// Parameters:
//   - r: The io.Reader to read the serialized field data from
//   - fieldType: The Type of field to parse (IntType, StringType, BoolType, or FloatType)
//
// Returns:
//   - Field: The parsed field instance of the appropriate type
//   - error: An error if the field type is unsupported, has invalid size, or parsing fails
func ParseField(r io.Reader, fieldType Type) (Field, error) {
	size := fieldType.Size()
	if size == 0 {
		return nil, fmt.Errorf("invalid field type size: %v", fieldType)
	}

	switch fieldType {
	case IntType:
		return parseIntField(r, size)

	case Int32Type:
		return parseInt32Field(r, size)

	case Int64Type:
		return parseInt64Field(r, size)

	case Uint32Type:
		return parseUint32Field(r, size)

	case Uint64Type:
		return parseUint64Field(r, size)

	case StringType:
		return parseStringField(r)

	case BoolType:
		return parseBoolField(r)

	case FloatType:
		return parseFloat64Field(r, size)

	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

// parseIntField reads and parses an integer field from the reader.
func parseIntField(r io.Reader, maxSize uint32) (*IntField, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	value := int64(binary.BigEndian.Uint64(bytes)) // #nosec G115
	return NewIntField(value), nil
}

// parseStringField reads and parses a string field from the reader.
// The string is expected to be serialized in the format:
// 1. 4 bytes for the actual string length (big-endian uint32)
// 2. The string bytes (up to the specified length)
// 3. Padding bytes to reach the StringMaxSize limit
func parseStringField(r io.Reader) (*StringField, error) {
	lengthBytes, err := readBytes(r, 4)
	if err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBytes))

	strBytes, err := readBytes(r, uint32(length)) // #nosec G115
	if err != nil {
		return nil, err
	}

	paddingSize := StringMaxSize - length
	if _, err := readBytes(r, uint32(paddingSize)); err != nil { // #nosec G115
		return nil, err
	}

	return NewStringField(string(strBytes), StringMaxSize), nil
}

// parseBoolField reads and parses a boolean field from the reader.
func parseBoolField(r io.Reader) (*BoolField, error) {
	bytes, err := readBytes(r, 1)
	if err != nil {
		return nil, err
	}
	value := bytes[0] != 0
	return NewBoolField(value), nil
}

// parseInt32Field reads and parses a 32-bit signed integer field from the reader.
func parseInt32Field(r io.Reader, maxSize uint32) (*Int32Field, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	value := int32(binary.BigEndian.Uint32(bytes)) // #nosec G115
	return NewInt32Field(value), nil
}

// parseInt64Field reads and parses a 64-bit signed integer field from the reader.
func parseInt64Field(r io.Reader, maxSize uint32) (*Int64Field, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	value := int64(binary.BigEndian.Uint64(bytes)) // #nosec G115
	return NewInt64Field(value), nil
}

// parseUint32Field reads and parses a 32-bit unsigned integer field from the reader.
func parseUint32Field(r io.Reader, maxSize uint32) (*Uint32Field, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	value := binary.BigEndian.Uint32(bytes)
	return NewUint32Field(value), nil
}

// parseUint64Field reads and parses a 64-bit unsigned integer field from the reader.
func parseUint64Field(r io.Reader, maxSize uint32) (*Uint64Field, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	value := binary.BigEndian.Uint64(bytes)
	return NewUint64Field(value), nil
}

// parseFloat64Field reads and parses a 64-bit floating-point field from the reader.
func parseFloat64Field(r io.Reader, maxSize uint32) (*Float64Field, error) {
	bytes, err := readBytes(r, maxSize)
	if err != nil {
		return nil, err
	}
	bits := binary.BigEndian.Uint64(bytes)
	value := math.Float64frombits(bits)
	return NewFloat64Field(value), nil
}
