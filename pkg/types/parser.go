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
// The integer is expected to be serialized as a big-endian uint32.
//
// Parameters:
//   - r: The io.Reader to read the serialized integer data from
//   - maxSize: The maximum size in bytes for the integer field (should be 4 for int32)
//
// Returns:
//   - *IntField: The parsed IntField instance
//   - error: An error if reading fails or if the data is incomplete
func parseIntField(r io.Reader, maxSize uint32) (*IntField, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := int64(binary.BigEndian.Uint64(bytes))
	return NewIntField(value), nil
}

// parseStringField reads and parses a string field from the reader.
// The string is expected to be serialized in the format:
// 1. 4 bytes for the actual string length (big-endian uint32)
// 2. The string bytes (up to the specified length)
// 3. Padding bytes to reach the StringMaxSize limit
//
// Parameters:
//   - r: The io.Reader to read the serialized string data from
//
// Returns:
//   - *StringField: The parsed StringField instance with StringMaxSize as maximum size
//   - error: An error if reading fails or if the data is incomplete
func parseStringField(r io.Reader) (*StringField, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBytes))

	strBytes := make([]byte, length)
	if _, err := io.ReadFull(r, strBytes); err != nil {
		return nil, err
	}

	paddingSize := StringMaxSize - length
	padding := make([]byte, paddingSize)
	if _, err := io.ReadFull(r, padding); err != nil {
		return nil, err
	}

	return NewStringField(string(strBytes), StringMaxSize), nil
}

// parseBoolField reads and parses a boolean field from the reader.
// The boolean is expected to be serialized as a single byte (0 for false, non-zero for true).
//
// Parameters:
//   - r: The io.Reader to read the serialized boolean data from
//
// Returns:
//   - *BoolField: The parsed BoolField instance
//   - error: An error if reading fails or if the data is incomplete
func parseBoolField(r io.Reader) (*BoolField, error) {
	bytes := make([]byte, 1)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := bytes[0] != 0
	return NewBoolField(value), nil
}

// parseInt32Field reads and parses a 32-bit signed integer field from the reader.
// The integer is expected to be serialized as a big-endian int32.
//
// Parameters:
//   - r: The io.Reader to read the serialized integer data from
//   - maxSize: The maximum size in bytes for the integer field (should be 4 for int32)
//
// Returns:
//   - *Int32Field: The parsed Int32Field instance
//   - error: An error if reading fails or if the data is incomplete
func parseInt32Field(r io.Reader, maxSize uint32) (*Int32Field, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := int32(binary.BigEndian.Uint32(bytes))
	return NewInt32Field(value), nil
}

// parseInt64Field reads and parses a 64-bit signed integer field from the reader.
// The integer is expected to be serialized as a big-endian int64.
//
// Parameters:
//   - r: The io.Reader to read the serialized integer data from
//   - maxSize: The maximum size in bytes for the integer field (should be 8 for int64)
//
// Returns:
//   - *Int64Field: The parsed Int64Field instance
//   - error: An error if reading fails or if the data is incomplete
func parseInt64Field(r io.Reader, maxSize uint32) (*Int64Field, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := int64(binary.BigEndian.Uint64(bytes))
	return NewInt64Field(value), nil
}

// parseUint32Field reads and parses a 32-bit unsigned integer field from the reader.
// The integer is expected to be serialized as a big-endian uint32.
//
// Parameters:
//   - r: The io.Reader to read the serialized integer data from
//   - maxSize: The maximum size in bytes for the integer field (should be 4 for uint32)
//
// Returns:
//   - *Uint32Field: The parsed Uint32Field instance
//   - error: An error if reading fails or if the data is incomplete
func parseUint32Field(r io.Reader, maxSize uint32) (*Uint32Field, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := binary.BigEndian.Uint32(bytes)
	return NewUint32Field(value), nil
}

// parseUint64Field reads and parses a 64-bit unsigned integer field from the reader.
// The integer is expected to be serialized as a big-endian uint64.
//
// Parameters:
//   - r: The io.Reader to read the serialized integer data from
//   - maxSize: The maximum size in bytes for the integer field (should be 8 for uint64)
//
// Returns:
//   - *Uint64Field: The parsed Uint64Field instance
//   - error: An error if reading fails or if the data is incomplete
func parseUint64Field(r io.Reader, maxSize uint32) (*Uint64Field, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := binary.BigEndian.Uint64(bytes)
	return NewUint64Field(value), nil
}

// parseFloat64Field reads and parses a 64-bit floating-point field from the reader.
// The float is expected to be serialized as an 8-byte IEEE 754 double-precision
// floating-point number in big-endian byte order.
//
// Parameters:
//   - r: The io.Reader to read the serialized float data from
//   - maxSize: The maximum size in bytes for the float field (should be 8 for float64)
//
// Returns:
//   - *Float64Field: The parsed Float64Field instance
//   - error: An error if reading fails or if the data is incomplete
func parseFloat64Field(r io.Reader, maxSize uint32) (*Float64Field, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	bits := binary.BigEndian.Uint64(bytes)
	value := math.Float64frombits(bits)
	return NewFloat64Field(value), nil
}
