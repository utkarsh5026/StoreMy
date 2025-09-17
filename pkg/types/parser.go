package types

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ParseField(r io.Reader, fieldType Type) (Field, error) {
	size := fieldType.Size()
	if size == 0 {
		return nil, fmt.Errorf("invalid field type size: %v", fieldType)
	}

	switch fieldType {
	case IntType:
		return parseIntField(r, size)

	case StringType:
		return parseStringField(r, size)

	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

func parseIntField(r io.Reader, maxSize uint32) (*IntField, error) {
	bytes := make([]byte, maxSize)
	if _, err := r.Read(bytes); err != nil {
		return nil, err
	}

	value := int32(binary.BigEndian.Uint32(bytes))
	return NewIntField(value), nil
}

func parseStringField(r io.Reader, maxSize uint32) (*StringField, error) {
	lengthBytes := make([]byte, 4)
	if _, err := r.Read(lengthBytes); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBytes))
	if length > int(maxSize) {
		length = int(maxSize)
	}

	strBytes := make([]byte, length)
	if _, err := r.Read(strBytes); err != nil {
		return nil, err
	}

	return NewStringField(string(strBytes), int(maxSize)), nil
}
