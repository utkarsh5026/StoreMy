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
		return parseStringField(r)

	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

func parseIntField(r io.Reader, maxSize uint32) (*IntField, error) {
	bytes := make([]byte, maxSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}

	value := int32(binary.BigEndian.Uint32(bytes))
	return NewIntField(value), nil
}

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
