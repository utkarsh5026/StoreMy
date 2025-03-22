package fields

import (
	"encoding/binary"
	"strconv"
)

type IntField struct {
	value int
}

// NewIntField creates a new IntField with the given value
func NewIntField(value int) *IntField {
	return &IntField{value: value}
}

// Type returns the type of this field (IntType)
func (f *IntField) Type() Type {
	return IntType
}

// String returns a string representation of this field
func (f *IntField) String() string {
	return strconv.Itoa(f.value)
}

// Compare compares this field with another using the given predicate
func (f *IntField) Compare(op Predicate, other Field) (bool, error) {
	if other.Type() != IntType {
		return false, ErrTypeMismatch
	}
	otherValue := other.(*IntField).value
	switch op {
	case Equals:
		return f.value == otherValue, nil
	case NotEquals:
		return f.value != otherValue, nil
	case GreaterThan:
		return f.value > otherValue, nil
	case LessThan:
		return f.value < otherValue, nil
	case GreaterThanOrEqual:
		return f.value >= otherValue, nil
	case LessThanOrEqual:
		return f.value <= otherValue, nil
	default:
		return false, ErrUnsupportedPredicate
	}
}

// Serialize converts this field to a byte slice for storage
// Uses big-endian encoding for integers
func (f *IntField) Serialize() []byte {
	bytes := make([]byte, 4) // 32-bit integer
	binary.BigEndian.PutUint32(bytes, uint32(f.value))
	return bytes
}

// GetValue returns the integer value of this field
func (f *IntField) GetValue() int {
	return f.value
}

// DeserializeIntField creates an IntField from a serialized byte slice
func DeserializeIntField(bytes []byte) (*IntField, error) {
	if len(bytes) != 4 {
		return nil, ErrInvalidDataType
	}

	value := int(binary.BigEndian.Uint32(bytes))
	return NewIntField(value), nil
}
