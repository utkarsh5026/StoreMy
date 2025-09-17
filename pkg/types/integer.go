package types

import (
	"encoding/binary"
	"io"
	"strconv"
)

type IntField struct {
	Value int32
}

// NewIntField creates a new IntField with the given integer value
func NewIntField(value int32) *IntField {
	return &IntField{Value: value}
}

// Serialize writes the integer value to the given writer in big-endian format
func (f *IntField) Serialize(w io.Writer) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(f.Value))
	_, err := w.Write(bytes)
	return err
}

// Compare compares this integer field with another field based on the given predicate
func (f *IntField) Compare(op Predicate, other Field) (bool, error) {
	otherIntField, ok := other.(*IntField)
	if !ok {
		return false, nil
	}

	switch op {
	case Equals:
		return f.Value == otherIntField.Value, nil

	case LessThan:
		return f.Value < otherIntField.Value, nil

	case GreaterThan:
		return f.Value > otherIntField.Value, nil

	case LessThanOrEqual:
		return f.Value <= otherIntField.Value, nil

	case GreaterThanOrEqual:
		return f.Value >= otherIntField.Value, nil

	case NotEqual:
		return f.Value != otherIntField.Value, nil

	default:
		return false, nil
	}
}

// Type returns IntType
func (f *IntField) Type() Type {
	return IntType
}

// String returns string representation of the integer
func (f *IntField) String() string {
	return strconv.FormatInt(int64(f.Value), 10)
}

// Equals checks if this integer field is equal to another field
func (f *IntField) Equals(other Field) bool {
	otherInt, ok := other.(*IntField)
	if !ok {
		return false
	}
	return f.Value == otherInt.Value
}

// Hash returns a hash code for this integer field
func (f *IntField) Hash() (uint32, error) {
	return uint32(f.Value), nil
}
