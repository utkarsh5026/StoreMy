package types

import (
	"encoding/binary"
	"io"
	"storemy/pkg/primitives"
	"strconv"
)

type IntField struct {
	Value int64
}

// NewIntField creates a new IntField with the given integer value
func NewIntField(value int64) *IntField {
	return &IntField{Value: value}
}

// Serialize writes the integer value to the given writer in big-endian format
func (f *IntField) Serialize(w io.Writer) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(f.Value))
	_, err := w.Write(bytes)
	return err
}

// Compare compares this integer field with another field based on the given predicate
func (f *IntField) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherIntField, ok := other.(*IntField)
	if !ok {
		return false, nil
	}

	switch op {
	case primitives.Equals:
		return f.Value == otherIntField.Value, nil

	case primitives.LessThan:
		return f.Value < otherIntField.Value, nil

	case primitives.GreaterThan:
		return f.Value > otherIntField.Value, nil

	case primitives.LessThanOrEqual:
		return f.Value <= otherIntField.Value, nil

	case primitives.GreaterThanOrEqual:
		return f.Value >= otherIntField.Value, nil

	case primitives.NotEqual:
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
	return strconv.FormatInt(f.Value, 10)
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
	// Use upper 32 bits XOR lower 32 bits for hash
	return uint32(f.Value>>32) ^ uint32(f.Value), nil
}

// Length returns the length of the integer field in bytes (always 8 bytes for int64)
func (f *IntField) Length() uint32 {
	return 8
}
