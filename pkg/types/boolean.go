package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"storemy/pkg/primitives"
)

// BoolField represents a boolean field type in the database.
// It stores a single boolean value and provides operations for
// serialization, comparison, and other field operations.
type BoolField struct {
	Value bool // The boolean value stored in this field
}

// NewBoolField creates a new BoolField instance with the specified boolean value.
// Parameters:
//   - value: The boolean value to store in the field
//
// Returns:
//   - *BoolField: A pointer to the newly created BoolField
func NewBoolField(value bool) *BoolField {
	return &BoolField{Value: value}
}

// Serialize writes the boolean field to the provided writer in binary format.
// Parameters:
//   - w: The io.Writer to write the serialized data to
//
// Returns:
//   - error: An error if the write operation fails, nil otherwise
func (b *BoolField) Serialize(w io.Writer) error {
	var byteValue byte

	if b.Value {
		byteValue = 1
	} else {
		byteValue = 0
	}

	return binary.Write(w, binary.BigEndian, byteValue)
}

// Compare performs a comparison operation between this BoolField and another Field
//
// Parameters:
//   - op: The comparison predicate to apply
//   - other: The other Field to compare against (must be a *BoolField)
//
// Returns:
//   - bool: The result of the comparison operation
//   - error: An error if the other field is not a BoolField or if an unsupported predicate is used
func (b *BoolField) Compare(op primitives.Predicate, other Field) (bool, error) {
	a, ok := other.(*BoolField)
	if !ok {
		return false, fmt.Errorf("cannot compare BoolField with %T", other)
	}

	switch op {
	case primitives.Equals:
		return b.Value == a.Value, nil
	case primitives.NotEqual:
		return b.Value != a.Value, nil
	case primitives.LessThan:
		return !b.Value && a.Value, nil
	case primitives.GreaterThan:
		return b.Value && !a.Value, nil
	case primitives.LessThanOrEqual:
		return !b.Value || a.Value, nil
	case primitives.GreaterThanOrEqual:
		return b.Value || !a.Value, nil
	default:
		return false, fmt.Errorf("unsupported predicate for BoolField: %v", op)
	}
}

// Type returns the type identifier for this field.
// Returns:
//   - Type: Always returns BoolType for boolean fields
func (b *BoolField) Type() Type {
	return BoolType
}

// String returns a string representation of the boolean value.
// Returns:
//   - string: "true" if the value is true, "false" otherwise
func (b *BoolField) String() string {
	if b.Value {
		return "true"
	}
	return "false"
}

// Equals checks if this BoolField is equal to another Field.
// Parameters:
//   - other: The other Field to compare for equality
//
// Returns:
//   - bool: true if both fields are BoolFields with the same value, false otherwise
func (b *BoolField) Equals(other Field) bool {
	otherBool, ok := other.(*BoolField)
	if !ok {
		return false
	}
	return b.Value == otherBool.Value
}

// Hash returns a hash value for this boolean field using FNV-1a hashing.
//
// Returns:
//   - primitives.HashCode: The FNV-1a hash of the boolean value
//   - error: Always returns nil for boolean fields
func (b *BoolField) Hash() (primitives.HashCode, error) {
	if b.Value {
		return fnvHash([]byte{1}), nil
	}
	return fnvHash([]byte{0}), nil
}

// Length returns the serialized size of this boolean field in bytes.
//
// Returns:
//   - uint32: Always returns 1 (the size in bytes)
func (b *BoolField) Length() uint32 {
	return 1
}
