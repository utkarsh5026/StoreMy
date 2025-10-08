package types

import (
	"encoding/binary"
	"io"
	"storemy/pkg/primitives"
	"strings"
)

// StringMaxSize defines the default maximum size for string fields in bytes.
const (
	StringMaxSize = 128
)

// StringField represents a variable-length string field type in the database.
type StringField struct {
	Value   string // The string value stored in this field
	MaxSize int    // The maximum allowed size for this string field in bytes
}

// NewStringField creates a new StringField instance with the specified string value and maximum size.
// If the provided value exceeds the maximum size, it will be truncated to fit.
//
// Parameters:
//   - value: The string value to store in the field
//   - maxSize: The maximum allowed size for the string in bytes
//
// Returns:
//   - *StringField: A pointer to the newly created StringField
func NewStringField(value string, maxSize int) *StringField {
	if len(value) > maxSize {
		value = value[:maxSize]
	}

	return &StringField{
		Value:   value,
		MaxSize: maxSize,
	}
}

// Compare performs a comparison operation between this StringField and another Field
// using the specified predicate. String comparisons are performed lexicographically.
//
// Parameters:
//   - op: The comparison predicate to apply (supports all standard predicates plus Like)
//   - other: The other Field to compare against (must be a *StringField)
//
// Returns:
//   - bool: The result of the comparison operation
//   - error: An error if the other field is not a StringField or if comparison fails
func (s *StringField) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherStringField, ok := other.(*StringField)
	if !ok {
		return false, nil
	}

	cmp := strings.Compare(s.Value, otherStringField.Value)

	switch op {
	case primitives.Equals:
		return cmp == 0, nil

	case primitives.LessThan:
		return cmp < 0, nil

	case primitives.GreaterThan:
		return cmp > 0, nil

	case primitives.LessThanOrEqual:
		return cmp <= 0, nil

	case primitives.GreaterThanOrEqual:
		return cmp >= 0, nil

	case primitives.NotEqual:
		return cmp != 0, nil

	case primitives.Like:
		return strings.Contains(s.Value, otherStringField.Value), nil
	default:
		return false, nil
	}
}

// Serialize writes the string field to the provided writer in binary format.
// The serialization format consists of:
// 1. 4 bytes for the actual string length (big-endian uint32)
// 2. The string bytes (up to MaxSize)
// 3. Padding bytes to reach the MaxSize limit
//
// Parameters:
//   - w: The io.Writer to write the serialized data to
//
// Returns:
//   - error: An error if any write operation fails, nil otherwise
func (s *StringField) Serialize(w io.Writer) error {
	length := len(s.Value)
	if length > s.MaxSize {
		length = s.MaxSize
	}

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))

	if _, err := w.Write(lengthBytes); err != nil {
		return err
	}

	if _, err := w.Write([]byte(s.Value[:length])); err != nil {
		return err
	}

	padding := make([]byte, s.MaxSize-length)
	_, err := w.Write(padding)
	return err
}

// Type returns the type identifier for this field.
func (s *StringField) Type() Type {
	return StringType
}

// String returns the string value stored in this field.
func (s *StringField) String() string {
	return s.Value
}

// Equals checks if this StringField is equal to another Field.
// Parameters:
//   - other: The other Field to compare for equality
//
// Returns:
//   - bool: true if both fields are StringFields with the same value and MaxSize, false otherwise
func (s *StringField) Equals(other Field) bool {
	otherStringField, ok := other.(*StringField)
	if !ok {
		return false
	}
	return s.Value == otherStringField.Value && s.MaxSize == otherStringField.MaxSize
}

// Hash returns a hash value for this string field using a simple polynomial hash function.
//
// Returns:
//   - uint32: The computed hash value for the string
//   - error: Always returns nil for string fields
func (s *StringField) Hash() (uint32, error) {
	hash := 0
	for _, c := range s.Value {
		hash = 31*hash + int(c)
	}
	return uint32(hash), nil
}

// Length returns the total serialized size of this string field in bytes.
//
// Returns:
//   - uint32: The total serialized size (4 bytes + StringMaxSize)
func (s *StringField) Length() uint32 {
	return 4 + StringMaxSize
}
