package fields

import "strings"

type StringField struct {
	value     string
	maxLength int
}

// NewStringField creates a new StringField with the given value and maximum length
// If the string exceeds maxLength, it will be truncated
func NewStringField(value string, maxLength int) *StringField {
	if len(value) > maxLength {
		value = value[:maxLength]
	}

	return &StringField{
		value:     value,
		maxLength: maxLength,
	}
}

// Type returns the type of this field (StringType)
func (f *StringField) Type() Type {
	return StringType
}

// String returns a string representation of this field
func (f *StringField) String() string {
	return f.value
}

// Compare compares this field with another using the given predicate
func (f *StringField) Compare(op Predicate, other Field) (bool, error) {
	if other.Type() != StringType {
		return false, ErrTypeMismatch
	}

	otherValue := other.(*StringField).value

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
	case Like:
		return strings.Contains(f.value, otherValue), nil
	default:
		return false, ErrUnsupportedPredicate
	}
}

// Serialize converts this field to a byte slice for storage
func (f *StringField) Serialize() []byte {
	bytes := make([]byte, f.maxLength)
	copy(bytes, f.value)
	return bytes
}

// GetValue returns the string value of this field
func (f *StringField) GetValue() string {
	return f.value
}

// GetMaxLength returns the maximum length of this string field
func (f *StringField) GetMaxLength() int {
	return f.maxLength
}

// DeserializeStringField creates a StringField from a serialized byte slice
func DeserializeStringField(bytes []byte, maxLength int) *StringField {
	value := string(bytes)

	if len(value) > maxLength {
		value = value[:maxLength]
	}

	return &StringField{
		value:     value,
		maxLength: maxLength,
	}
}
