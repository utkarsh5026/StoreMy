package fields

import (
	"strings"
)

type StringField struct {
	value     string
	maxLength int
}

// NewStringField creates a new StringField
func NewStringField(value string, maxLength int) *StringField {
	// Truncate the string if it exceeds maxLength
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return &StringField{
		value:     value,
		maxLength: maxLength,
	}
}

func (sf *StringField) Type() Type {
	return StringType
}

func (sf *StringField) String() string {
	return sf.value
}

// Compare compares the value of the StringField with another Field based on the given Predicate.
// If the other Field is not of type StringType, it returns false and a TypeMismatch error.
//
// Parameters:
//
//	p - the Predicate to use for comparison
//	other - the Field to compare with
//
// Returns:
//
//	bool - the result of the comparison
//	error - an error if the comparison cannot be performed
func (sf *StringField) Compare(p Predicate, other Field) (bool, error) {
	if other.Type() != StringType {
		return false, TypeMismatch
	}

	othValue := other.(*StringField).value
	switch p {
	case Equals:
		return sf.value == othValue, nil

	case NotEquals:
		return sf.value != othValue, nil

	case GreaterThan:
		return sf.value > othValue, nil

	case LessThan:
		return sf.value < othValue, nil

	case GreaterThanOrEqual:
		return sf.value >= othValue, nil

	case LessThanOrEqual:
		return sf.value <= othValue, nil

	case Like:
		return strings.Contains(sf.value, othValue), nil

	default:
		return false, UnSupportedPredicate
	}
}

// Serialize converts the value of the StringField to a byte slice.
// The byte slice will have a length equal to the maxLength of the StringField.
// If the value is shorter than maxLength, the remaining bytes will be zero.
//
// Returns:
//
//	[]byte - the byte slice representation of the StringField value
func (sf *StringField) Serialize() []byte {
	bytes := make([]byte, sf.maxLength)
	copy(bytes, sf.value)
	return bytes
}

// GetMaxLength returns the maximum length of the StringField
func (sf *StringField) GetMaxLength() int {
	return sf.maxLength
}

func (sf *StringField) GetValue() string {
	return sf.value
}

func deserializeStringField(bytes []byte, maxLength int) *StringField {
	value := string(bytes)
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return &StringField{
		value:     value,
		maxLength: maxLength,
	}
}
