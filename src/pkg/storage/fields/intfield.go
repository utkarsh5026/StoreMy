package fields

import (
	"encoding/binary"
	"strconv"
)

type IntField struct {
	value int
}

func NewIntField(value int) *IntField {
	return &IntField{value: value}
}

func (itf *IntField) Type() Type {
	return IntType
}

// String returns the string representation of the IntField. (e.g. "123")
func (itf *IntField) String() string {
	return strconv.Itoa(itf.value)
}

// Compare compares the value of the IntField with another Field based on the given Predicate.
// If the other Field is not of type IntType, it returns false and a TypeMismatch error.
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
func (itf *IntField) Compare(p Predicate, other Field) (bool, error) {
	if other.Type() != IntType {
		return false, TypeMismatch
	}

	othValue := other.(*IntField).value
	switch p {
	case Equals:
		return itf.value == othValue, nil

	case NotEquals:
		return itf.value != othValue, nil

	case GreaterThan:
		return itf.value > othValue, nil

	case LessThan:
		return itf.value < othValue, nil

	case GreaterThanOrEqual:
		return itf.value >= othValue, nil

	case LessThanOrEqual:
		return itf.value <= othValue, nil

	default:
		return false, UnSupportedPredicate
	}
}

// Serialize converts the value of the IntField to a byte slice using "big-endian" encoding.
//
// Returns:
//
//	[]byte - the byte slice representation of the IntField value
func (itf *IntField) Serialize() []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(itf.value))
	return bytes
}

// GetValue returns the value of the IntField.
func (itf *IntField) GetValue() int {
	return itf.value
}

func deserializeIntField(bytes []byte) (*IntField, error) {
	if len(bytes) != 4 {
		return nil, InvalidDataType
	}

	value := int(binary.BigEndian.Uint32(bytes))
	return NewIntField(value), nil
}
