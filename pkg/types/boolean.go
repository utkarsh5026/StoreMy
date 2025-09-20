package types

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BoolField struct {
	Value bool
}

func NewBoolField(value bool) *BoolField {
	return &BoolField{Value: value}
}

func (b *BoolField) Serialize(w io.Writer) error {
	var byteValue byte

	if b.Value {
		byteValue = 1
	} else {
		byteValue = 0
	}

	return binary.Write(w, binary.BigEndian, byteValue)
}

func (b *BoolField) Compare(op Predicate, other Field) (bool, error) {
	a, ok := other.(*BoolField)
	if !ok {
		return false, fmt.Errorf("cannot compare BoolField with %T", other)
	}

	switch op {
	case Equals:
		return b.Value == a.Value, nil
	case NotEqual:
		return b.Value != a.Value, nil
	case LessThan:
		return !b.Value && a.Value, nil
	case GreaterThan:
		return b.Value && !a.Value, nil
	case LessThanOrEqual:
		return !b.Value || a.Value, nil
	case GreaterThanOrEqual:
		return b.Value || !a.Value, nil
	default:
		return false, fmt.Errorf("unsupported predicate for BoolField: %v", op)
	}
}

func (b *BoolField) Type() Type {
	return BoolType
}

func (b *BoolField) String() string {
	if b.Value {
		return "true"
	}
	return "false"
}

func (b *BoolField) Equals(other Field) bool {
	otherBool, ok := other.(*BoolField)
	if !ok {
		return false
	}
	return b.Value == otherBool.Value
}

func (b *BoolField) Hash() (uint32, error) {
	if b.Value {
		return 1, nil
	}
	return 0, nil
}

func (b *BoolField) Length() uint32 {
	return 1
}
