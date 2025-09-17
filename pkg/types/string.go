package types

import (
	"encoding/binary"
	"io"
	"strings"
)

type StringField struct {
	Value   string
	MaxSize int
}

func NewStringField(value string, maxSize int) *StringField {
	if len(value) > maxSize {
		value = value[:maxSize]
	}

	return &StringField{
		Value:   value,
		MaxSize: maxSize,
	}
}

func (s *StringField) Compare(op Predicate, other Field) (bool, error) {
	otherStringField, ok := other.(*StringField)
	if !ok {
		return false, nil
	}

	cmp := strings.Compare(s.Value, otherStringField.Value)

	switch op {
	case Equals:
		return cmp == 0, nil

	case LessThan:
		return cmp < 0, nil

	case GreaterThan:
		return cmp > 0, nil

	case LessThanOrEqual:
		return cmp <= 0, nil

	case GreaterThanOrEqual:
		return cmp >= 0, nil

	case NotEqual:
		return cmp != 0, nil

	case Like:
		return strings.Contains(s.Value, otherStringField.Value), nil
	default:
		return false, nil
	}
}

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

func (s *StringField) Type() Type {
	return StringType
}

func (s *StringField) String() string {
	return s.Value
}

func (s *StringField) Equals(other Field) bool {
	otherStringField, ok := other.(*StringField)
	if !ok {
		return false
	}
	return s.Value == otherStringField.Value && s.MaxSize == otherStringField.MaxSize
}

func (s *StringField) Hash() (uint32, error) {
	hash := 0
	for _, c := range s.Value {
		hash = 31*hash + int(c)
	}
	return uint32(hash), nil
}
