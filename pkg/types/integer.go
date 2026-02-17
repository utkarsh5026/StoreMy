package types

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"storemy/pkg/primitives"
	"strconv"
)

// Int32Field represents a 32-bit signed integer field
type Int32Field struct {
	Value int32
}

func NewInt32Field(value int32) *Int32Field {
	return &Int32Field{Value: value}
}

func (f *Int32Field) Serialize(w io.Writer) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(f.Value)) // #nosec G115
	_, err := w.Write(bytes)
	return err
}

func (f *Int32Field) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherField, ok := other.(*Int32Field)
	if !ok {
		return false, nil
	}
	return compareInt32(f.Value, otherField.Value, op), nil
}

func (f *Int32Field) Type() Type {
	return Int32Type
}

func (f *Int32Field) String() string {
	return strconv.FormatInt(int64(f.Value), 10)
}

func (f *Int32Field) Equals(other Field) bool {
	otherField, ok := other.(*Int32Field)
	if !ok {
		return false
	}
	return f.Value == otherField.Value
}

func (f *Int32Field) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(f.Value)) // #nosec G115
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *Int32Field) Length() uint32 {
	return 4
}

// Int64Field represents a 64-bit signed integer field
type Int64Field struct {
	Value int64
}

func NewInt64Field(value int64) *Int64Field {
	return &Int64Field{Value: value}
}

func (f *Int64Field) Serialize(w io.Writer) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(f.Value)) // #nosec G115
	_, err := w.Write(bytes)
	return err
}

func (f *Int64Field) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherField, ok := other.(*Int64Field)
	if !ok {
		return false, nil
	}
	return compareInt64(f.Value, otherField.Value, op), nil
}

func (f *Int64Field) Type() Type {
	return Int64Type
}

func (f *Int64Field) String() string {
	return strconv.FormatInt(f.Value, 10)
}

func (f *Int64Field) Equals(other Field) bool {
	otherField, ok := other.(*Int64Field)
	if !ok {
		return false
	}
	return f.Value == otherField.Value
}

func (f *Int64Field) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(f.Value)) // #nosec G115
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *Int64Field) Length() uint32 {
	return 8
}

// Uint32Field represents a 32-bit unsigned integer field
type Uint32Field struct {
	Value uint32
}

func NewUint32Field(value uint32) *Uint32Field {
	return &Uint32Field{Value: value}
}

func (f *Uint32Field) Serialize(w io.Writer) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, f.Value)
	_, err := w.Write(bytes)
	return err
}

func (f *Uint32Field) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherField, ok := other.(*Uint32Field)
	if !ok {
		return false, nil
	}
	return compareUint32(f.Value, otherField.Value, op), nil
}

func (f *Uint32Field) Type() Type {
	return Uint32Type
}

func (f *Uint32Field) String() string {
	return strconv.FormatUint(uint64(f.Value), 10)
}

func (f *Uint32Field) Equals(other Field) bool {
	otherField, ok := other.(*Uint32Field)
	if !ok {
		return false
	}
	return f.Value == otherField.Value
}

func (f *Uint32Field) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, f.Value)
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *Uint32Field) Length() uint32 {
	return 4
}

// Uint64Field represents a 64-bit unsigned integer field
type Uint64Field struct {
	Value uint64
}

func NewUint64Field(value uint64) *Uint64Field {
	return &Uint64Field{Value: value}
}

func (f *Uint64Field) Serialize(w io.Writer) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, f.Value)
	_, err := w.Write(bytes)
	return err
}

func (f *Uint64Field) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherField, ok := other.(*Uint64Field)
	if !ok {
		return false, nil
	}
	return compareUint64(f.Value, otherField.Value, op), nil
}

func (f *Uint64Field) Type() Type {
	return Uint64Type
}

func (f *Uint64Field) String() string {
	return strconv.FormatUint(f.Value, 10)
}

func (f *Uint64Field) Equals(other Field) bool {
	otherField, ok := other.(*Uint64Field)
	if !ok {
		return false
	}
	return f.Value == otherField.Value
}

func (f *Uint64Field) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, f.Value)
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *Uint64Field) Length() uint32 {
	return 8
}

// IntField is kept for backward compatibility (alias for Int64Field)
type IntField struct {
	Value int64
}

func NewIntField(value int64) *IntField {
	return &IntField{Value: value}
}

func (f *IntField) Serialize(w io.Writer) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(f.Value)) // #nosec G115
	_, err := w.Write(bytes)
	return err
}

func (f *IntField) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherIntField, ok := other.(*IntField)
	if !ok {
		return false, nil
	}
	return compareInt64(f.Value, otherIntField.Value, op), nil
}

func (f *IntField) Type() Type {
	return IntType
}

func (f *IntField) String() string {
	return strconv.FormatInt(f.Value, 10)
}

func (f *IntField) Equals(other Field) bool {
	otherInt, ok := other.(*IntField)
	if !ok {
		return false
	}
	return f.Value == otherInt.Value
}

func (f *IntField) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	value := f.Value
	bytes := make([]byte, 8)
	for i := range 8 {
		bytes[i] = byte(value >> (8 * i)) // #nosec G115
	}
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *IntField) Length() uint32 {
	return 8
}

// Helper comparison functions to keep code DRY
func compareInt32(a, b int32, op primitives.Predicate) bool {
	switch op {
	case primitives.Equals:
		return a == b
	case primitives.LessThan:
		return a < b
	case primitives.GreaterThan:
		return a > b
	case primitives.LessThanOrEqual:
		return a <= b
	case primitives.GreaterThanOrEqual:
		return a >= b
	case primitives.NotEqual:
		return a != b
	default:
		return false
	}
}

func compareInt64(a, b int64, op primitives.Predicate) bool {
	switch op {
	case primitives.Equals:
		return a == b
	case primitives.LessThan:
		return a < b
	case primitives.GreaterThan:
		return a > b
	case primitives.LessThanOrEqual:
		return a <= b
	case primitives.GreaterThanOrEqual:
		return a >= b
	case primitives.NotEqual:
		return a != b
	default:
		return false
	}
}

func compareUint32(a, b uint32, op primitives.Predicate) bool {
	switch op {
	case primitives.Equals:
		return a == b
	case primitives.LessThan:
		return a < b
	case primitives.GreaterThan:
		return a > b
	case primitives.LessThanOrEqual:
		return a <= b
	case primitives.GreaterThanOrEqual:
		return a >= b
	case primitives.NotEqual:
		return a != b
	default:
		return false
	}
}

func compareUint64(a, b uint64, op primitives.Predicate) bool {
	switch op {
	case primitives.Equals:
		return a == b
	case primitives.LessThan:
		return a < b
	case primitives.GreaterThan:
		return a > b
	case primitives.LessThanOrEqual:
		return a <= b
	case primitives.GreaterThanOrEqual:
		return a >= b
	case primitives.NotEqual:
		return a != b
	default:
		return false
	}
}
