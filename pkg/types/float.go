package types

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"storemy/pkg/primitives"
	"strconv"
)

const (
	epsilon = 1e-9
)

type Float64Field struct {
	Value float64
}

func NewFloat64Field(value float64) *Float64Field {
	return &Float64Field{Value: value}
}

func (f *Float64Field) Serialize(w io.Writer) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, math.Float64bits(f.Value))
	_, err := w.Write(bytes)
	return err
}

func (f *Float64Field) Compare(op primitives.Predicate, other Field) (bool, error) {
	otherFloat64Field, ok := other.(*Float64Field)
	if !ok {
		// Handle cross-type comparison with IntField
		if intField, ok := other.(*IntField); ok {
			otherValue := float64(intField.Value)
			return f.compareFloat64Values(op, otherValue)
		}
		return false, fmt.Errorf("cannot compare Float64Field with %T", other)
	}

	return f.compareFloat64Values(op, otherFloat64Field.Value)
}

func (f *Float64Field) compareFloat64Values(op primitives.Predicate, other float64) (bool, error) {
	switch op {
	case primitives.Equals:
		return math.Abs(f.Value-other) < epsilon, nil
	case primitives.LessThan:
		return f.Value < other, nil
	case primitives.GreaterThan:
		return f.Value > other, nil
	case primitives.LessThanOrEqual:
		return f.Value <= other, nil
	case primitives.GreaterThanOrEqual:
		return f.Value >= other, nil
	case primitives.NotEqual:
		return math.Abs(f.Value-other) >= epsilon, nil
	default:
		return false, fmt.Errorf("unsupported predicate for Float64Field: %v", op)
	}
}

func (f *Float64Field) Type() Type {
	return FloatType
}

// String returns string representation of the float64
func (f *Float64Field) String() string {
	return strconv.FormatFloat(f.Value, 'f', -1, 64)
}

func (f *Float64Field) Equals(other Field) bool {
	otherFloat64, ok := other.(*Float64Field)
	if !ok {
		return false
	}
	return math.Abs(f.Value-otherFloat64.Value) < epsilon
}

func (f *Float64Field) Hash() (primitives.HashCode, error) {
	h := fnv.New32a()
	bytes := make([]byte, 8)
	bits := uint64(f.Value)
	for i := range 8 {
		bytes[i] = byte(bits >> (8 * i)) // #nosec G115
	}
	_, _ = h.Write(bytes)
	return primitives.HashCode(h.Sum32()), nil
}

func (f *Float64Field) Length() uint32 {
	return 8
}
