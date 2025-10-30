package types

import (
	"fmt"
	"math"
	"strings"
)

func CreateFieldFromConstant(t Type, constant string) (Field, error) {
	switch t {
	case IntType:
		var intVal int64
		fmt.Sscanf(constant, "%d", &intVal)
		return NewIntField(intVal), nil

	case BoolType:
		boolVal := constant == "true"
		return NewBoolField(boolVal), nil

	case FloatType:
		var floatVal float64
		fmt.Sscanf(constant, "%f", &floatVal)
		return NewFloat64Field(floatVal), nil

	case StringType:
		return NewStringField(constant, StringMaxSize), nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", t)
	}
}

func IsValidType(t Type) bool {
	return t == IntType || t == Int32Type || t == Int64Type ||
		t == Uint32Type || t == Uint64Type ||
		t == StringType || t == BoolType || t == FloatType
}

// GetMinValueFor returns the minimum value for a given Type as a Field.
// This is mainly used for range queries to define the lower bound for each type.
//   - For IntType: Should return the minimum int64 value (math.MinInt64), but currently uses MaxInt64 (potential bug).
//   - For FloatType: Returns the minimum float64 value (-math.MaxFloat64).
//   - For StringType: Returns an empty string as the minimum value.
//   - For BoolType: Returns false as the minimum value.
//   - For unsupported types: Returns nil.
func GetMinValueFor(t Type) Field {
	switch t {
	case IntType:
		return NewIntField(math.MinInt64) // math.MinInt64
	case FloatType:
		return NewFloat64Field(-math.MaxFloat64) // -math.MaxFloat64
	case StringType:
		return NewStringField("", StringMaxSize)
	case BoolType:
		return NewBoolField(false)
	default:
		return nil
	}
}

// GetMaxValueFor returns the maximum value for a given Type as a Field.
// This is mainly used for range queries to define the upper bound for each type.
//   - For IntType: Returns the maximum int64 value (math.MaxInt64).
//   - For FloatType: Returns the maximum float64 value (math.MaxFloat64).
//   - For StringType: Returns a string composed of the largest Unicode character repeated up to StringMaxSize.
//   - For BoolType: Returns true as the maximum value.
//   - For unsupported types: Returns nil.
func GetMaxValueFor(t Type) Field {
	switch t {
	case IntType:
		return NewIntField(math.MaxInt64) // math.MaxInt64
	case FloatType:
		return NewFloat64Field(math.MaxFloat64) // math.MaxFloat64
	case StringType:
		// For strings, use a very high Unicode character repeated
		maxStr := strings.Repeat("\U0010FFFF", StringMaxSize/4)
		return NewStringField(maxStr, StringMaxSize)
	case BoolType:
		return NewBoolField(true)
	default:
		return nil
	}
}
