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
		if _, err := fmt.Sscanf(constant, "%d", &intVal); err != nil {
			return nil, fmt.Errorf("invalid integer constant %q: %w", constant, err)
		}
		return NewIntField(intVal), nil

	case BoolType:
		boolVal := constant == "true"
		return NewBoolField(boolVal), nil

	case FloatType:
		var floatVal float64
		if _, err := fmt.Sscanf(constant, "%f", &floatVal); err != nil {
			return nil, fmt.Errorf("invalid float constant %q: %w", constant, err)
		}
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
