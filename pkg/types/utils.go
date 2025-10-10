package types

import (
	"fmt"
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
