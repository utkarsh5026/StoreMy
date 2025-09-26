package planner

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/parser/plan"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func buildPredicateFromFilterNode(filter *plan.FilterNode, tupleDesc *tuple.TupleDescription) (*query.Predicate, error) {
	fieldIndex := -1
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == filter.Field {
			fieldIndex = i
			break
		}
	}

	if fieldIndex == -1 {
		return nil, fmt.Errorf("field %s not found", filter.Field)
	}

	fieldType, _ := tupleDesc.TypeAtIndex(fieldIndex)
	var constantField types.Field

	switch fieldType {
	case types.IntType:
		var intVal int32
		fmt.Sscanf(filter.Constant, "%d", &intVal)
		constantField = types.NewIntField(intVal)
	case types.BoolType:
		var boolVal bool
		if filter.Constant == "true" {
			boolVal = true
		} else {
			boolVal = false
		}
		constantField = types.NewBoolField(boolVal)
	case types.FloatType:
		var floatVal float64
		fmt.Sscanf(filter.Constant, "%f", &floatVal)
		constantField = types.NewFloat64Field(floatVal)
	case types.StringType:
		constantField = types.NewStringField(filter.Constant, types.StringMaxSize)
	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}

	var op query.PredicateOp
	switch filter.Predicate {
	case types.Equals:
		op = query.Equals
	case types.LessThan:
		op = query.LessThan
	case types.GreaterThan:
		op = query.GreaterThan
	case types.LessThanOrEqual:
		op = query.LessThanOrEqual
	case types.GreaterThanOrEqual:
		op = query.GreaterThanOrEqual
	case types.NotEqual:
		op = query.NotEqual
	default:
		return nil, fmt.Errorf("unsupported predicate type")
	}

	return query.NewPredicate(fieldIndex, op, constantField), nil
}
