package planner

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/parser/plan"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

func buildPredicateFromFilterNode(filter *plan.FilterNode, tupleDesc *tuple.TupleDescription) (*query.Predicate, error) {
	fieldName := filter.Field
	if dotIndex := strings.LastIndex(filter.Field, "."); dotIndex != -1 {
		fieldName = filter.Field[dotIndex+1:]
	}

	fieldIndex := -1
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
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

	op, err := getPredicateOperation(filter.Predicate)
	if err != nil {
		return nil, err
	}

	return query.NewPredicate(fieldIndex, op, constantField), nil
}

func getPredicateOperation(predicate types.Predicate) (query.PredicateOp, error) {
	predicateMap := map[types.Predicate]query.PredicateOp{
		types.Equals:             query.Equals,
		types.LessThan:           query.LessThan,
		types.GreaterThan:        query.GreaterThan,
		types.LessThanOrEqual:    query.LessThanOrEqual,
		types.GreaterThanOrEqual: query.GreaterThanOrEqual,
		types.NotEqual:           query.NotEqual,
	}

	op, exists := predicateMap[predicate]
	if !exists {
		return 0, fmt.Errorf("unsupported predicate type: %v", predicate)
	}

	return op, nil
}
