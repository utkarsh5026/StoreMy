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
	fieldIndex, err := findFieldIndex(filter.Field, tupleDesc)
	if err != nil {
		return nil, err
	}

	constantField, err := createConstantField(filter.Constant, tupleDesc, fieldIndex)
	if err != nil {
		return nil, err
	}

	op, err := getPredicateOperation(filter.Predicate)
	if err != nil {
		return nil, err
	}

	return query.NewPredicate(fieldIndex, op, constantField), nil
}

func findFieldIndex(fieldPath string, tupleDesc *tuple.TupleDescription) (int, error) {
	fieldName := fieldPath
	if dotIndex := strings.LastIndex(fieldPath, "."); dotIndex != -1 {
		fieldName = fieldPath[dotIndex+1:]
	}

	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}

	return -1, fmt.Errorf("field %s not found", fieldPath)
}

func createConstantField(constantValue string, tupleDesc *tuple.TupleDescription, fieldIndex int) (types.Field, error) {
	fieldType, _ := tupleDesc.TypeAtIndex(fieldIndex)

	switch fieldType {
	case types.IntType:
		var intVal int32
		fmt.Sscanf(constantValue, "%d", &intVal)
		return types.NewIntField(intVal), nil
	case types.BoolType:
		boolVal := constantValue == "true"
		return types.NewBoolField(boolVal), nil
	case types.FloatType:
		var floatVal float64
		fmt.Sscanf(constantValue, "%f", &floatVal)
		return types.NewFloat64Field(floatVal), nil
	case types.StringType:
		return types.NewStringField(constantValue, types.StringMaxSize), nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
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
