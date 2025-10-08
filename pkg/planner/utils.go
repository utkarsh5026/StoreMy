package planner

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/parser/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// buildPredicateFromFilterNode constructs a query predicate from a filter node in the execution plan.
// It takes a filter node containing field name, predicate type, and constant value, along with
// tuple description for type information, and returns a fully constructed predicate for query execution.
func buildPredicateFromFilterNode(filter *plan.FilterNode, tupleDesc *tuple.TupleDescription) (*query.Predicate, error) {
	fieldIndex, err := locateField(filter.Field, tupleDesc)
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

// locateField locates the index of a field within the tuple description by name.
// It supports both simple field names and dotted field paths (e.g., "table.field").
// For dotted paths, it extracts the field name after the last dot.
func locateField(fieldPath string, tupleDesc *tuple.TupleDescription) (int, error) {
	fieldName := fieldPath
	if dotIndex := strings.LastIndex(fieldPath, "."); dotIndex != -1 {
		fieldName = fieldPath[dotIndex+1:]
	}
	return findFieldIndex(fieldName, tupleDesc)
}

// createConstantField converts a string constant value to the appropriate typed field
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

// getPredicateOperation maps a predicate type to its corresponding query operation.
func getPredicateOperation(predicate primitives.Predicate) (query.PredicateOp, error) {
	predicateMap := map[primitives.Predicate]query.PredicateOp{
		primitives.Equals:             query.Equals,
		primitives.LessThan:           query.LessThan,
		primitives.GreaterThan:        query.GreaterThan,
		primitives.LessThanOrEqual:    query.LessThanOrEqual,
		primitives.GreaterThanOrEqual: query.GreaterThanOrEqual,
		primitives.NotEqual:           query.NotEqual,
	}

	op, exists := predicateMap[predicate]
	if !exists {
		return 0, fmt.Errorf("unsupported predicate type: %v", predicate)
	}

	return op, nil
}
