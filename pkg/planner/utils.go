package planner

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/parser/plan"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// buildPredicateFromFilterNode constructs a query predicate from a filter node in the execution plan.
// It takes a filter node containing field name, predicate type, and constant value, along with
// tuple description for type information, and returns a fully constructed predicate for query execution.
//
// Parameters:
//   - filter: FilterNode containing the filter criteria (field, predicate type, constant value)
//   - tupleDesc: TupleDescription providing schema information for field lookup and type validation
//
// Returns:
//   - *query.Predicate: A predicate object ready for query execution
//   - error: Any error encountered during predicate construction
//
// Example:
//
//	filter := &plan.FilterNode{Field: "age", Predicate: types.GreaterThan, Constant: "18"}
//	predicate, err := buildPredicateFromFilterNode(filter, tupleDesc)
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

// findFieldIndex locates the index of a field within the tuple description by name.
// It supports both simple field names and dotted field paths (e.g., "table.field").
// For dotted paths, it extracts the field name after the last dot.
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

// createConstantField converts a string constant value to the appropriate typed field
// based on the field type at the specified index in the tuple description.
// Supports conversion to int, bool, float, and string field types.
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
// Uses a lookup map for efficient and maintainable predicate type conversion.
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
