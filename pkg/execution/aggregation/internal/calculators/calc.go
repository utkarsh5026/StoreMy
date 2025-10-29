package calculators

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// GetCalculator returns an AggregateCalculator appropriate for the given field type and aggregate operation.
//
// Parameters:
//   - fieldType: The type of the field to be aggregated (e.g., IntType, BoolType, StringType, FloatType).
//   - op: The aggregate operation to perform (e.g., Min, Max, Count, Avg, etc.).
//
// Returns:
//   - core.AggregateCalculator: An implementation of the AggregateCalculator interface suitable for the field type and operation.
//   - error: An error if the field type is unsupported.
func GetCalculator(fieldType types.Type, op core.AggregateOp) (core.AggregateCalculator, error) {
	switch fieldType {
	case types.IntType:
		return NewIntCalculator(op), nil
	case types.BoolType:
		return NewBooleanCalculator(op), nil
	case types.StringType:
		return NewStringCalculator(op), nil
	case types.FloatType:
		return NewFloatCalculator(op), nil
	default:
		return nil, fmt.Errorf("unsupported field type for aggregation: %v", fieldType)
	}
}
