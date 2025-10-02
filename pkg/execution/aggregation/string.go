package aggregation

import (
	"fmt"
	"storemy/pkg/types"
)

// StringCalculator implements aggregation operations specifically for string data types.
//
// Supported operations:
// - COUNT: Returns the number of non-null string values
// - MIN: Returns the lexicographically smallest string value
// - MAX: Returns the lexicographically largest string value
type StringCalculator struct {
	op           AggregateOp
	groupToCount map[string]int32
	groupToAgg   map[string]any
}

// NewStringCalculator creates a new string aggregation calculator.
func NewStringCalculator(op AggregateOp) *StringCalculator {
	return &StringCalculator{
		op:           op,
		groupToCount: make(map[string]int32),
		groupToAgg:   make(map[string]any),
	}
}

// ValidateOperation checks if the given aggregation operation is supported for strings.
func (sc *StringCalculator) ValidateOperation(op AggregateOp) error {
	switch op {
	case Count, Min, Max:
		return nil
	default:
		return fmt.Errorf("string aggregator does not support operation: %s", op.String())
	}
}

// GetResultType returns the expected result type for the aggregation operation.
//
// Type mapping:
// - COUNT operations return IntType (int32)
// - MIN/MAX operations return StringType (preserving original type)
func (sc *StringCalculator) GetResultType(op AggregateOp) types.Type {
	switch op {
	case Count:
		return types.IntType
	case Min, Max:
		return types.StringType
	default:
		return types.IntType // Default fallback
	}
}

// InitializeGroup initializes the aggregation state for a new group.
//
// Sets up initial values:
// - COUNT: starts at 0
// - MIN/MAX: starts with empty string (will be set on first value)
func (sc *StringCalculator) InitializeGroup(groupKey string) {
	sc.groupToCount[groupKey] = 0
	sc.groupToAgg[groupKey] = sc.getInitValue()
}

// UpdateAggregate processes a new field value and updates the aggregate for the given group.
//
// Processing logic by operation:
// - COUNT: Increments the count for the group
// - MIN: Updates if new value is lexicographically smaller than current minimum
// - MAX: Updates if new value is lexicographically larger than current maximum
//
// Parameters:
//   - groupKey: String representation of the grouping key
//   - fieldValue: The field value to process (must be a StringField)
//
// Returns:
//   - error: nil on success, error if field is not a string or operation fails
func (sc *StringCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	stringField, ok := fieldValue.(*types.StringField)
	if !ok {
		return fmt.Errorf("aggregate field is not a string")
	}

	aggValue := stringField.Value
	switch sc.op {
	case Count:
		currentVal := sc.groupToAgg[groupKey]
		sc.groupToAgg[groupKey] = currentVal.(int32) + 1

	case Min:
		if sc.groupToCount[groupKey] == 0 {
			sc.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sc.groupToAgg[groupKey].(string)
			if aggValue < currentVal {
				sc.groupToAgg[groupKey] = aggValue
			}
		}

	case Max:
		if sc.groupToCount[groupKey] == 0 {
			sc.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sc.groupToAgg[groupKey].(string)
			if aggValue > currentVal {
				sc.groupToAgg[groupKey] = aggValue
			}
		}
	default:
		return fmt.Errorf("unsupported string operation: %v", sc.op)
	}

	// Update count for tracking purposes
	sc.groupToCount[groupKey]++
	return nil
}

// getInitValue returns the appropriate initial value for the aggregation operation.
func (sc *StringCalculator) getInitValue() any {
	switch sc.op {
	case Count:
		return int32(0)
	case Min:
		return ""
	case Max:
		return ""
	default:
		return int32(0)
	}
}

// GetFinalValue retrieves the final aggregated value for a group and converts it to a Field.
//
// Handles type conversion:
// - string values -> StringField with proper length
// - int32 values -> IntField
func (sc *StringCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := sc.groupToAgg[groupKey]

	switch v := aggValue.(type) {
	case string:
		return types.NewStringField(v, len(v)), nil
	case int32:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

// StringAggregator is a high-level aggregator for string-based operations.
type StringAggregator struct {
	*BaseAggregator
}

// NewStringAggregator creates a new string aggregator with the specified configuration.
//
// This constructor sets up both the string-specific calculator and the base
// aggregation infrastructure needed for processing grouped aggregations.
func NewStringAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*StringAggregator, error) {
	calculator := NewStringCalculator(op)
	base, err := NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &StringAggregator{
		BaseAggregator: base,
	}, nil
}
