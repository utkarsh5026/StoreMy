package calculators

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// StringCalculator handles string-specific aggregation logic for various aggregate operations.
// It maintains maps for tracking aggregate values and counts per group key,
// supporting operations like COUNT, MIN, and MAX on string fields.
// The groupToAgg map stores either string values (for MIN/MAX) or int64 values (for COUNT).
type StringCalculator struct {
	op           core.AggregateOp
	groupToCount map[string]int64
	groupToAgg   map[string]any
}

// NewStringCalculator creates and initializes a new StringCalculator for the specified aggregate operation.
// It sets up the internal maps for tracking aggregates and counts per group.
//
// Parameters:
//   - op: The aggregate operation to perform (Count, Min, or Max)
//
// Returns:
//   - *StringCalculator: A new calculator instance ready for aggregation
func NewStringCalculator(op core.AggregateOp) *StringCalculator {
	return &StringCalculator{
		op:           op,
		groupToCount: make(map[string]int64),
		groupToAgg:   make(map[string]any),
	}
}

// ValidateOperation checks if the given aggregate operation is supported for string values.
//
// Parameters:
//   - op: The aggregate operation to validate
//
// Returns:
//   - error: nil if operation is supported (Count, Min, Max), error otherwise
func (sc *StringCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Count, core.Min, core.Max:
		return nil
	default:
		return fmt.Errorf("string aggregator does not support operation: %s", op.String())
	}
}

// GetResultType returns the data type of the aggregation result.
// For COUNT operations, returns IntType.
// For MIN/MAX operations, returns StringType.
//
// Parameters:
//   - op: The aggregate operation
//
// Returns:
//   - types.Type: The result type (StringType or IntType)
func (sc *StringCalculator) GetResultType(op core.AggregateOp) types.Type {
	switch op {
	case core.Count:
		return types.IntType
	case core.Min, core.Max:
		return types.StringType
	default:
		return types.IntType // Default fallback
	}
}

// InitializeGroup sets up the initial aggregate value for a new group.
// The initial value depends on the aggregate operation:
//   - COUNT: 0
//   - MIN/MAX: empty string
//
// Also initializes the count tracker to 0.
//
// Parameters:
//   - groupKey: The key identifying the group to initialize
func (sc *StringCalculator) InitializeGroup(groupKey string) {
	sc.groupToCount[groupKey] = 0
	sc.groupToAgg[groupKey] = sc.getInitValue()
}

// UpdateAggregate updates the aggregate value for a group with a new field value.
// The update logic varies by operation:
//   - COUNT: Increments the count
//   - MIN: Updates if new value is lexicographically smaller (or sets first value)
//   - MAX: Updates if new value is lexicographically larger (or sets first value)
//
// Parameters:
//   - groupKey: The key identifying the group to update
//   - fieldValue: The new field value to incorporate (must be *types.StringField)
//
// Returns:
//   - error: nil on success, error if fieldValue is not a StringField or operation is unsupported
func (sc *StringCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	stringField, ok := fieldValue.(*types.StringField)
	if !ok {
		return fmt.Errorf("aggregate field is not a string")
	}

	aggValue := stringField.Value
	switch sc.op {
	case core.Count:
		currentVal := sc.groupToAgg[groupKey]
		sc.groupToAgg[groupKey] = currentVal.(int64) + 1

	case core.Min:
		if sc.groupToCount[groupKey] == 0 {
			sc.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sc.groupToAgg[groupKey].(string)
			if aggValue < currentVal {
				sc.groupToAgg[groupKey] = aggValue
			}
		}

	case core.Max:
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

	sc.groupToCount[groupKey]++
	return nil
}

// getInitValue returns the initial aggregate value based on the operation.
//
// Returns:
//   - any: Initial value (int64(0) for COUNT, empty string for MIN/MAX)
func (sc *StringCalculator) getInitValue() any {
	switch sc.op {
	case core.Count:
		return int64(0)
	case core.Min:
		return ""
	case core.Max:
		return ""
	default:
		return int64(0)
	}
}

// GetFinalValue computes and returns the final aggregate value for a group.
// Returns the stored aggregate value, which may be either a string (for MIN/MAX)
// or an int64 (for COUNT), wrapped in the appropriate Field type.
//
// Parameters:
//   - groupKey: The key identifying the group
//
// Returns:
//   - types.Field: The final aggregate value as a StringField or IntField
//   - error: Error if the aggregate value type is unexpected
func (sc *StringCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := sc.groupToAgg[groupKey]

	switch v := aggValue.(type) {
	case string:
		return types.NewStringField(v, len(v)), nil
	case int64:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}
