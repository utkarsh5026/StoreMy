package calculators

import (
	"fmt"
	"math"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// FloatCalculator handles float-specific aggregation logic for various aggregate operations.
// It maintains separate maps for tracking aggregate values and counts per group key,
// supporting operations like MIN, MAX, SUM, AVG, and COUNT on float64 fields.
type FloatCalculator struct {
	groupToAgg   map[string]float64 // Maps group value to aggregate result
	groupToCount map[string]int64   // Maps group value to count (for AVG)
	op           core.AggregateOp   // Store operation for context
}

// NewFloatCalculator creates and initializes a new FloatCalculator for the specified aggregate operation.
// It sets up the internal maps for tracking aggregates and counts per group.
//
// Parameters:
//   - op: The aggregate operation to perform (Min, Max, Sum, Avg, or Count)
//
// Returns:
//   - *FloatCalculator: A new calculator instance ready for aggregation
func NewFloatCalculator(op core.AggregateOp) *FloatCalculator {
	return &FloatCalculator{
		groupToAgg:   make(map[string]float64),
		groupToCount: make(map[string]int64),
		op:           op,
	}
}

// ValidateOperation checks if the given aggregate operation is supported for float values.
//
// Parameters:
//   - op: The aggregate operation to validate
//
// Returns:
//   - error: nil if operation is supported, error otherwise
func (fc *FloatCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Min, core.Max, core.Sum, core.Avg, core.Count:
		return nil
	default:
		return fmt.Errorf("float aggregator does not support operation: %s", op.String())
	}
}

// GetResultType returns the data type of the aggregation result.
// For COUNT operations, returns IntType.
// For all other operations, returns FloatType.
//
// Parameters:
//   - op: The aggregate operation
//
// Returns:
//   - types.Type: The result type (FloatType or IntType)
func (fc *FloatCalculator) GetResultType(op core.AggregateOp) types.Type {
	if op == core.Count {
		return types.IntType
	}
	return types.FloatType
}

// InitializeGroup sets up the initial aggregate value for a new group.
// The initial value depends on the aggregate operation:
//   - MIN: +Inf (positive infinity)
//   - MAX: -Inf (negative infinity)
//   - SUM/AVG/COUNT: 0.0
//
// For AVG operations, also initializes the count tracker to 0.
//
// Parameters:
//   - groupKey: The key identifying the group to initialize
func (fc *FloatCalculator) InitializeGroup(groupKey string) {
	switch fc.op {
	case core.Min:
		fc.groupToAgg[groupKey] = math.Inf(1)
	case core.Max:
		fc.groupToAgg[groupKey] = math.Inf(-1)
	case core.Sum, core.Avg, core.Count:
		fc.groupToAgg[groupKey] = 0.0
	}

	if fc.op == core.Avg {
		fc.groupToCount[groupKey] = 0
	}
}

// UpdateAggregate updates the aggregate value for a group with a new field value.
// The update logic varies by operation:
//   - MIN: Updates if new value is smaller
//   - MAX: Updates if new value is larger
//   - SUM: Adds the new value to current sum
//   - AVG: Adds the new value and increments count
//   - COUNT: Increments the count by 1.0
//
// Parameters:
//   - groupKey: The key identifying the group to update
//   - fieldValue: The new field value to incorporate (must be *types.Float64Field)
//
// Returns:
//   - error: nil on success, error if fieldValue is not a Float64Field
func (fc *FloatCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	floatField, ok := fieldValue.(*types.Float64Field)
	if !ok {
		return fmt.Errorf("expected FloatField, got %T", fieldValue)
	}

	value := floatField.Value
	currentAgg := fc.groupToAgg[groupKey]

	switch fc.op {
	case core.Min:
		if value < currentAgg {
			fc.groupToAgg[groupKey] = value
		}
	case core.Max:
		if value > currentAgg {
			fc.groupToAgg[groupKey] = value
		}
	case core.Sum:
		fc.groupToAgg[groupKey] = currentAgg + value
	case core.Avg:
		fc.groupToAgg[groupKey] = currentAgg + value
		fc.groupToCount[groupKey]++
	case core.Count:
		fc.groupToAgg[groupKey] = currentAgg + 1.0
	}

	return nil
}

// GetFinalValue computes and returns the final aggregate value for a group.
// For AVG operations, divides the sum by the count to get the average.
// For COUNT operations, converts the float count to an IntField.
// For other operations, returns the stored aggregate value as a Float64Field.
//
// Parameters:
//   - groupKey: The key identifying the group
//
// Returns:
//   - types.Field: The final aggregate value as a Float64Field or IntField
//   - error: Always nil for float calculations
func (fc *FloatCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := fc.groupToAgg[groupKey]
	if fc.op == core.Avg {
		count := fc.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / float64(count)
		}
	}

	if fc.op == core.Count {
		return types.NewIntField(int64(aggValue)), nil
	}
	return types.NewFloat64Field(aggValue), nil
}

// FloatAggregator is a specialized aggregator for float64 field types.
// It wraps the BaseAggregator and provides float-specific aggregation functionality.
type FloatAggregator struct {
	*core.BaseAggregator
}

// NewFloatAggregator creates a new FloatAggregator instance for aggregating float64 fields.
//
// Returns:
//   - *FloatAggregator: A new aggregator instance
//   - error: Error if validation fails or initialization encounters issues
func NewFloatAggregator(config *core.AggregatorConfig) (*FloatAggregator, error) {
	calculator := NewFloatCalculator(config.Operation)
	base, err := core.NewBaseAggregator(config, calculator)
	if err != nil {
		return nil, err
	}

	return &FloatAggregator{
		BaseAggregator: base,
	}, nil
}
