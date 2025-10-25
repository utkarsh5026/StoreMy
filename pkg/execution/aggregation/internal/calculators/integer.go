package calculators

import (
	"fmt"
	"math"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// IntCalculator handles integer-specific aggregation logic for various aggregate operations.
// It maintains separate maps for tracking aggregate values and counts per group key,
// supporting operations like MIN, MAX, SUM, AVG, and COUNT on integer fields.
type IntCalculator struct {
	groupToAgg   map[string]int64 // Maps group value to aggregate result
	groupToCount map[string]int64 // Maps group value to count (for AVG)
	op           core.AggregateOp // Store operation for context
}

// NewIntCalculator creates and initializes a new IntCalculator for the specified aggregate operation.
// It sets up the internal maps for tracking aggregates and counts per group.
//
// Parameters:
//   - op: The aggregate operation to perform (Min, Max, Sum, Avg, or Count)
//
// Returns:
//   - *IntCalculator: A new calculator instance ready for aggregation
func NewIntCalculator(op core.AggregateOp) *IntCalculator {
	calc := &IntCalculator{
		groupToAgg:   make(map[string]int64),
		groupToCount: make(map[string]int64),
		op:           op,
	}
	return calc
}

// ValidateOperation checks if the given aggregate operation is supported for integer values.
//
// Parameters:
//   - op: The aggregate operation to validate
//
// Returns:
//   - error: nil if operation is supported, error otherwise
func (ic *IntCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Min, core.Max, core.Sum, core.Avg, core.Count:
		return nil
	default:
		return fmt.Errorf("integer aggregator does not support operation: %s", op.String())
	}
}

// GetResultType returns the data type of the aggregation result.
// For integer calculations, this always returns IntType.
//
// Parameters:
//   - op: The aggregate operation (unused, but required by interface)
//
// Returns:
//   - types.Type: The result type (IntType)
func (ic *IntCalculator) GetResultType(op core.AggregateOp) types.Type {
	return types.IntType
}

// InitializeGroup sets up the initial aggregate value for a new group.
// The initial value depends on the aggregate operation:
//   - MIN: MaxInt32
//   - MAX: MinInt32
//   - SUM/AVG/COUNT: 0
//
// For AVG operations, also initializes the count tracker to 0.
//
// Parameters:
//   - groupKey: The key identifying the group to initialize
func (ic *IntCalculator) InitializeGroup(groupKey string) {
	switch ic.op {
	case core.Min:
		ic.groupToAgg[groupKey] = math.MaxInt32
	case core.Max:
		ic.groupToAgg[groupKey] = math.MinInt32
	case core.Sum, core.Avg, core.Count:
		ic.groupToAgg[groupKey] = 0
	}

	if ic.op == core.Avg {
		ic.groupToCount[groupKey] = 0
	}
}

// UpdateAggregate updates the aggregate value for a group with a new field value.
// The update logic varies by operation:
//   - MIN: Updates if new value is smaller
//   - MAX: Updates if new value is larger
//   - SUM: Adds the new value to current sum
//   - AVG: Adds the new value and increments count
//   - COUNT: Increments the count
//
// Parameters:
//   - groupKey: The key identifying the group to update
//   - fieldValue: The new field value to incorporate (must be *types.IntField)
//
// Returns:
//   - error: nil on success, error if fieldValue is not an IntField
func (ic *IntCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	intField, ok := fieldValue.(*types.IntField)
	if !ok {
		return fmt.Errorf("expected IntField, got %T", fieldValue)
	}

	aggValue := intField.Value
	currentAgg := ic.groupToAgg[groupKey]

	switch ic.op {
	case core.Min:
		if aggValue < currentAgg {
			ic.groupToAgg[groupKey] = aggValue
		}
	case core.Max:
		if aggValue > currentAgg {
			ic.groupToAgg[groupKey] = aggValue
		}
	case core.Sum:
		ic.groupToAgg[groupKey] = currentAgg + aggValue
	case core.Avg:
		ic.groupToAgg[groupKey] = currentAgg + aggValue
		ic.groupToCount[groupKey]++
	case core.Count:
		ic.groupToAgg[groupKey]++
	}

	return nil
}

// GetFinalValue computes and returns the final aggregate value for a group.
// For AVG operations, divides the sum by the count to get the average.
// For other operations, returns the stored aggregate value directly.
//
// Parameters:
//   - groupKey: The key identifying the group
//
// Returns:
//   - types.Field: The final aggregate value as an IntField
//   - error: Always nil for integer calculations
func (ic *IntCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := ic.groupToAgg[groupKey]

	if ic.op == core.Avg {
		count := ic.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / count
		}
	}
	return types.NewIntField(aggValue), nil
}

// IntAggregator is a specialized aggregator for integer field types.
// It wraps the BaseAggregator and provides integer-specific aggregation functionality.
type IntAggregator struct {
	*core.BaseAggregator
}

// NewIntAggregator creates a new IntAggregator instance for aggregating integer fields.
//
// Parameters:
//   - gbField: The index of the field to group by
//   - gbFieldType: The data type of the group-by field
//   - aField: The index of the field to aggregate
//   - op: The aggregate operation to perform
//
// Returns:
//   - *IntAggregator: A new aggregator instance
//   - error: Error if validation fails or initialization encounters issues
func NewIntAggregator(gbField int, gbFieldType types.Type, aField int, op core.AggregateOp) (*IntAggregator, error) {
	calculator := NewIntCalculator(op)
	base, err := core.NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &IntAggregator{
		BaseAggregator: base,
	}, nil
}
