package calculators

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// BooleanCalculator handles boolean-specific aggregation logic for various aggregate operations.
// It maintains maps for tracking aggregate values and counts per group key,
// supporting operations like COUNT, AND, OR, and SUM on boolean fields.
// The groupToAgg map stores either bool values (for AND/OR) or int64 values (for COUNT/SUM).
type BooleanCalculator struct {
	groupToAgg   map[string]any   // bool for AND/OR, int64 for COUNT/SUM
	groupToCount map[string]int64 // for count tracking
	op           core.AggregateOp // Store operation for context
}

// NewBooleanCalculator creates and initializes a new BooleanCalculator for the specified aggregate operation.
// It sets up the internal maps for tracking aggregates and counts per group.
//
// Parameters:
//   - op: The aggregate operation to perform (Count, And, Or, or Sum)
//
// Returns:
//   - *BooleanCalculator: A new calculator instance ready for aggregation
func NewBooleanCalculator(op core.AggregateOp) *BooleanCalculator {
	return &BooleanCalculator{
		groupToAgg:   make(map[string]any),
		groupToCount: make(map[string]int64),
		op:           op,
	}
}

// ValidateOperation checks if the given aggregate operation is supported for boolean values.
//
// Parameters:
//   - op: The aggregate operation to validate
//
// Returns:
//   - error: nil if operation is supported (Count, And, Or, Sum), error otherwise
func (bc *BooleanCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Count, core.And, core.Or, core.Sum:
		return nil
	default:
		return fmt.Errorf("boolean aggregator does not support operation: %s", op.String())
	}
}

// GetResultType returns the data type of the aggregation result.
// For AND/OR operations, returns BoolType.
// For COUNT/SUM operations, returns IntType.
//
// Parameters:
//   - op: The aggregate operation
//
// Returns:
//   - types.Type: The result type (BoolType or IntType)
func (bc *BooleanCalculator) GetResultType(op core.AggregateOp) types.Type {
	switch op {
	case core.And, core.Or:
		return types.BoolType
	case core.Count, core.Sum:
		return types.IntType
	default:
		return types.IntType
	}
}

// InitializeGroup sets up the initial aggregate value for a new group.
// The initial value depends on the aggregate operation:
//   - AND: true (identity element for logical AND)
//   - OR: false (identity element for logical OR)
//   - COUNT/SUM: 0
//
// Also initializes the count tracker to 0 for all operations.
//
// Parameters:
//   - groupKey: The key identifying the group to initialize
func (bc *BooleanCalculator) InitializeGroup(groupKey string) {
	switch bc.op {
	case core.And:
		bc.groupToAgg[groupKey] = true
	case core.Or:
		bc.groupToAgg[groupKey] = false
	case core.Count, core.Sum:
		bc.groupToAgg[groupKey] = int64(0)
	}
	bc.groupToCount[groupKey] = 0
}

// UpdateAggregate updates the aggregate value for a group with a new field value.
// The update logic varies by operation:
//   - AND: Performs logical AND with current value
//   - OR: Performs logical OR with current value
//   - SUM: Increments count if value is true (treats true as 1, false as 0)
//   - COUNT: Increments the count
//
// Parameters:
//   - groupKey: The key identifying the group to update
//   - fieldValue: The new field value to incorporate (must be *types.BoolField)
//
// Returns:
//   - error: nil on success, error if fieldValue is not a BoolField
func (bc *BooleanCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	boolField, ok := fieldValue.(*types.BoolField)
	if !ok {
		return fmt.Errorf("expected BoolField, got %T", fieldValue)
	}

	value := boolField.Value

	switch bc.op {
	case core.And:
		currentVal := bc.groupToAgg[groupKey].(bool)
		bc.groupToAgg[groupKey] = currentVal && value
	case core.Or:
		currentVal := bc.groupToAgg[groupKey].(bool)
		bc.groupToAgg[groupKey] = currentVal || value
	case core.Sum:
		currentVal := bc.groupToAgg[groupKey].(int64)
		if value {
			bc.groupToAgg[groupKey] = currentVal + 1
		}
	case core.Count:
		currentVal := bc.groupToAgg[groupKey].(int64)
		bc.groupToAgg[groupKey] = currentVal + 1
	}

	bc.groupToCount[groupKey]++
	return nil
}

// GetFinalValue computes and returns the final aggregate value for a group.
// Returns the stored aggregate value, which may be either a bool (for AND/OR)
// or an int64 (for COUNT/SUM), wrapped in the appropriate Field type.
//
// Parameters:
//   - groupKey: The key identifying the group
//
// Returns:
//   - types.Field: The final aggregate value as a BoolField or IntField
//   - error: Error if the aggregate value type is unexpected
func (bc *BooleanCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := bc.groupToAgg[groupKey]

	switch v := aggValue.(type) {
	case bool:
		return types.NewBoolField(v), nil
	case int64:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

// BooleanAggregator is a specialized aggregator for boolean field types.
// It wraps the BaseAggregator and provides boolean-specific aggregation functionality.
type BooleanAggregator struct {
	*core.BaseAggregator
}

// NewBooleanAggregator creates a new BooleanAggregator instance for aggregating boolean fields.
//
// Parameters:
//   - gbField: The index of the field to group by
//   - gbFieldType: The data type of the group-by field
//   - aField: The index of the field to aggregate
//   - op: The aggregate operation to perform
//
// Returns:
//   - *BooleanAggregator: A new aggregator instance
//   - error: Error if validation fails or initialization encounters issues
func NewBooleanAggregator(gbField int, gbFieldType types.Type, aField int, op core.AggregateOp) (*BooleanAggregator, error) {
	calculator := NewBooleanCalculator(op)
	base, err := core.NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &BooleanAggregator{
		BaseAggregator: base,
	}, nil
}
