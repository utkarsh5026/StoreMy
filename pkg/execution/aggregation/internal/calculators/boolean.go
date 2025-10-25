package calculators

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// BooleanCalculator handles boolean-specific aggregation logic
type BooleanCalculator struct {
	groupToAgg   map[string]any   // bool for AND/OR, int64 for COUNT/SUM
	groupToCount map[string]int64 // for count tracking
	op           core.AggregateOp
}

func NewBooleanCalculator(op core.AggregateOp) *BooleanCalculator {
	return &BooleanCalculator{
		groupToAgg:   make(map[string]any),
		groupToCount: make(map[string]int64),
		op:           op,
	}
}

func (bc *BooleanCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Count, core.And, core.Or, core.Sum:
		return nil
	default:
		return fmt.Errorf("boolean aggregator does not support operation: %s", op.String())
	}
}

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

type BooleanAggregator struct {
	*core.BaseAggregator
}

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
