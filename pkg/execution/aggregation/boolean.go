package aggregation

import (
	"fmt"
	"storemy/pkg/types"
)

// BooleanCalculator handles boolean-specific aggregation logic
type BooleanCalculator struct {
	groupToAgg   map[string]any   // bool for AND/OR, int32 for COUNT/SUM
	groupToCount map[string]int32 // for count tracking
	op           AggregateOp
}

func NewBooleanCalculator(op AggregateOp) *BooleanCalculator {
	return &BooleanCalculator{
		groupToAgg:   make(map[string]any),
		groupToCount: make(map[string]int32),
		op:           op,
	}
}

func (bc *BooleanCalculator) ValidateOperation(op AggregateOp) error {
	switch op {
	case Count, And, Or, Sum:
		return nil
	default:
		return fmt.Errorf("boolean aggregator does not support operation: %s", op.String())
	}
}

func (bc *BooleanCalculator) GetResultType(op AggregateOp) types.Type {
	switch op {
	case And, Or:
		return types.BoolType
	case Count, Sum:
		return types.IntType
	default:
		return types.IntType
	}
}

func (bc *BooleanCalculator) InitializeGroup(groupKey string) {
	switch bc.op {
	case And:
		bc.groupToAgg[groupKey] = true
	case Or:
		bc.groupToAgg[groupKey] = false
	case Count, Sum:
		bc.groupToAgg[groupKey] = int32(0)
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
	case And:
		currentVal := bc.groupToAgg[groupKey].(bool)
		bc.groupToAgg[groupKey] = currentVal && value
	case Or:
		currentVal := bc.groupToAgg[groupKey].(bool)
		bc.groupToAgg[groupKey] = currentVal || value
	case Sum:
		currentVal := bc.groupToAgg[groupKey].(int32)
		if value {
			bc.groupToAgg[groupKey] = currentVal + 1
		}
	case Count:
		currentVal := bc.groupToAgg[groupKey].(int32)
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
	case int32:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

type BooleanAggregator struct {
	*BaseAggregator
}

func NewBooleanAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*BooleanAggregator, error) {
	calculator := NewBooleanCalculator(op)
	base, err := NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &BooleanAggregator{
		BaseAggregator: base,
	}, nil
}
