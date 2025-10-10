package aggregation

import (
	"fmt"
	"math"
	"storemy/pkg/types"
)

// FloatCalculator handles float-specific aggregation logic
type FloatCalculator struct {
	groupToAgg   map[string]float64
	groupToCount map[string]int64
	op           AggregateOp
}

func NewFloatCalculator(op AggregateOp) *FloatCalculator {
	return &FloatCalculator{
		groupToAgg:   make(map[string]float64),
		groupToCount: make(map[string]int64),
		op:           op,
	}
}

func (fc *FloatCalculator) ValidateOperation(op AggregateOp) error {
	switch op {
	case Min, Max, Sum, Avg, Count:
		return nil
	default:
		return fmt.Errorf("float aggregator does not support operation: %s", op.String())
	}
}

func (fc *FloatCalculator) GetResultType(op AggregateOp) types.Type {
	if op == Count {
		return types.IntType
	}
	return types.FloatType
}

func (fc *FloatCalculator) InitializeGroup(groupKey string) {
	switch fc.op {
	case Min:
		fc.groupToAgg[groupKey] = math.Inf(1)
	case Max:
		fc.groupToAgg[groupKey] = math.Inf(-1)
	case Sum, Avg, Count:
		fc.groupToAgg[groupKey] = 0.0
	}

	if fc.op == Avg {
		fc.groupToCount[groupKey] = 0
	}
}

func (fc *FloatCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	floatField, ok := fieldValue.(*types.Float64Field)
	if !ok {
		return fmt.Errorf("expected FloatField, got %T", fieldValue)
	}

	value := floatField.Value
	currentAgg := fc.groupToAgg[groupKey]

	switch fc.op {
	case Min:
		if value < currentAgg {
			fc.groupToAgg[groupKey] = value
		}
	case Max:
		if value > currentAgg {
			fc.groupToAgg[groupKey] = value
		}
	case Sum:
		fc.groupToAgg[groupKey] = currentAgg + value
	case Avg:
		fc.groupToAgg[groupKey] = currentAgg + value
		fc.groupToCount[groupKey]++
	case Count:
		fc.groupToAgg[groupKey] = currentAgg + 1.0
	}

	return nil
}

func (fc *FloatCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := fc.groupToAgg[groupKey]
	if fc.op == Avg {
		count := fc.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / float64(count)
		}
	}

	if fc.op == Count {
		return types.NewIntField(int64(aggValue)), nil
	}
	return types.NewFloat64Field(aggValue), nil
}

type FloatAggregator struct {
	*BaseAggregator
}

func NewFloatAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*FloatAggregator, error) {
	calculator := NewFloatCalculator(op)
	base, err := NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &FloatAggregator{
		BaseAggregator: base,
	}, nil
}
