package calculators

import (
	"fmt"
	"math"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// FloatCalculator handles float-specific aggregation logic
type FloatCalculator struct {
	groupToAgg   map[string]float64
	groupToCount map[string]int64
	op           core.AggregateOp
}

func NewFloatCalculator(op core.AggregateOp) *FloatCalculator {
	return &FloatCalculator{
		groupToAgg:   make(map[string]float64),
		groupToCount: make(map[string]int64),
		op:           op,
	}
}

func (fc *FloatCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Min, core.Max, core.Sum, core.Avg, core.Count:
		return nil
	default:
		return fmt.Errorf("float aggregator does not support operation: %s", op.String())
	}
}

func (fc *FloatCalculator) GetResultType(op core.AggregateOp) types.Type {
	if op == core.Count {
		return types.IntType
	}
	return types.FloatType
}

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

type FloatAggregator struct {
	*core.BaseAggregator
}

func NewFloatAggregator(gbField int, gbFieldType types.Type, aField int, op core.AggregateOp) (*FloatAggregator, error) {
	calculator := NewFloatCalculator(op)
	base, err := core.NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &FloatAggregator{
		BaseAggregator: base,
	}, nil
}
