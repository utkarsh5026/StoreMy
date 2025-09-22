package aggregation

import (
	"fmt"
	"math"
	"storemy/pkg/types"
)

// IntCalculator handles integer-specific aggregation logic
type IntCalculator struct {
	groupToAgg   map[string]int32 // Maps group value to aggregate result
	groupToCount map[string]int32 // Maps group value to count (for AVG)
	op           AggregateOp      // Store operation for context
}

func NewIntCalculator(op AggregateOp) *IntCalculator {
	calc := &IntCalculator{
		groupToAgg:   make(map[string]int32),
		groupToCount: make(map[string]int32),
		op:           op,
	}
	return calc
}

func (ic *IntCalculator) ValidateOperation(op AggregateOp) error {
	switch op {
	case Min, Max, Sum, Avg, Count:
		return nil
	default:
		return fmt.Errorf("integer aggregator does not support operation: %s", op.String())
	}
}

func (ic *IntCalculator) GetResultType(op AggregateOp) types.Type {
	return types.IntType
}

func (ic *IntCalculator) InitializeGroup(groupKey string) {
	switch ic.op {
	case Min:
		ic.groupToAgg[groupKey] = math.MaxInt32
	case Max:
		ic.groupToAgg[groupKey] = math.MinInt32
	case Sum, Avg, Count:
		ic.groupToAgg[groupKey] = 0
	}

	if ic.op == Avg {
		ic.groupToCount[groupKey] = 0
	}
}

func (ic *IntCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	intField, ok := fieldValue.(*types.IntField)
	if !ok {
		return fmt.Errorf("expected IntField, got %T", fieldValue)
	}

	aggValue := intField.Value
	currentAgg := ic.groupToAgg[groupKey]

	switch ic.op {
	case Min:
		if aggValue < currentAgg {
			ic.groupToAgg[groupKey] = aggValue
		}
	case Max:
		if aggValue > currentAgg {
			ic.groupToAgg[groupKey] = aggValue
		}
	case Sum:
		ic.groupToAgg[groupKey] = currentAgg + aggValue
	case Avg:
		ic.groupToAgg[groupKey] = currentAgg + aggValue
		ic.groupToCount[groupKey]++
	case Count:
		ic.groupToAgg[groupKey]++
	}

	return nil
}

func (ic *IntCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := ic.groupToAgg[groupKey]

	if ic.op == Avg {
		count := ic.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / count
		}
	}
	return types.NewIntField(aggValue), nil
}

type IntAggregator struct {
	*BaseAggregator
}

func NewIntAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*IntAggregator, error) {
	calculator := NewIntCalculator(op)
	base, err := NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &IntAggregator{
		BaseAggregator: base,
	}, nil
}
