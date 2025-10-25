package calculators

import (
	"fmt"
	"math"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

// IntCalculator handles integer-specific aggregation logic
type IntCalculator struct {
	groupToAgg   map[string]int64 // Maps group value to aggregate result
	groupToCount map[string]int64 // Maps group value to count (for AVG)
	op           core.AggregateOp      // Store operation for context
}

func NewIntCalculator(op core.AggregateOp) *IntCalculator {
	calc := &IntCalculator{
		groupToAgg:   make(map[string]int64),
		groupToCount: make(map[string]int64),
		op:           op,
	}
	return calc
}

func (ic *IntCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Min, core.Max, core.Sum, core.Avg, core.Count:
		return nil
	default:
		return fmt.Errorf("integer aggregator does not support operation: %s", op.String())
	}
}

func (ic *IntCalculator) GetResultType(op core.AggregateOp) types.Type {
	return types.IntType
}

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

type IntAggregator struct {
	*core.BaseAggregator
}

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
