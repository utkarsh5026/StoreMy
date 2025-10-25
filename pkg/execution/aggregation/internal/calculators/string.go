package calculators

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
)

type StringCalculator struct {
	op           core.AggregateOp
	groupToCount map[string]int64
	groupToAgg   map[string]any
}

func NewStringCalculator(op core.AggregateOp) *StringCalculator {
	return &StringCalculator{
		op:           op,
		groupToCount: make(map[string]int64),
		groupToAgg:   make(map[string]any),
	}
}

func (sc *StringCalculator) ValidateOperation(op core.AggregateOp) error {
	switch op {
	case core.Count, core.Min, core.Max:
		return nil
	default:
		return fmt.Errorf("string aggregator does not support operation: %s", op.String())
	}
}

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

func (sc *StringCalculator) InitializeGroup(groupKey string) {
	sc.groupToCount[groupKey] = 0
	sc.groupToAgg[groupKey] = sc.getInitValue()
}

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

type StringAggregator struct {
	*core.BaseAggregator
}

func NewStringAggregator(gbField int, gbFieldType types.Type, aField int, op core.AggregateOp) (*StringAggregator, error) {
	calculator := NewStringCalculator(op)
	base, err := core.NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &StringAggregator{
		BaseAggregator: base,
	}, nil
}
