package aggregation

import (
	"fmt"
	"storemy/pkg/types"
)

type StringCalculator struct {
	op           AggregateOp
	groupToCount map[string]int32
	groupToAgg   map[string]any
}

func NewStringCalculator(op AggregateOp) *StringCalculator {
	return &StringCalculator{
		op:           op,
		groupToCount: make(map[string]int32),
		groupToAgg:   make(map[string]any),
	}
}

func (sc *StringCalculator) ValidateOperation(op AggregateOp) error {
	switch op {
	case Count, Min, Max:
		return nil
	default:
		return fmt.Errorf("string aggregator does not support operation: %s", op.String())
	}
}

func (sc *StringCalculator) GetResultType(op AggregateOp) types.Type {
	switch op {
	case Count:
		return types.IntType
	case Min, Max:
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
	case Count:
		currentVal := sc.groupToAgg[groupKey]
		sc.groupToAgg[groupKey] = currentVal.(int32) + 1

	case Min:
		if sc.groupToCount[groupKey] == 0 {
			sc.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sc.groupToAgg[groupKey].(string)
			if aggValue < currentVal {
				sc.groupToAgg[groupKey] = aggValue
			}
		}

	case Max:
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
	case Count:
		return int32(0)
	case Min:
		return ""
	case Max:
		return ""
	default:
		return int32(0)
	}
}

func (sc *StringCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	aggValue := sc.groupToAgg[groupKey]

	switch v := aggValue.(type) {
	case string:
		return types.NewStringField(v, len(v)), nil
	case int32:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

type StringAggregator struct {
	*BaseAggregator
}

func NewStringAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*StringAggregator, error) {
	calculator := NewStringCalculator(op)
	base, err := NewBaseAggregator(gbField, gbFieldType, aField, op, calculator)
	if err != nil {
		return nil, err
	}

	return &StringAggregator{
		BaseAggregator: base,
	}, nil
}
