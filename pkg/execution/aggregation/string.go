package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// StringAggregator handles aggregation over string fields
// Supports COUNT, Min, and Max operations (strings ordered lexicographically)
type StringAggregator struct {
	gbField      int                     // Index of grouping field
	gbFieldType  types.Type              // Type of grouping field
	aField       int                     // Index of field to aggregate
	op           AggregateOp             // Aggregation operation
	groupToAgg   map[string]interface{}  // Maps group value to aggregate result (string or int32)
	groupToCount map[string]int32        // Maps group value to count
	tupleDesc    *tuple.TupleDescription // Description of result tuples
	mutex        sync.RWMutex            // Protects concurrent access
}

func NewStringAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*StringAggregator, error) {
	switch op {
	case Count, Min, Max:
		// These are supported
	default:
		return nil, fmt.Errorf("string aggregator does not support operation: %s", op.String())
	}

	agg := &StringAggregator{
		gbField:      gbField,
		gbFieldType:  gbFieldType,
		aField:       aField,
		op:           op,
		groupToAgg:   make(map[string]any),
		groupToCount: make(map[string]int32),
	}

	td, err := agg.createTupleDesc()
	if err != nil {
		return nil, fmt.Errorf("error creating StringAggregator %v", err)
	}
	agg.tupleDesc = td
	return agg, nil
}

func (sa *StringAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	var resultType types.Type
	var fieldName string

	switch sa.op {
	case Count:
		resultType = types.IntType
		fieldName = "COUNT"
	case Min:
		resultType = types.StringType
		fieldName = "MIN"
	case Max:
		resultType = types.StringType
		fieldName = "MAX"
	default:
		resultType = types.IntType
		fieldName = sa.op.String()
	}

	if sa.gbField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{resultType},
			[]string{fieldName},
		)
	}
	return tuple.NewTupleDesc(
		[]types.Type{sa.gbFieldType, resultType},
		[]string{"group", fieldName},
	)
}

func (sa *StringAggregator) GetTupleDesc() *tuple.TupleDescription {
	return sa.tupleDesc
}

// getInitValue returns the initial value for different string operations
func (sa *StringAggregator) getInitValue() interface{} {
	switch sa.op {
	case Count:
		return int32(0)
	case Min:
		return "" // Will be set to first actual value
	case Max:
		return "" // Will be set to first actual value
	default:
		return int32(0)
	}
}

func (sa *StringAggregator) Merge(tup *tuple.Tuple) error {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	var groupKey string
	if sa.gbField == NoGrouping {
		groupKey = "NO_GROUPING"
	} else {
		groupField, err := tup.GetField(sa.gbField)
		if err != nil {
			return fmt.Errorf("failed to get grouping field: %v", err)
		}
		groupKey = groupField.String()
	}

	aggField, err := tup.GetField(sa.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	stringField, ok := aggField.(*types.StringField)
	if !ok {
		return fmt.Errorf("aggregate field is not a string")
	}

	aggValue := stringField.Value

	// Initialize group if not exists
	if _, exists := sa.groupToAgg[groupKey]; !exists {
		sa.groupToAgg[groupKey] = sa.getInitValue()
		sa.groupToCount[groupKey] = 0
	}

	// Update aggregate value based on operation
	switch sa.op {
	case Count:
		currentVal := sa.groupToAgg[groupKey].(int32)
		sa.groupToAgg[groupKey] = currentVal + 1
	case Min:
		if sa.groupToCount[groupKey] == 0 {
			// First value
			sa.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sa.groupToAgg[groupKey].(string)
			if aggValue < currentVal {
				sa.groupToAgg[groupKey] = aggValue
			}
		}
	case Max:
		if sa.groupToCount[groupKey] == 0 {
			// First value
			sa.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sa.groupToAgg[groupKey].(string)
			if aggValue > currentVal {
				sa.groupToAgg[groupKey] = aggValue
			}
		}
	default:
		return fmt.Errorf("unsupported string operation: %v", sa.op)
	}

	sa.groupToCount[groupKey]++
	return nil
}

func (sa *StringAggregator) GetGroups() []string {
	groups := make([]string, 0, len(sa.groupToAgg))
	for groupKey := range sa.groupToAgg {
		groups = append(groups, groupKey)
	}
	return groups
}

func (sa *StringAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	aggValue := sa.groupToAgg[groupKey]

	switch v := aggValue.(type) {
	case string:
		return types.NewStringField(v, len(v)), nil
	case int32:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

func (sa *StringAggregator) GetGroupingField() int {
	return sa.gbField
}

func (sa *StringAggregator) RLock() {
	sa.mutex.RLock()
}

func (sa *StringAggregator) RUnlock() {
	sa.mutex.RUnlock()
}

func (sa *StringAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(sa)
}
