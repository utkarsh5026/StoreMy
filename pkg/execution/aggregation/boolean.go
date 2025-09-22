package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

type BooleanAggregator struct {
	gbField      int                     // Index of grouping field
	gbFieldType  types.Type              // Type of grouping field
	aField       int                     // Index of field to aggregate
	op           AggregateOp             // Aggregation operation
	groupToAgg   map[string]interface{}  // Maps group value to aggregate result (bool or int32)
	groupToCount map[string]int32        // Maps group value to count (for AVG)
	tupleDesc    *tuple.TupleDescription // Description of result tuples
	mutex        sync.RWMutex            // Protects concurrent access
}

func NewBooleanAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*BooleanAggregator, error) {
	switch op {
	case Count, And, Or, Sum:
		// These are supported
	default:
		return nil, fmt.Errorf("boolean aggregator does not support operation: %s", op.String())
	}

	agg := &BooleanAggregator{
		gbField:      gbField,
		gbFieldType:  gbFieldType,
		aField:       aField,
		op:           op,
		groupToAgg:   make(map[string]interface{}),
		groupToCount: make(map[string]int32),
	}

	tupleDesc, err := agg.createTupleDesc()
	if err != nil {
		return nil, err
	}
	agg.tupleDesc = tupleDesc
	return agg, nil
}

// getInitValue returns the initial value for different boolean operations
func (ba *BooleanAggregator) getInitValue() any {
	switch ba.op {
	case And:
		return true
	case Or:
		return false
	case Count, Sum:
		return int32(0)
	default:
		return int32(0)
	}
}

func (ba *BooleanAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	var resultType types.Type
	switch ba.op {
	case And, Or:
		resultType = types.BoolType
	case Count, Sum:
		resultType = types.IntType
	default:
		resultType = types.IntType
	}

	if ba.gbField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{resultType},
			[]string{ba.op.String()},
		)
	}

	return tuple.NewTupleDesc(
		[]types.Type{ba.gbFieldType, resultType},
		[]string{"group", ba.op.String()},
	)

}

func (ba *BooleanAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ba.tupleDesc
}

func (ba *BooleanAggregator) Merge(tup *tuple.Tuple) error {
	ba.mutex.Lock()
	defer ba.mutex.Unlock()

	// Extract group value
	var groupKey string
	if ba.gbField == NoGrouping {
		groupKey = "NO_GROUPING"
	} else {
		groupField, err := tup.GetField(ba.gbField)
		if err != nil {
			return fmt.Errorf("failed to get grouping field: %v", err)
		}
		groupKey = groupField.String()
	}

	// Extract aggregate field value
	aggField, err := tup.GetField(ba.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	boolField, ok := aggField.(*types.BoolField)
	if !ok {
		return fmt.Errorf("aggregate field is not a boolean")
	}

	aggValue := boolField.Value

	// Initialize group if not exists
	if _, exists := ba.groupToAgg[groupKey]; !exists {
		ba.groupToAgg[groupKey] = ba.getInitValue()
		ba.groupToCount[groupKey] = 0
	}

	// Update aggregate value based on operation
	switch ba.op {
	case And:
		currentVal := ba.groupToAgg[groupKey].(bool)
		ba.groupToAgg[groupKey] = currentVal && aggValue
	case Or:
		currentVal := ba.groupToAgg[groupKey].(bool)
		ba.groupToAgg[groupKey] = currentVal || aggValue
	case Sum:
		currentVal := ba.groupToAgg[groupKey].(int32)
		if aggValue {
			ba.groupToAgg[groupKey] = currentVal + 1
		}
		// else add 0 (false)
	case Count:
		currentVal := ba.groupToAgg[groupKey].(int32)
		ba.groupToAgg[groupKey] = currentVal + 1
	default:
		return fmt.Errorf("unsupported boolean operation: %v", ba.op)
	}

	ba.groupToCount[groupKey]++
	return nil
}

func (ba *BooleanAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(ba)
}

func (ba *BooleanAggregator) GetGroups() []string {
	groups := make([]string, 0, len(ba.groupToAgg))
	for groupKey := range ba.groupToAgg {
		groups = append(groups, groupKey)
	}
	return groups
}

func (ba *BooleanAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	aggValue := ba.groupToAgg[groupKey]
	switch v := aggValue.(type) {
	case bool:
		return types.NewBoolField(v), nil
	case int32:
		return types.NewIntField(v), nil
	default:
		return nil, fmt.Errorf("unexpected aggregate value type: %T", v)
	}
}

func (ba *BooleanAggregator) GetGroupingField() int {
	return ba.gbField
}

func (ba *BooleanAggregator) RLock() {
	ba.mutex.RLock()
}

func (ba *BooleanAggregator) RUnlock() {
	ba.mutex.RUnlock()
}
