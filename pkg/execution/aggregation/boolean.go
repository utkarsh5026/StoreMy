package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// BooleanAggregator performs aggregation operations on boolean fields.
// It supports Count, And, Or, and Sum operations with optional grouping.
type BooleanAggregator struct {
	gbField      int                     // Index of grouping field (-1 for no grouping)
	gbFieldType  types.Type              // Type of grouping field
	aField       int                     // Index of field to aggregate
	op           AggregateOp             // Aggregation operation (Count, And, Or, Sum)
	groupToAgg   map[string]any          // Maps group value to aggregate result (bool or int32)
	groupToCount map[string]int32        // Maps group value to count (for AVG operations)
	tupleDesc    *tuple.TupleDescription // Description of result tuples
	mutex        sync.RWMutex            // Protects concurrent access to aggregation state
}

// NewBooleanAggregator creates a new boolean aggregator with the specified parameters.
//
// Parameters:
//   - gbField: Index of the grouping field (use NoGrouping for no grouping)
//   - gbFieldType: Type of the grouping field
//   - aField: Index of the field to aggregate (must be boolean type)
//   - op: Aggregation operation (Count, And, Or, Sum)
//
// Returns:
//   - *BooleanAggregator: The created aggregator
//   - error: Error if the operation is not supported
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

// getInitValue returns the initial value for different boolean operations.
//
// Returns:
//   - true for And operation (identity element for AND)
//   - false for Or operation (identity element for OR)
//   - 0 for Count and Sum operations
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

// createTupleDesc creates the tuple description for the aggregation result.
// The result contains either one field (aggregate result) or two fields
// (grouping field + aggregate result) depending on whether grouping is used.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
//   - error: Error if tuple description creation fails
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

// GetTupleDesc returns the tuple description for the aggregation result.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
func (ba *BooleanAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ba.tupleDesc
}

// Merge processes a single tuple and updates the aggregation state.
//
// Parameters:
//   - tup: The tuple to process and merge into the aggregation
//
// Returns:
//   - error: Error if tuple processing fails (e.g., field access errors, type mismatches)
func (ba *BooleanAggregator) Merge(tup *tuple.Tuple) error {
	ba.mutex.Lock()
	defer ba.mutex.Unlock()

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

	aggField, err := tup.GetField(ba.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	boolField, ok := aggField.(*types.BoolField)
	if !ok {
		return fmt.Errorf("aggregate field is not a boolean")
	}

	aggValue := boolField.Value

	if _, exists := ba.groupToAgg[groupKey]; !exists {
		ba.groupToAgg[groupKey] = ba.getInitValue()
		ba.groupToCount[groupKey] = 0
	}

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
	case Count:
		currentVal := ba.groupToAgg[groupKey].(int32)
		ba.groupToAgg[groupKey] = currentVal + 1
	default:
		return fmt.Errorf("unsupported boolean operation: %v", ba.op)
	}

	ba.groupToCount[groupKey]++
	return nil
}

// Iterator returns a database iterator for the aggregation results.
//
// Returns:
//   - iterator.DbIterator: Iterator that yields result tuples
func (ba *BooleanAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(ba)
}

// GetGroups returns all group keys that have been processed.
//
// Returns:
//   - []string: Slice of group keys
func (ba *BooleanAggregator) GetGroups() []string {
	groups := make([]string, 0, len(ba.groupToAgg))
	for groupKey := range ba.groupToAgg {
		groups = append(groups, groupKey)
	}
	return groups
}

// GetAggregateValue returns the aggregate value for a specific group.
//
// Parameters:
//   - groupKey: The group key to get the aggregate value for
//
// Returns:
//   - types.Field: The aggregate value as a field (BoolField for And/Or, IntField for Count/Sum)
//   - error: Error if the group key doesn't exist or value type is unexpected
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

// GetGroupingField returns the index of the grouping field.
//
// Returns:
//   - int: Index of the grouping field (NoGrouping if no grouping is used)
func (ba *BooleanAggregator) GetGroupingField() int {
	return ba.gbField
}

// RLock acquires a read lock on the aggregator's mutex.
func (ba *BooleanAggregator) RLock() {
	ba.mutex.RLock()
}

// RUnlock releases a read lock on the aggregator's mutex.
func (ba *BooleanAggregator) RUnlock() {
	ba.mutex.RUnlock()
}
