package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// StringAggregator handles aggregation over string fields.
// Supports COUNT, Min, and Max operations with strings ordered lexicographically.
type StringAggregator struct {
	gbField      int                     // Index of grouping field (-1 for no grouping)
	gbFieldType  types.Type              // Type of grouping field
	aField       int                     // Index of field to aggregate
	op           AggregateOp             // Aggregation operation (Count, Min, Max)
	groupToAgg   map[string]any          // Maps group value to aggregate result (string or int32)
	groupToCount map[string]int32        // Maps group value to count
	tupleDesc    *tuple.TupleDescription // Description of result tuples
	mutex        sync.RWMutex            // Protects concurrent access to aggregation state
}

// NewStringAggregator creates a new string aggregator with the specified parameters.
//
// Parameters:
//   - gbField: Index of the grouping field (use NoGrouping for no grouping)
//   - gbFieldType: Type of the grouping field
//   - aField: Index of the field to aggregate (must be string type)
//   - op: Aggregation operation (Count, Min, Max)
//
// Returns:
//   - *StringAggregator: The created aggregator
//   - error: Error if the operation is not supported
func NewStringAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*StringAggregator, error) {
	switch op {
	case Count, Min, Max:
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

// createTupleDesc creates the tuple description for the aggregation result.
// The result contains either one field (aggregate result) or two fields
// (grouping field + aggregate result) depending on whether grouping is used.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
//   - error: Error if tuple description creation fails
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

// GetTupleDesc returns the tuple description for the aggregation result.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
func (sa *StringAggregator) GetTupleDesc() *tuple.TupleDescription {
	return sa.tupleDesc
}

// getInitValue returns the initial value for different string operations.
//
// Returns:
//   - int32(0) for Count operation
//   - "" (empty string) for Min and Max operations (will be set to first actual value)
func (sa *StringAggregator) getInitValue() interface{} {
	switch sa.op {
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

// Merge processes a single tuple and updates the aggregation state.
// This method is thread-safe and can be called concurrently.
//
// Parameters:
//   - tup: The tuple to process and merge into the aggregation
//
// Returns:
//   - error: Error if tuple processing fails (e.g., field access errors, type mismatches)
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
	if _, exists := sa.groupToAgg[groupKey]; !exists {
		sa.groupToAgg[groupKey] = sa.getInitValue()
		sa.groupToCount[groupKey] = 0
	}

	switch sa.op {
	case Count:
		currentVal := sa.groupToAgg[groupKey].(int32)
		sa.groupToAgg[groupKey] = currentVal + 1
	case Min:
		if sa.groupToCount[groupKey] == 0 {
			sa.groupToAgg[groupKey] = aggValue
		} else {
			currentVal := sa.groupToAgg[groupKey].(string)
			if aggValue < currentVal {
				sa.groupToAgg[groupKey] = aggValue
			}
		}
	case Max:
		if sa.groupToCount[groupKey] == 0 {
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

// GetGroups returns all group keys that have been processed.
//
// Returns:
//   - []string: Slice of group keys
func (sa *StringAggregator) GetGroups() []string {
	groups := make([]string, 0, len(sa.groupToAgg))
	for groupKey := range sa.groupToAgg {
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
//   - types.Field: The aggregate value as a field (StringField for Min/Max, IntField for Count)
//   - error: Error if the group key doesn't exist or value type is unexpected
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

// GetGroupingField returns the index of the grouping field.
//
// Returns:
//   - int: Index of the grouping field (NoGrouping if no grouping is used)
func (sa *StringAggregator) GetGroupingField() int {
	return sa.gbField
}

// RLock acquires a read lock on the aggregator's mutex.
func (sa *StringAggregator) RLock() {
	sa.mutex.RLock()
}

// RUnlock releases a read lock on the aggregator's mutex.
func (sa *StringAggregator) RUnlock() {
	sa.mutex.RUnlock()
}

// Iterator returns a database iterator for the aggregation results.
//
// Returns:
//   - iterator.DbIterator: Iterator that yields result tuples
func (sa *StringAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(sa)
}
