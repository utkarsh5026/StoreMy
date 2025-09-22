package aggregation

import (
	"fmt"
	"math"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// FloatAggregator handles aggregation over float fields.
// Supports MIN, MAX, SUM, AVG, and COUNT operations on float64 values.
type FloatAggregator struct {
	gbField      int                     // Index of grouping field (-1 for no grouping)
	gbFieldType  types.Type              // Type of grouping field
	aField       int                     // Index of field to aggregate
	op           AggregateOp             // Aggregation operation (Min, Max, Sum, Avg, Count)
	groupToAgg   map[string]float64      // Maps group value to aggregate result
	groupToCount map[string]int32        // Maps group value to count (for AVG operations)
	tupleDesc    *tuple.TupleDescription // Description of result tuples
	mutex        sync.RWMutex            // Protects concurrent access to aggregation state
}

// NewFloatAggregator creates a new float aggregator with the specified parameters.
//
// Parameters:
//   - gbField: Index of the grouping field (use NoGrouping for no grouping)
//   - gbFieldType: Type of the grouping field
//   - aField: Index of the field to aggregate (must be float type)
//   - op: Aggregation operation (Min, Max, Sum, Avg, Count)
//
// Returns:
//   - *FloatAggregator: The created aggregator
func NewFloatAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) *FloatAggregator {
	agg := &FloatAggregator{
		gbField:      gbField,
		gbFieldType:  gbFieldType,
		aField:       aField,
		op:           op,
		groupToAgg:   make(map[string]float64),
		groupToCount: make(map[string]int32),
	}

	agg.tupleDesc, _ = agg.createTupleDesc()
	return agg
}

// getInitValue returns the initial value for different float operations.
//
// Returns:
//   - +Inf for Min operation (so any actual value will be smaller)
//   - -Inf for Max operation (so any actual value will be larger)
//   - 0.0 for Sum, Avg, and Count operations
func (fa *FloatAggregator) getInitValue() float64 {
	switch fa.op {
	case Min:
		return math.Inf(1)
	case Max:
		return math.Inf(-1)
	case Sum, Avg, Count:
		return 0.0
	default:
		return 0.0
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
func (fa *FloatAggregator) Merge(tup *tuple.Tuple) error {
	fa.mutex.Lock()
	defer fa.mutex.Unlock()

	var groupKey string
	if fa.gbField == NoGrouping {
		groupKey = "NO_GROUPING"
	} else {
		groupField, err := tup.GetField(fa.gbField)
		if err != nil {
			return fmt.Errorf("failed to get grouping field: %v", err)
		}
		groupKey = groupField.String()
	}

	aggField, err := tup.GetField(fa.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	floatField, ok := aggField.(*types.Float64Field)
	if !ok {
		return fmt.Errorf("aggregate field is not a float")
	}

	aggValue := floatField.Value
	if _, exists := fa.groupToAgg[groupKey]; !exists {
		fa.groupToAgg[groupKey] = fa.getInitValue()
		if fa.op == Avg {
			fa.groupToCount[groupKey] = 0
		}
	}

	currentAgg := fa.groupToAgg[groupKey]

	switch fa.op {
	case Min:
		if aggValue < currentAgg {
			fa.groupToAgg[groupKey] = aggValue
		}
	case Max:
		if aggValue > currentAgg {
			fa.groupToAgg[groupKey] = aggValue
		}
	case Sum:
		fa.groupToAgg[groupKey] = currentAgg + aggValue
	case Avg:
		fa.groupToAgg[groupKey] = currentAgg + aggValue
		fa.groupToCount[groupKey]++
	case Count:
		fa.groupToAgg[groupKey] = currentAgg + 1.0
	default:
		return fmt.Errorf("unsupported float operation: %v", fa.op)
	}

	return nil
}

// createTupleDesc creates the tuple description for the aggregation result.
// The result contains either one field (aggregate result) or two fields
// (grouping field + aggregate result) depending on whether grouping is used.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
//   - error: Error if tuple description creation fails
func (fa *FloatAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	var resultType types.Type
	if fa.op == Count {
		resultType = types.IntType
	} else {
		resultType = types.FloatType
	}

	if fa.gbField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{resultType},
			[]string{fa.op.String()},
		)
	} else {
		return tuple.NewTupleDesc(
			[]types.Type{fa.gbFieldType, resultType},
			[]string{"group", fa.op.String()},
		)
	}
}

// GetTupleDesc returns the tuple description for the aggregation result.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
func (fa *FloatAggregator) GetTupleDesc() *tuple.TupleDescription {
	return fa.tupleDesc
}

// GetGroups returns all group keys that have been processed.
//
// Returns:
//   - []string: Slice of group keys
func (fa *FloatAggregator) GetGroups() []string {
	groups := make([]string, 0, len(fa.groupToAgg))
	for groupKey := range fa.groupToAgg {
		groups = append(groups, groupKey)
	}
	return groups
}

// GetAggregateValue returns the aggregate value for a specific group.
// For AVG operations, this method calculates the final average by dividing
// the accumulated sum by the count.
//
// Parameters:
//   - groupKey: The group key to get the aggregate value for
//
// Returns:
//   - types.Field: The aggregate value as a field (Float64Field for numeric ops, IntField for Count)
//   - error: Error if the group key doesn't exist
func (fa *FloatAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	aggValue := fa.groupToAgg[groupKey]

	if fa.op == Avg {
		count := fa.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / float64(count)
		}
	}

	if fa.op == Count {
		return types.NewIntField(int32(aggValue)), nil
	} else {
		return types.NewFloat64Field(aggValue), nil
	}
}

// GetGroupingField returns the index of the grouping field.
//
// Returns:
//   - int: Index of the grouping field (NoGrouping if no grouping is used)
func (fa *FloatAggregator) GetGroupingField() int {
	return fa.gbField
}

// RLock acquires a read lock on the aggregator's mutex.
func (fa *FloatAggregator) RLock() {
	fa.mutex.RLock()
}

// RUnlock releases a read lock on the aggregator's mutex.
func (fa *FloatAggregator) RUnlock() {
	fa.mutex.RUnlock()
}

// Iterator returns a database iterator for the aggregation results.
//
// Returns:
//   - iterator.DbIterator: Iterator that yields result tuples
func (fa *FloatAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(fa)
}
