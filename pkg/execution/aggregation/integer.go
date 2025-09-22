package aggregation

import (
	"fmt"
	"math"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// IntegerAggregator handles aggregation operations over integer fields.
// It maintains internal state for each group and supports all aggregate operations
// including MIN, MAX, SUM, AVG, and COUNT. The aggregator is thread-safe and
// can handle both grouped and non-grouped aggregations.
type IntegerAggregator struct {
	groupByField   int                     // Field index to group by (-1 for no grouping)
	groupFieldType types.Type              // Type of the grouping field
	aggrField      int                     // Field index to aggregate
	op             AggregateOp             // Aggregation operation to perform
	groupToAgg     map[string]int32        // Maps group keys to aggregate values
	groupToCount   map[string]int32        // Maps group keys to counts (used for AVG)
	tupleDesc      *tuple.TupleDescription // Description of output tuples
	mutex          sync.RWMutex            // Protects concurrent access to maps
}

// NewIntAggregator creates a new integer aggregator with the specified configuration.
//
// Parameters:
//   - gbField: Field index to group by (use NoGrouping constant for no grouping)
//   - gbFieldType: Type of the grouping field (ignored if gbField is NoGrouping)
//   - aField: Field index containing values to aggregate
//   - op: Aggregation operation to perform (Min, Max, Sum, Avg, Count)
//
// Returns:
//   - *IntegerAggregator: Configured aggregator instance
//   - error: Error if tuple description creation fails
func NewIntAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*IntegerAggregator, error) {
	agg := &IntegerAggregator{
		groupByField:   gbField,
		groupFieldType: gbFieldType,
		aggrField:      aField,
		op:             op,
		groupToAgg:     make(map[string]int32),
		groupToCount:   make(map[string]int32),
	}

	td, err := agg.createTupleDesc()
	if err != nil {
		return nil, fmt.Errorf("error creating IntegerAggregator %v", err)
	}
	agg.tupleDesc = td
	return agg, nil
}

// createTupleDesc creates the tuple description for the aggregator's output.
// For non-grouped aggregations, returns a single-field tuple with the aggregate result.
// For grouped aggregations, returns a two-field tuple with the group key and aggregate result.
//
// Returns:
//   - *tuple.TupleDescription: Description of output tuples
//   - error: Error if tuple description creation fails
func (ia *IntegerAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	if ia.groupByField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{types.IntType},
			[]string{ia.op.String()},
		)
	}

	return tuple.NewTupleDesc(
		[]types.Type{ia.groupFieldType, types.IntType},
		[]string{"group", ia.op.String()},
	)
}

// GetTupleDesc returns the tuple description for this aggregator's output.
//
// Returns:
//   - *tuple.TupleDescription: The output tuple description
func (ia *IntegerAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ia.tupleDesc
}

// Merge processes a single tuple and updates the aggregation state.
// The tuple is processed by extracting the group key and aggregate value,
// then updating the internal aggregation maps accordingly.
//
// Parameters:
//   - tup: The tuple to process and merge into the aggregation
//
// Returns:
//   - error: Error if tuple processing fails (invalid fields, type mismatches, etc.)
func (ia *IntegerAggregator) Merge(tup *tuple.Tuple) error {
	ia.mutex.Lock()
	defer ia.mutex.Unlock()

	groupKey, err := ia.extractGroupKey(tup)
	if err != nil {
		return fmt.Errorf("failed to extract group key: %w", err)
	}

	aggField, err := tup.GetField(ia.aggrField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	intField, ok := aggField.(*types.IntField)
	if !ok {
		return fmt.Errorf("aggregate field is not an integer")
	}

	aggValue := intField.Value
	ia.initializeGroupIfNeeded(groupKey)
	return ia.updateAggregate(groupKey, aggValue)
}

// getInitValue returns the appropriate initial value for the aggregation operation.
//
// Returns:
//   - int32: The initial value for the current aggregation operation
func (ia *IntegerAggregator) getInitValue() int32 {
	switch ia.op {
	case Min:
		return math.MaxInt32
	case Max:
		return math.MinInt32
	case Sum, Avg, Count:
		return 0
	default:
		return 0
	}
}

// updateAggregate updates the aggregate value for a specific group with a new value.
//
// Parameters:
//   - groupKey: The group identifier
//   - aggValue: The new value to incorporate into the aggregate
//
// Returns:
//   - error: Error if the operation is unsupported
func (ia *IntegerAggregator) updateAggregate(groupKey string, aggValue int32) error {
	currentAgg := ia.groupToAgg[groupKey]

	switch ia.op {
	case Min:
		if aggValue < currentAgg {
			ia.groupToAgg[groupKey] = aggValue
		}

	case Max:
		if aggValue > currentAgg {
			ia.groupToAgg[groupKey] = aggValue
		}

	case Sum:
		ia.groupToAgg[groupKey] = currentAgg + aggValue

	case Avg:
		ia.groupToAgg[groupKey] = currentAgg + aggValue
		ia.groupToCount[groupKey]++

	case Count:
		ia.groupToAgg[groupKey]++

	default:
		return fmt.Errorf("unsupported operation: %v", ia.op)
	}

	return nil
}

// initializeGroupIfNeeded initializes a new group with default values if it doesn't exist.
// This ensures that each group starts with the appropriate initial value for the
// aggregation operation. For AVG operations, it also initializes the count to 0.
//
// Parameters:
//   - groupKey: The group identifier to initialize
func (ia *IntegerAggregator) initializeGroupIfNeeded(groupKey string) {
	if _, exists := ia.groupToAgg[groupKey]; exists {
		return
	}

	ia.groupToAgg[groupKey] = ia.getInitValue()

	if ia.op == Avg {
		ia.groupToCount[groupKey] = 0
	}
}

// extractGroupKey extracts the grouping key from a tuple.
// For non-grouped aggregations (groupByField == NoGrouping), returns a constant key.
// For grouped aggregations, extracts the value from the specified grouping field
// and converts it to a string representation.
//
// Parameters:
//   - tup: The tuple to extract the group key from
//
// Returns:
//   - string: The group key as a string
//   - error: Error if the grouping field cannot be accessed
func (ia *IntegerAggregator) extractGroupKey(tup *tuple.Tuple) (string, error) {
	if ia.groupByField == NoGrouping {
		return "NO_GROUPING", nil
	}

	groupField, err := tup.GetField(ia.groupByField)
	if err != nil {
		return "", fmt.Errorf("failed to get grouping field: %w", err)
	}

	return groupField.String(), nil
}

// Iterator returns a DbIterator over the aggregation results.
//
// Returns:
//   - iterator.DbIterator: Iterator over the aggregation results
func (ia *IntegerAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(ia)
}

func (ia *IntegerAggregator) RLock() {
	ia.mutex.RLock()
}

func (ia *IntegerAggregator) RUnlock() {
	ia.mutex.RUnlock()
}

func (ia *IntegerAggregator) GetGroupingField() int {
	return ia.groupByField
}

func (ia *IntegerAggregator) GetGroups() []string {
	groups := make([]string, 0, len(ia.groupToAgg))
	for groupKey := range ia.groupToAgg {
		groups = append(groups, groupKey)
	}
	return groups
}

func (ia *IntegerAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	aggValue := ia.groupToAgg[groupKey]

	if ia.op == Avg {
		count := ia.groupToCount[groupKey]
		if count > 0 {
			aggValue = aggValue / count
		}
	}

	return types.NewIntField(aggValue), nil
}
