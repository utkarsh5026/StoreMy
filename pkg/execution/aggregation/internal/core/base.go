package core

import (
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// BaseAggregator contains all common aggregation functionality.
// This eliminates code duplication across all aggregator types.
//
// The BaseAggregator implements the core logic for SQL aggregation operations
// (COUNT, SUM, AVG, MIN, MAX) with optional GROUP BY support. It delegates
// type-specific calculation logic to an AggregateCalculator implementation.
//
// Thread-safety: All public methods that modify state are protected by a mutex.
// Use RLock/RUnlock for read-only operations when iterating over results.
type BaseAggregator struct {
	gbField     primitives.ColumnID     // Index of grouping field (-1 for NoGrouping)
	gbFieldType types.Type              // Type of grouping field
	aField      primitives.ColumnID     // Index of field to aggregate
	op          AggregateOp             // Aggregation operation (COUNT, SUM, AVG, MIN, MAX)
	tupleDesc   *tuple.TupleDescription // Description of result tuples
	mutex       sync.RWMutex            // Protects concurrent access to groups and calculator
	groups      map[string]bool         // Track which groups exist (groupKey -> exists)
	calculator  AggregateCalculator     // Type-specific aggregation logic
}

// NewBaseAggregator creates a new BaseAggregator instance.
//
// Parameters:
//   - gbField: Index of the grouping field (use NoGrouping constant for no grouping)
//   - gbFieldType: Type of the grouping field
//   - aField: Index of the field to aggregate
//   - op: Aggregation operation to perform (COUNT, SUM, AVG, MIN, MAX)
//   - calculator: Type-specific calculator for performing aggregations
//
// Returns:
//   - *BaseAggregator: Initialized aggregator ready to process tuples
//   - error: If the operation is invalid for the given calculator or tuple description fails
func NewBaseAggregator(gbField primitives.ColumnID, gbFieldType types.Type, aField primitives.ColumnID, op AggregateOp, calculator AggregateCalculator) (*BaseAggregator, error) {
	if err := calculator.ValidateOperation(op); err != nil {
		return nil, err
	}

	agg := &BaseAggregator{
		gbField:     gbField,
		gbFieldType: gbFieldType,
		aField:      aField,
		op:          op,
		groups:      make(map[string]bool),
		calculator:  calculator,
	}

	tupleDesc, err := agg.createTupleDesc()
	if err != nil {
		return nil, err
	}
	agg.tupleDesc = tupleDesc
	return agg, nil
}

// createTupleDesc creates the tuple description for aggregation results.
// For non-grouped aggregates, returns a single field with the aggregate result.
// For grouped aggregates, returns two fields: group key and aggregate result.
func (ba *BaseAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	resultType := ba.calculator.GetResultType(ba.op)

	if ba.gbField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{resultType},
			[]string{ba.op.String()},
		)
	} else {
		return tuple.NewTupleDesc(
			[]types.Type{ba.gbFieldType, resultType},
			[]string{"group", ba.op.String()},
		)
	}
}

// GetGroups returns all group keys that have been processed.
// For non-grouped aggregates, returns a single "NO_GROUPING" key.
//
// Returns:
//   - []string: Slice of group keys (order is non-deterministic)
func (ba *BaseAggregator) GetGroups() []string {
	return slices.Collect(maps.Keys(ba.groups))
}

// GetAggregateValue retrieves the computed aggregate value for a specific group.
//
// Parameters:
//   - groupKey: The group identifier (use "NO_GROUPING" for non-grouped aggregates)
//
// Returns:
//   - types.Field: The computed aggregate value
//   - error: If the group doesn't exist or value retrieval fails
func (ba *BaseAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	return ba.calculator.GetFinalValue(groupKey)
}

// GetTupleDesc returns the tuple description for aggregation result tuples.
//
// Returns:
//   - *tuple.TupleDescription: Description of result tuples
func (ba *BaseAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ba.tupleDesc
}

// GetGroupingField returns the index of the grouping field.
//
// Returns:
//   - int: Index of the grouping field (NoGrouping if no grouping is used)
func (ba *BaseAggregator) GetGroupingField() primitives.ColumnID {
	return ba.gbField
}

// RLock acquires a read lock on the aggregator.
// Use this when reading aggregate results to ensure consistency.
// Must be followed by RUnlock().
func (ba *BaseAggregator) RLock() {
	ba.mutex.RLock()
}

// RUnlock releases a read lock on the aggregator.
// Must be called after RLock() when done reading.
func (ba *BaseAggregator) RUnlock() {
	ba.mutex.RUnlock()
}

// Iterator creates a new iterator over the aggregation results.
// Each result tuple contains the group key (if grouped) and aggregate value.
//
// Returns:
//   - iterator.DbIterator: Iterator for accessing aggregation results
func (ba *BaseAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(ba)
}

// Merge processes a new tuple into the aggregate.
// This is the main method for feeding data into the aggregator.
//
// The method extracts the group key from the tuple, retrieves the field
// to aggregate, and delegates the actual aggregation to the calculator.
// New groups are automatically initialized on first encounter.
//
// Thread-safe: This method is protected by a write lock.
//
// Parameters:
//   - tup: The tuple to merge into the aggregate
//
// Returns:
//   - error: If field extraction fails or aggregation update fails
func (ba *BaseAggregator) Merge(tup *tuple.Tuple) error {
	ba.mutex.Lock()
	defer ba.mutex.Unlock()

	groupKey, err := ba.extractGroupKey(tup)
	if err != nil {
		return err
	}

	aggField, err := tup.GetField(ba.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	if !ba.groups[groupKey] {
		ba.calculator.InitializeGroup(groupKey)
		ba.groups[groupKey] = true
	}

	return ba.calculator.UpdateAggregate(groupKey, aggField)
}

// extractGroupKey extracts the grouping key from a tuple.
// For non-grouped aggregates, returns the constant "NO_GROUPING".
// For grouped aggregates, returns the string representation of the grouping field.
func (ba *BaseAggregator) extractGroupKey(tup *tuple.Tuple) (string, error) {
	if ba.gbField == NoGrouping {
		return "NO_GROUPING", nil
	}

	groupField, err := tup.GetField(ba.gbField)
	if err != nil {
		return "", fmt.Errorf("failed to get grouping field: %v", err)
	}

	return groupField.String(), nil
}

// InitializeDefault initializes the default group for non-grouped aggregates.
// This ensures that COUNT(*) on an empty table returns 0 instead of no rows.
//
// For grouped aggregates, this method does nothing as groups are created
// dynamically as tuples are processed.
//
// This method should be called before processing any tuples when you want
// to ensure a result even if no tuples are merged.
//
// Returns:
//   - error: Always returns nil, provided for interface consistency
func (ba *BaseAggregator) InitializeDefault() error {
	if ba.gbField != NoGrouping {
		return nil
	}

	ba.mutex.Lock()
	defer ba.mutex.Unlock()

	groupKey := "NO_GROUPING"
	if !ba.groups[groupKey] {
		ba.calculator.InitializeGroup(groupKey)
		ba.groups[groupKey] = true
	}

	return nil
}
