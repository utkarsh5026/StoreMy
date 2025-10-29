package core

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// AggregatorIterator provides iterator-based access to aggregation results.
// It implements the DbIterator interface to allow sequential traversal of
// aggregate values computed by a GroupAggregator.
//
// Snapshot Behavior:
// The iterator materializes all result tuples at Open() time. This means:
//   - New groups added after Open() won't be visible in this iteration
//   - The snapshot is refreshed on each Open() call
//   - Rewind() does NOT refresh the snapshot - it uses the same tuples from Open()
//
// Result Format:
//   - Non-grouped aggregates: Single-field tuple containing aggregate value
//   - Grouped aggregates: Two-field tuple (group key, aggregate value)
//
// Thread Safety:
// The iterator itself is NOT thread-safe. Each goroutine should use its own
// iterator instance. However, the iterator safely coordinates with the underlying
// aggregator which may be accessed concurrently during Open().
type AggregatorIterator struct {
	aggregator GroupAggregator                       // The aggregator to iterate over (immutable after construction)
	tupleIter  *iterator.SliceIterator[*tuple.Tuple] // Internal iterator over materialized tuples
	opened     bool                                  // Track if the iterator is currently open
}

// NewAggregatorIterator creates a new iterator for the given aggregator.
//
// The iterator is created in closed state - call Open() before use.
//
// Parameters:
//   - agg: The GroupAggregator to iterate over (must not be nil)
//
// Returns:
//   - *AggregatorIterator: A new iterator ready to be opened
//
// Panics:
//   - If agg is nil
func NewAggregatorIterator(agg GroupAggregator) *AggregatorIterator {
	if agg == nil {
		panic("NewAggregatorIterator: aggregator cannot be nil")
	}

	return &AggregatorIterator{
		aggregator: agg,
		tupleIter:  iterator.NewSliceIterator[*tuple.Tuple](nil),
		opened:     false,
	}
}

// Open initializes the iterator and materializes all aggregation result tuples.
//
// This method:
//   - Takes a read lock on the aggregator to safely capture groups and values
//   - Materializes all result tuples at once (group key + aggregate value)
//   - Resets iteration state to the beginning
//
// The snapshot is stable for the lifetime of this iteration (until Close/Reopen).
// New groups added to the aggregator after Open() won't appear in this iteration.
//
// Must be called before HasNext() or Next(). Can be called again after Close() to reopen.
//
// Returns:
//   - error: If the iterator is already opened (call Close() first), or if
//     tuple materialization fails
func (i *AggregatorIterator) Open() error {
	if i.opened {
		return fmt.Errorf("iterator already opened - call Close() first")
	}

	i.aggregator.RLock()
	groups := i.aggregator.GetGroups()
	isGrouped := i.aggregator.GetGroupingField() != NoGrouping
	tupleDesc := i.aggregator.GetTupleDesc()

	tuples := make([]*tuple.Tuple, 0, len(groups))

	for _, groupKey := range groups {
		aggValue, err := i.aggregator.GetAggregateValue(groupKey)
		if err != nil {
			i.aggregator.RUnlock()
			return fmt.Errorf("failed to get aggregate value for group '%s': %w", groupKey, err)
		}

		resultTuple := tuple.NewTuple(tupleDesc)

		if !isGrouped {
			if err := resultTuple.SetField(0, aggValue); err != nil {
				i.aggregator.RUnlock()
				return fmt.Errorf("failed to set aggregate field: %w", err)
			}
		} else {
			groupField := types.NewStringField(groupKey, len(groupKey))

			if err := resultTuple.SetField(0, groupField); err != nil {
				i.aggregator.RUnlock()
				return fmt.Errorf("failed to set group field: %w", err)
			}
			if err := resultTuple.SetField(1, aggValue); err != nil {
				i.aggregator.RUnlock()
				return fmt.Errorf("failed to set aggregate field: %w", err)
			}
		}

		tuples = append(tuples, resultTuple)
	}
	i.aggregator.RUnlock()

	// Create new iterator with materialized tuples (always ready to use)
	i.tupleIter = iterator.NewSliceIterator(tuples)
	i.opened = true

	return nil
}

// HasNext checks if there are more tuples available.
//
// Returns true if there are more tuples to iterate over in the snapshot.
//
// Returns:
//   - bool: true if more tuples are available, false otherwise
//   - error: If the iterator is not opened
func (i *AggregatorIterator) HasNext() (bool, error) {
	if !i.opened {
		return false, fmt.Errorf("iterator not opened - call Open() first")
	}
	return i.tupleIter.HasNext(), nil
}

// Next returns the next tuple from the aggregation results.
//
// Returns:
//   - *tuple.Tuple: The next result tuple
//   - error: If iterator not opened, or no more tuples available
func (i *AggregatorIterator) Next() (*tuple.Tuple, error) {
	return i.tupleIter.Next()
}

// Rewind resets the iterator to the beginning, allowing re-iteration.
//
// Returns:
//   - error: If the iterator is not opened
func (i *AggregatorIterator) Rewind() error {
	if !i.opened {
		return fmt.Errorf("iterator not opened - call Open() first")
	}
	return i.tupleIter.Rewind()
}

// Close releases resources held by the iterator.
// After closing, the iterator can be reopened with Open() to get a fresh snapshot.
// Multiple calls to Close() are safe (idempotent).
//
// Returns:
//   - error: Always returns nil (kept for interface compatibility)
func (i *AggregatorIterator) Close() error {
	i.opened = false
	// Release tuple data for GC
	i.tupleIter = iterator.NewSliceIterator[*tuple.Tuple](nil)
	return nil
}

// GetTupleDesc returns the tuple description for result tuples.
//
// The tuple description defines the schema of tuples returned by this iterator:
//
// Non-grouped aggregates:
//   - Field 0: Aggregate result (e.g., INT for COUNT, DOUBLE for AVG)
//   - Example: [COUNT(*)] or [SUM(amount)]
//
// Grouped aggregates:
//   - Field 0: Group key (type matches the grouping field)
//   - Field 1: Aggregate result
//   - Example: [category, COUNT(*)] or [region, SUM(sales)]
//
// This method can be called at any time (iterator doesn't need to be opened).
//
// Returns:
//   - *tuple.TupleDescription: Description of the result tuples
func (i *AggregatorIterator) GetTupleDesc() *tuple.TupleDescription {
	return i.aggregator.GetTupleDesc()
}
