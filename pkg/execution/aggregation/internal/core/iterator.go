package core

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// AggregatorIterator provides iterator-based access to aggregation results.
// It implements the DbIterator interface to allow sequential traversal of
// aggregate values computed by a GroupAggregator.
//
// Snapshot Behavior:
// The iterator maintains a snapshot of group keys at Open() time and iterates
// through them, fetching aggregate values on demand. This means:
//   - New groups added after Open() won't be visible in this iteration
//   - The snapshot is refreshed on each Open() call
//   - Rewind() does NOT refresh the snapshot - it uses the same groups from Open()
//
// Result Format:
//   - Non-grouped aggregates: Single-field tuple containing aggregate value
//   - Grouped aggregates: Two-field tuple (group key, aggregate value)
//
// Thread Safety:
// The iterator itself is NOT thread-safe. Each goroutine should use its own
// iterator instance. However, the iterator safely coordinates with the underlying
// aggregator which may be accessed concurrently.
type AggregatorIterator struct {
	aggregator GroupAggregator         // The aggregator to iterate over (immutable after construction)
	tupleDesc  *tuple.TupleDescription // Description of result tuples (immutable after construction)
	groups     []string                // Snapshot of group keys taken at Open() time
	currentIdx int                     // Current position in groups slice [0, len(groups))
	opened     bool                    // Whether the iterator is currently open
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
		tupleDesc:  agg.GetTupleDesc(),
		currentIdx: 0,
		opened:     false,
	}
}

// Open initializes the iterator and takes a snapshot of the aggregator's groups.
//
// This method:
//   - Takes a read lock on the aggregator to safely capture the current groups
//   - Creates a snapshot of all group keys at this moment in time
//   - Resets iteration state to the beginning
//
// The snapshot is stable for the lifetime of this iteration (until Close/Reopen).
// New groups added to the aggregator after Open() won't appear in this iteration.
//
// Must be called before HasNext() or Next(). Can be called again after Close() to reopen.
//
// Returns:
//   - error: If the iterator is already opened (call Close() first)
func (i *AggregatorIterator) Open() error {
	if i.opened {
		return fmt.Errorf("iterator already opened - call Close() first")
	}

	i.aggregator.RLock()
	i.groups = i.aggregator.GetGroups()
	i.aggregator.RUnlock()

	i.currentIdx = 0
	i.opened = true

	return nil
}

// HasNext checks if there are more tuples available.
//
// Returns true if there are more groups to iterate over in the snapshot.
//
// Usage Pattern:
//
//	for hasNext, err := iter.HasNext(); hasNext; hasNext, err = iter.HasNext() {
//	    tuple, err := iter.Next()
//	    // process tuple
//	}
//
// Returns:
//   - bool: true if more tuples are available, false otherwise
//   - error: If the iterator is not opened
func (i *AggregatorIterator) HasNext() (bool, error) {
	if !i.opened {
		return false, fmt.Errorf("iterator not opened - call Open() first")
	}

	return i.currentIdx < len(i.groups), nil
}

// readNext reads the next result tuple from the aggregator.
//
// This is an internal method that:
//  1. Checks if more groups are available in the snapshot
//  2. Fetches the aggregate value from the aggregator (with locking)
//  3. Constructs the appropriate result tuple format
//
// Tuple formats:
//   - Non-grouped: [aggregateValue]
//   - Grouped:     [groupKey, aggregateValue]
//
// Returns:
//   - *tuple.Tuple: The next result tuple, or nil if exhausted
//   - error: If fetching aggregate value or tuple construction fails
func (i *AggregatorIterator) readNext() (*tuple.Tuple, error) {
	if i.currentIdx >= len(i.groups) {
		return nil, nil
	}

	groupKey := i.groups[i.currentIdx]
	i.currentIdx++

	i.aggregator.RLock()
	aggValue, err := i.aggregator.GetAggregateValue(groupKey)
	isGrouped := i.aggregator.GetGroupingField() != NoGrouping
	i.aggregator.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("failed to get aggregate value for group '%s': %w", groupKey, err)
	}

	resultTuple := tuple.NewTuple(i.tupleDesc)

	if !isGrouped {
		if err := resultTuple.SetField(0, aggValue); err != nil {
			return nil, fmt.Errorf("failed to set aggregate field: %w", err)
		}
	} else {
		groupField := types.NewStringField(groupKey, len(groupKey))

		if err := resultTuple.SetField(0, groupField); err != nil {
			return nil, fmt.Errorf("failed to set group field: %w", err)
		}
		if err := resultTuple.SetField(1, aggValue); err != nil {
			return nil, fmt.Errorf("failed to set aggregate field: %w", err)
		}
	}

	return resultTuple, nil
}

// Next returns the next tuple from the aggregation results.
//
// Returns:
//   - *tuple.Tuple: The next result tuple
//   - error: If iterator not opened, or no more tuples available
func (i *AggregatorIterator) Next() (*tuple.Tuple, error) {
	if !i.opened {
		return nil, fmt.Errorf("iterator not opened - call Open() first")
	}

	if i.currentIdx >= len(i.groups) {
		return nil, fmt.Errorf("no more tuples available")
	}

	return i.readNext()
}

// Rewind resets the iterator to the beginning, allowing re-iteration.
//
// Important Notes:
//   - Does NOT refresh the snapshot - reuses the groups from Open()
//   - To get a fresh snapshot, call Close() followed by Open()
//
// Use Case:
//   - When you need to iterate over the same result set multiple times
//   - More efficient than Close()/Open() if the snapshot doesn't need updating
//
// Returns:
//   - error: If the iterator is not opened
func (i *AggregatorIterator) Rewind() error {
	if !i.opened {
		return fmt.Errorf("iterator not opened - call Open() first")
	}

	i.currentIdx = 0
	return nil
}

// Close releases resources held by the iterator.
//
// This method:
//   - Marks the iterator as closed
//   - Releases the snapshot of groups (helps GC for large result sets)
//   - Does NOT release the aggregator reference (it's not owned by the iterator)
//
// After closing, the iterator can be reopened with Open() to get a fresh snapshot.
// Multiple calls to Close() are safe (idempotent).
//
// Returns:
//   - error: Always returns nil (kept for interface compatibility)
func (i *AggregatorIterator) Close() error {
	if !i.opened {
		return nil
	}

	i.opened = false
	i.groups = nil

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
	return i.tupleDesc
}
