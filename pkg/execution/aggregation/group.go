package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// GroupAggregator defines the interface for aggregation operations that can optionally group results by a field.
// This interface provides a generic way to iterate over aggregated data, whether it's a single aggregate
// (like COUNT(*)) or grouped aggregates (like COUNT(*) GROUP BY field).
type GroupAggregator interface {
	// GetGroups returns all group keys for iteration over aggregated results.
	// For grouped aggregations, this returns the string representation of each group's key value.
	//
	// Returns:
	//   - []string: slice of group keys, empty slice if no results
	GetGroups() []string

	// GetAggregateValue returns the computed aggregate value for the specified group.
	// For non-grouped aggregations, groupKey should be an empty string.
	// For grouped aggregations, groupKey should match one of the keys returned by GetGroups().
	//
	// Parameters:
	//   - groupKey: the group identifier (empty string for non-grouped aggregations)
	//
	// Returns:
	//   - types.Field: the aggregated value for the group
	//   - error: error if groupKey is invalid or computation failed
	GetAggregateValue(groupKey string) (types.Field, error)

	// GetTupleDesc returns the tuple description that describes the structure of result tuples.
	// For non-grouped aggregations, this contains only the aggregate field.
	// For grouped aggregations, this contains the grouping field followed by the aggregate field.
	//
	// Returns:
	//   - *tuple.TupleDescription: description of result tuple structure
	GetTupleDesc() *tuple.TupleDescription

	// GetGroupingField returns the index of the field used for grouping.
	//
	// Returns:
	//   - int: field index for grouping, or -1 if this is a non-grouped aggregation
	GetGroupingField() int

	// RLock acquires a read lock for thread-safe access during iteration.
	RLock()

	// RUnlock releases the read lock acquired by RLock().
	// Must be called after finishing access to aggregation results.
	RUnlock()
}
