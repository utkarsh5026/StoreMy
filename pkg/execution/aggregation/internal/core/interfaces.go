package core

import (
	"storemy/pkg/primitives"
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
	GetGroupingField() primitives.ColumnID

	// RLock acquires a read lock for thread-safe access during iteration.
	RLock()

	// RUnlock releases the read lock acquired by RLock().
	// Must be called after finishing access to aggregation results.
	RUnlock()
}

// AggregateCalculator defines the interface for computing aggregate functions
// over groups of data. It provides methods to initialize groups, update
// aggregate values incrementally, and retrieve final results.
type AggregateCalculator interface {
	// InitializeGroup sets up initial state for a new group.
	// This method should be called once per group before any UpdateAggregate calls.
	//
	// Parameters:
	//   groupKey: A unique identifier for the group being aggregated
	InitializeGroup(groupKey string)

	// UpdateAggregate processes a new value for the aggregate computation.
	// This method is called for each data point that belongs to the group.
	//
	// Parameters:
	//   groupKey: The group identifier (must be previously initialized)
	//   fieldValue: The value to include in the aggregate calculation
	//
	// Returns:
	//   error: Non-nil if the value type is incompatible or groupKey is invalid
	UpdateAggregate(groupKey string, fieldValue types.Field) error

	// GetFinalValue returns the final aggregated value for a group.
	// This method should only be called after all values have been processed
	// via UpdateAggregate calls.
	//
	// Parameters:
	//   groupKey: The group identifier to get the result for
	//
	// Returns:
	//   types.Field: The computed aggregate value
	//   error: Non-nil if the groupKey is invalid or computation failed
	GetFinalValue(groupKey string) (types.Field, error)

	// ValidateOperation checks if the given aggregate operation is supported
	// by this calculator implementation.
	//
	// Parameters:
	//   op: The aggregate operation to validate (e.g., SUM, COUNT, AVG)
	//
	// Returns:
	//   error: Non-nil if the operation is not supported by this calculator
	ValidateOperation(op AggregateOp) error

	// GetResultType returns the data type of the result that will be produced
	// by the specified aggregate operation.
	//
	// Parameters:
	//   op: The aggregate operation to get the result type for
	//
	// Returns:
	//   types.Type: The data type of the aggregate result
	GetResultType(op AggregateOp) types.Type
}
