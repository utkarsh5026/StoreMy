package aggregation

import "storemy/pkg/types"

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
