package abstract

import "StoreMy/src/pkg/fields"

// Aggregator is an interface for merging tuples into an aggregate and creating an iterator over group aggregate results.
type Aggregator interface {
	// Merge merges a new tuple into the aggregate for a distinct group value;
	// creates a new group aggregate result if the group value has not yet been encountered.
	//
	// Parameters:
	//   value: The Tuple containing an aggregate field and a group-by field.
	//
	// Returns:
	//   error: An error if the merge operation fails.
	Merge(value fields.Tuple) error

	// Iterator creates a DatabaseIterator over group aggregate results.
	//
	// Returns:
	//   DatabaseIterator: An iterator over the group aggregate results.
	Iterator() DatabaseIterator
}
