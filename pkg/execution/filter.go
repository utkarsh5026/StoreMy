package execution

import (
	"fmt"
	"storemy/pkg/tuple"
)

// Filter represents a filtering operator that applies a predicate to each tuple
// from its child operator, only returning tuples that satisfy the predicate condition.
// It implements the DbIterator interface and acts as a pipeline stage in query execution.
type Filter struct {
	base      *BaseIterator
	predicate *Predicate
	child     DbIterator
}

// NewFilter creates a new Filter operator with the specified predicate and child operator.
// The filter will evaluate the predicate against each tuple from the child operator,
// only passing through tuples that satisfy the predicate condition.
//
// Parameters:
//   - predicate: The condition to evaluate against each tuple (cannot be nil)
//   - child: The child operator that provides input tuples (cannot be nil)
//
// Returns:
//   - *Filter: A new Filter instance configured with the given predicate and child
//   - error: An error if either predicate or child is nil
func NewFilter(predicate *Predicate, child DbIterator) (*Filter, error) {
	if predicate == nil {
		return nil, fmt.Errorf("predicate cannot be nil")
	}
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	f := &Filter{
		predicate: predicate,
		child:     child,
	}

	f.base = NewBaseIterator(f.readNext)
	return f, nil
}

// Open initializes the Filter operator for execution by opening its child operator
// and marking itself as opened. This method must be called before any iteration operations.
// The data flow is established from child to this filter operator.
//
// Returns:
//   - error: An error if the child operator fails to open, nil otherwise
func (f *Filter) Open() error {
	// Open our child first - data flows from child to us
	if err := f.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	f.base.MarkOpened()
	return nil
}

// Close releases resources held by the Filter operator and its child operator.
// This method should be called when the filter is no longer needed to ensure
// proper cleanup of resources.
//
// Returns:
//   - error: Always returns nil as cleanup operations don't typically fail
func (f *Filter) Close() error {
	if f.child != nil {
		f.child.Close()
	}
	return f.base.Close()
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this filter.
// Since filtering operations don't modify the structure of tuples, this returns
// the same tuple description as the child operator.
//
// Returns:
//   - *tuple.TupleDescription: The schema description from the child operator
func (f *Filter) GetTupleDesc() *tuple.TupleDescription {
	return f.child.GetTupleDesc()
}

// HasNext checks if there are more filtered tuples available for iteration.
// This method delegates to the base iterator which handles the caching logic.
//
// Returns:
//   - bool: True if more tuples are available, false otherwise
//   - error: An error if the check operation fails
func (f *Filter) HasNext() (bool, error) { return f.base.HasNext() }

// Next returns the next filtered tuple that satisfies the predicate condition.
// This method delegates to the base iterator which calls readNext internally.
// Should only be called after HasNext() returns true.
//
// Returns:
//   - *tuple.Tuple: The next tuple that passes the filter condition
//   - error: An error if the iteration fails or no more tuples are available
func (f *Filter) Next() (*tuple.Tuple, error) { return f.base.Next() }

// readNext is the core filtering logic that reads tuples from the child operator
// and applies the predicate to determine which tuples to return. This method
// continues reading until it finds a tuple that satisfies the predicate or
// until no more tuples are available from the child.
//
// Returns:
//   - *tuple.Tuple: The next tuple that satisfies the predicate, or nil if no more tuples
//   - error: An error if any operation during filtering fails
func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		hasNext, err := f.child.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error checking if child has next: %v", err)
		}

		if !hasNext {
			return nil, nil // This signals "no more tuples"
		}

		t, err := f.child.Next()
		if err != nil {
			return nil, fmt.Errorf("error getting next tuple from child: %v", err)
		}

		if t == nil {
			return nil, nil
		}

		passes, err := f.predicate.Filter(t)
		if err != nil {
			return nil, fmt.Errorf("predicate evaluation failed: %v", err)
		}

		if passes {
			return t, nil
		}
	}
}

// Rewind resets the Filter operator to its initial state, allowing iteration
// to begin again from the first tuple. This involves rewinding the child operator
// and clearing any cached state in the base iterator.
//
// Returns:
//   - error: An error if the child operator fails to rewind, nil otherwise
func (f *Filter) Rewind() error {
	// Rewind our child
	if err := f.child.Rewind(); err != nil {
		return err
	}

	// Clear our cache so we start fresh
	f.base.ClearCache()
	return nil
}
