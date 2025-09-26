package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Filter represents a filtering operator that applies a predicate to each tuple
// from its child operator, only returning tuples that satisfy the predicate condition.
// It implements the DbIterator interface and acts as a pipeline stage in query execution.
type Filter struct {
	base      *BaseIterator
	predicate *Predicate
	child     iterator.DbIterator
}

// NewFilter creates a new Filter operator with the specified predicate and child iterator.
// The Filter will evaluate the predicate against each tuple from the child operator,
// passing through only those tuples that satisfy the condition.
func NewFilter(predicate *Predicate, child iterator.DbIterator) (*Filter, error) {
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

// readNext is the internal method that implements the filtering logic.
// It continuously reads tuples from the child operator and evaluates them
// against the predicate until it finds a tuple that satisfies the condition
// or reaches the end of the input stream.
func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		hasNext, err := f.child.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error checking if child has next: %v", err)
		}

		if !hasNext {
			return nil, nil
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

// Open initializes the Filter operator for iteration by opening its child operator.
//
// Returns an error if the child operator fails to open.
func (f *Filter) Open() error {
	if err := f.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	f.base.MarkOpened()
	return nil
}

// Close releases resources associated with the Filter operator by closing its child
// operator and performing cleanup.
func (f *Filter) Close() error {
	if f.child != nil {
		f.child.Close()
	}
	return f.base.Close()
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this operator.
// Since Filter doesn't modify the structure of tuples, it returns the same schema
// as its child operator.
func (f *Filter) GetTupleDesc() *tuple.TupleDescription {
	return f.child.GetTupleDesc()
}

// HasNext checks if there are more tuples available that satisfy the filter predicate.
func (f *Filter) HasNext() (bool, error) { return f.base.HasNext() }

// Next retrieves the next tuple that satisfies the filter predicate.
func (f *Filter) Next() (*tuple.Tuple, error) { return f.base.Next() }

// Rewind resets the Filter operator to the beginning of its result set.
// This allows the operator to be re-executed from the start, which is useful
// for operations that need to scan the filtered results multiple times.
func (f *Filter) Rewind() error {
	if err := f.child.Rewind(); err != nil {
		return err
	}

	f.base.ClearCache()
	return nil
}
