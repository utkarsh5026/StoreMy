package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Filter represents a filtering operator that applies a predicate to each tuple
// from its source operator, only returning tuples that satisfy the predicate condition.
type Filter struct {
	base      *BaseIterator
	predicate *Predicate
	source    *sourceOperator
}

// NewFilter creates a new Filter operator with the specified predicate and source iterator.
// The Filter will evaluate the predicate against each tuple from the source operator,
// passing through only those tuples that satisfy the condition.
func NewFilter(predicate *Predicate, source iterator.DbIterator) (*Filter, error) {
	if predicate == nil {
		return nil, fmt.Errorf("predicate cannot be nil")
	}

	childOp, err := NewSourceOperator(source)
	if err != nil {
		return nil, err
	}

	f := &Filter{
		predicate: predicate,
		source:    childOp,
	}

	f.base = NewBaseIterator(f.readNext)
	return f, nil
}

// readNext is the internal method that implements the filtering logic.
// It continuously reads tuples from the source operator and evaluates them
// against the predicate until it finds a tuple that satisfies the condition
// or reaches the end of the input stream.
func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		t, err := f.source.FetchNext()
		if err != nil || t == nil {
			return t, err
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

// Open initializes the Filter operator for iteration by opening its source operator.
func (f *Filter) Open() error {
	if err := f.source.Open(); err != nil {
		return err
	}

	f.base.MarkOpened()
	return nil
}

// Close releases resources associated with the Filter operator by closing its source
// operator and performing cleanup.
func (f *Filter) Close() error {
	if err := f.source.Close(); err != nil {
		return err
	}
	return f.base.Close()
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this operator.
// Since Filter doesn't modify the structure of tuples, it returns the same schema
// as its source operator.
func (f *Filter) GetTupleDesc() *tuple.TupleDescription {
	return f.source.GetTupleDesc()
}

// HasNext checks if there are more tuples available that satisfy the filter predicate.
func (f *Filter) HasNext() (bool, error) { return f.base.HasNext() }

// Next retrieves the next tuple that satisfies the filter predicate.
func (f *Filter) Next() (*tuple.Tuple, error) { return f.base.Next() }

// Rewind resets the Filter operator to the beginning of its result set.
func (f *Filter) Rewind() error {
	if err := f.source.Rewind(); err != nil {
		return err
	}

	f.base.ClearCache()
	return nil
}
