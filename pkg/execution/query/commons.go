package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// SourceIter encapsulates common child operator management logic
// for unary operators (operators with a single child).
type SourceIter struct {
	child iterator.DbIterator
}

// NewSourceOperator creates a new source operator wrapper with validation.
func NewSourceOperator(child iterator.DbIterator) (*SourceIter, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}
	return &SourceIter{child: child}, nil
}

// FetchNext retrieves the next tuple from the child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
func (c *SourceIter) FetchNext() (*tuple.Tuple, error) {
	hasNext, err := c.child.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if child has next: %w", err)
	}

	if !hasNext {
		return nil, nil
	}

	childTuple, err := c.child.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from child: %w", err)
	}

	return childTuple, nil
}

// Open opens the child operator.
func (c *SourceIter) Open() error {
	if err := c.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}
	return nil
}

// Close closes the child operator.
func (c *SourceIter) Close() error {
	if c.child != nil {
		return c.child.Close()
	}
	return nil
}

// Rewind rewinds the child operator.
func (c *SourceIter) Rewind() error {
	if err := c.child.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind child operator: %w", err)
	}
	return nil
}

// GetTupleDesc returns the child's tuple description.
func (c *SourceIter) GetTupleDesc() *tuple.TupleDescription {
	return c.child.GetTupleDesc()
}
