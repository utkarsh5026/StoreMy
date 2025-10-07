package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// sourceOperator encapsulates common child operator management logic
// for unary operators (operators with a single child).
type sourceOperator struct {
	child iterator.DbIterator
}

// NewSourceOperator creates a new source operator wrapper with validation.
func NewSourceOperator(child iterator.DbIterator) (*sourceOperator, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}
	return &sourceOperator{child: child}, nil
}

// FetchNext retrieves the next tuple from the child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
func (c *sourceOperator) FetchNext() (*tuple.Tuple, error) {
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
func (c *sourceOperator) Open() error {
	if err := c.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}
	return nil
}

// Close closes the child operator.
func (c *sourceOperator) Close() error {
	if c.child != nil {
		return c.child.Close()
	}
	return nil
}

// Rewind rewinds the child operator.
func (c *sourceOperator) Rewind() error {
	if err := c.child.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind child operator: %w", err)
	}
	return nil
}

// GetTupleDesc returns the child's tuple description.
func (c *sourceOperator) GetTupleDesc() *tuple.TupleDescription {
	return c.child.GetTupleDesc()
}
