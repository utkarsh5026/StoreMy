package iterator

import (
	"fmt"
	"storemy/pkg/tuple"
)

// UnaryOperator provides a base implementation for operators with a single child.
// It combines BaseIterator's caching logic with child operator management,
// eliminating boilerplate code in Filter, Project, Limit, and similar operators.
//
// UnaryOperator handles:
// - Opening/closing the child operator
// - Delegating HasNext/Next to BaseIterator
// - Providing FetchNext helper for reading from child
// - Managing rewind operations
// - Forwarding tuple schema from child
//
// Operators that embed UnaryOperator only need to implement their specific
// readNext logic - all lifecycle management is handled automatically.
type UnaryOperator struct {
	base  *BaseIterator
	child DbIterator
}

// NewUnaryOperator creates a new unary operator base with the given child and read function.
// The readNextFunc should implement the operator's specific transformation logic.
func NewUnaryOperator(child DbIterator, readNextFunc ReadNextFunc) (*UnaryOperator, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	u := &UnaryOperator{
		child: child,
	}
	u.base = NewBaseIterator(readNextFunc)
	return u, nil
}

// FetchNext retrieves the next tuple from the child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
//
// This is a convenience method for operators to read from their child
// without manually managing the HasNext/Next pattern.
func (u *UnaryOperator) FetchNext() (*tuple.Tuple, error) {
	hasNext, err := u.child.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if child has next: %w", err)
	}

	if !hasNext {
		return nil, nil
	}

	childTuple, err := u.child.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from child: %w", err)
	}

	return childTuple, nil
}

// Open opens the child operator and marks this operator as ready.
func (u *UnaryOperator) Open() error {
	if err := u.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}
	u.base.MarkOpened()
	return nil
}

// Close closes the child operator and releases resources.
func (u *UnaryOperator) Close() error {
	if u.child != nil {
		if err := u.child.Close(); err != nil {
			return err
		}
	}
	return u.base.Close()
}

// Rewind resets both the child operator and the base iterator cache.
func (u *UnaryOperator) Rewind() error {
	if err := u.child.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind child operator: %w", err)
	}
	return u.base.Rewind()
}

// GetTupleDesc returns the child's tuple description.
// Operators that transform the schema should override this method.
func (u *UnaryOperator) GetTupleDesc() *tuple.TupleDescription {
	return u.child.GetTupleDesc()
}

// HasNext checks if there are more tuples available.
func (u *UnaryOperator) HasNext() (bool, error) {
	return u.base.HasNext()
}

// Next returns the next tuple from the operator.
func (u *UnaryOperator) Next() (*tuple.Tuple, error) {
	return u.base.Next()
}

// GetChild returns the child operator (useful for inspection/testing).
func (u *UnaryOperator) GetChild() DbIterator {
	return u.child
}
