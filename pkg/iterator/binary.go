package iterator

import (
	"errors"
	"fmt"
	"storemy/pkg/tuple"
)

// BinaryOperator provides a base implementation for operators with two children.
// It combines BaseIterator's caching logic with dual-child operator management,
// eliminating boilerplate code in Join, Union, Intersect, and similar operators.
//
// BinaryOperator handles:
// - Opening/closing both left and right child operators
// - Delegating HasNext/Next to BaseIterator
// - Providing FetchLeft/FetchRight helpers for reading from children
// - Managing rewind operations for both children
//
// Operators that embed BinaryOperator only need to implement their specific
// readNext logic - all lifecycle management is handled automatically.
type BinaryOperator struct {
	base       *BaseIterator
	leftChild  DbIterator
	rightChild DbIterator
}

// NewBinaryOperator creates a new binary operator base with the given children and read function.
// The readNextFunc should implement the operator's specific logic for combining/processing
// tuples from both children.
func NewBinaryOperator(leftChild, rightChild DbIterator, readNextFunc ReadNextFunc) (*BinaryOperator, error) {
	if leftChild == nil {
		return nil, fmt.Errorf("left child operator cannot be nil")
	}
	if rightChild == nil {
		return nil, fmt.Errorf("right child operator cannot be nil")
	}

	b := &BinaryOperator{
		leftChild:  leftChild,
		rightChild: rightChild,
	}
	b.base = NewBaseIterator(readNextFunc)
	return b, nil
}

// FetchLeft retrieves the next tuple from the left child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
func (b *BinaryOperator) FetchLeft() (*tuple.Tuple, error) {
	hasNext, err := b.leftChild.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if left child has next: %w", err)
	}

	if !hasNext {
		return nil, nil
	}

	leftTuple, err := b.leftChild.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from left child: %w", err)
	}

	return leftTuple, nil
}

// FetchRight retrieves the next tuple from the right child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
func (b *BinaryOperator) FetchRight() (*tuple.Tuple, error) {
	hasNext, err := b.rightChild.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if right child has next: %w", err)
	}

	if !hasNext {
		return nil, nil
	}

	rightTuple, err := b.rightChild.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from right child: %w", err)
	}

	return rightTuple, nil
}

// Open opens both child operators and marks this operator as ready.
func (b *BinaryOperator) Open() error {
	if err := b.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := b.rightChild.Open(); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	b.base.MarkOpened()
	return nil
}

// Close closes both child operators and releases resources.
// Uses errors.Join to collect errors from both children if both fail.
func (b *BinaryOperator) Close() error {
	var errs []error

	if b.leftChild != nil {
		if err := b.leftChild.Close(); err != nil {
			errs = append(errs, fmt.Errorf("left child close: %w", err))
		}
	}

	if b.rightChild != nil {
		if err := b.rightChild.Close(); err != nil {
			errs = append(errs, fmt.Errorf("right child close: %w", err))
		}
	}

	if err := b.base.Close(); err != nil {
		errs = append(errs, fmt.Errorf("base iterator close: %w", err))
	}

	return errors.Join(errs...)
}

// Rewind resets both child operators and the base iterator cache.
func (b *BinaryOperator) Rewind() error {
	if err := b.leftChild.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind left child: %w", err)
	}

	if err := b.rightChild.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind right child: %w", err)
	}

	return b.base.Rewind()
}

// HasNext checks if there are more tuples available.
func (b *BinaryOperator) HasNext() (bool, error) {
	return b.base.HasNext()
}

// Next returns the next tuple from the operator.
func (b *BinaryOperator) Next() (*tuple.Tuple, error) {
	return b.base.Next()
}

// GetLeftChild returns the left child operator (useful for inspection/testing).
func (b *BinaryOperator) GetLeftChild() DbIterator {
	return b.leftChild
}

// GetRightChild returns the right child operator (useful for inspection/testing).
func (b *BinaryOperator) GetRightChild() DbIterator {
	return b.rightChild
}
