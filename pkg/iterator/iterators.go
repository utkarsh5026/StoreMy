package iterator

import (
	"errors"
	"fmt"
	"storemy/pkg/tuple"
)

// BinaryOperator provides a base implementation for operators with two children.
// It combines BaseIterator's caching logic with dual-child operator management,
// eliminating boilerplate code in Join, Union, Intersect, and similar operators.
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
	t, err := b.fetchChild(b.leftChild)
	if err != nil {
		return nil, fmt.Errorf("error fetching left child %w", err)
	}
	return t, nil
}

// FetchRight retrieves the next tuple from the right child operator.
// Returns the tuple if available, nil if no more tuples, or error.
// Handles all the HasNext/Next ceremony internally.
func (b *BinaryOperator) FetchRight() (*tuple.Tuple, error) {
	t, err := b.fetchChild(b.rightChild)
	if err != nil {
		return nil, fmt.Errorf("error fetching right child tuple: %w", err)
	}
	return t, nil
}

// fetchChild retrieves the next tuple from a child iterator.
// It first checks if the child iterator has a next tuple available using HasNext().
// If a tuple is available, it fetches and returns it using Next().
// Returns nil if the child iterator has no more tuples.
// Returns an error if either HasNext() or Next() operations fail.
func (b *BinaryOperator) fetchChild(child DbIterator) (*tuple.Tuple, error) {
	hasNext, err := child.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if child has next: %w", err)
	}

	if !hasNext {
		return nil, nil
	}

	t, err := child.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from child: %w", err)
	}

	return t, nil
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

	if err := b.leftChild.Close(); err != nil {
		errs = append(errs, fmt.Errorf("left child close: %w", err))
	}

	if err := b.rightChild.Close(); err != nil {
		errs = append(errs, fmt.Errorf("right child close: %w", err))
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

// UnaryOperator provides a base implementation for operators with a single child.
// It combines BaseIterator's caching logic with child operator management,
// eliminating boilerplate code in Filter, Project, Limit, and similar operators.
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

// SliceIterator provides a generic iterator over a slice of any type T.
// This encapsulates the common pattern of iterating through materialized data
// stored in a slice, eliminating duplicate slice+index logic across operators.
type SliceIterator[T any] struct {
	data         []T // The underlying slice to iterate over
	currentIndex int // Current position in the slice
}

// NewSliceIterator creates a new iterator over the given slice.
// The iterator is immediately ready to use - no lifecycle management needed.
func NewSliceIterator[T any](data []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data:         data,
		currentIndex: 0,
	}
}

// HasNext checks if there are more elements available.
// Returns true if there is at least one more element to consume.
func (it *SliceIterator[T]) HasNext() bool {
	return it.currentIndex < len(it.data)
}

// Next returns the next element from the slice and advances the position.
// Returns an error if there are no more elements.
func (it *SliceIterator[T]) Next() (T, error) {
	var zero T

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	element := it.data[it.currentIndex]
	it.currentIndex++
	return element, nil
}

// Peek returns the next element without advancing the position.
// Returns an error if there are no more elements.
func (it *SliceIterator[T]) Peek() (T, error) {
	var zero T

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	return it.data[it.currentIndex], nil
}

// Rewind resets the iterator position to the beginning of the slice.
// Does not modify the underlying data, just resets the read position.
// Useful when you need to iterate over the same data multiple times.
func (it *SliceIterator[T]) Rewind() error {
	it.currentIndex = 0
	return nil
}

// Len returns the total number of elements in the slice.
func (it *SliceIterator[T]) Len() int {
	return len(it.data)
}

// Remaining returns the number of elements left to iterate.
func (it *SliceIterator[T]) Remaining() int {
	if it.currentIndex >= len(it.data) {
		return 0
	}
	return len(it.data) - it.currentIndex
}

// CurrentIndex returns the current position in the slice (0-based).
// This is the index of the next element that will be returned by Next().
func (it *SliceIterator[T]) CurrentIndex() int {
	return it.currentIndex
}

// GetData returns the underlying slice.
// Note: This provides direct access to the internal data structure.
func (it *SliceIterator[T]) GetData() []T {
	return it.data
}
