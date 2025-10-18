package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// SetOperationType defines the type of set operation
type SetOperationType int

const (
	SetUnion SetOperationType = iota
	SetIntersect
	SetExcept
)

// SetOp provides common functionality for UNION, INTERSECT, and EXCEPT operators.
type SetOp struct {
	base        *BaseIterator
	leftChild   *sourceOperator
	rightChild  *sourceOperator
	opType      SetOperationType
	preserveAll bool // true for ALL variants (UNION ALL, INTERSECT ALL, etc.)

	// Shared state for different operations
	rightHashes map[uint32]int  // Hash -> count (used by all operations)
	leftSeen    map[uint32]bool // For deduplication (UNION, EXCEPT)
	leftDone    bool            // Track if left child exhausted (UNION)
	initialized bool            // Track if right hash set is built

	readNextFunc func() (*tuple.Tuple, error)
}

// NewSetOperationBase creates a new base for set operations with common validation.
func NewSetOperationBase(left, right iterator.DbIterator, opType SetOperationType, preserveAll bool) (*SetOp, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("set operation children cannot be nil")
	}

	leftOp, err := NewSourceOperator(left)
	if err != nil {
		return nil, fmt.Errorf("failed to create left source: %w", err)
	}

	rightOp, err := NewSourceOperator(right)
	if err != nil {
		return nil, fmt.Errorf("failed to create right source: %w", err)
	}

	// Validate schema compatibility
	if err := validateSchemaCompatibility(leftOp.GetTupleDesc(), rightOp.GetTupleDesc()); err != nil {
		return nil, err
	}

	return &SetOp{
		leftChild:   leftOp,
		rightChild:  rightOp,
		opType:      opType,
		preserveAll: preserveAll,
		rightHashes: make(map[uint32]int),
		initialized: false,
	}, nil
}

func validateSchemaCompatibility(left, right *tuple.TupleDescription) error {
	if left.NumFields() != right.NumFields() {
		return fmt.Errorf("schema mismatch: left has %d fields, right has %d fields",
			left.NumFields(), right.NumFields())
	}

	for i := 0; i < left.NumFields(); i++ {
		leftType, _ := left.TypeAtIndex(i)
		rightType, _ := right.TypeAtIndex(i)
		if leftType != rightType {
			return fmt.Errorf("schema mismatch at field %d: left type %v, right type %v",
				i, leftType, rightType)
		}
	}

	return nil
}

// GetTupleDesc returns the schema of the result.
func (s *SetOp) GetTupleDesc() *tuple.TupleDescription {
	return s.leftChild.GetTupleDesc()
}

// HasNext checks if there are more tuples available.
func (s *SetOp) HasNext() (bool, error) {
	return s.base.HasNext()
}

// Next retrieves the next tuple.
func (s *SetOp) Next() (*tuple.Tuple, error) {
	return s.base.Next()
}

// Rewind resets the operator to the beginning.
func (s *SetOp) Rewind() error {
	if err := s.leftChild.Rewind(); err != nil {
		return err
	}
	if err := s.rightChild.Rewind(); err != nil {
		return err
	}

	s.initialized = false
	s.leftDone = false
	s.rightHashes = make(map[uint32]int)

	if !s.preserveAll && s.opType != SetIntersect {
		s.leftSeen = make(map[uint32]bool)
	}

	s.base.ClearCache()
	return nil
}

// Close releases resources by closing both child operators.
func (s *SetOp) Close() error {
	leftErr := s.leftChild.Close()
	rightErr := s.rightChild.Close()

	if leftErr != nil {
		return leftErr
	}
	if rightErr != nil {
		return rightErr
	}

	return s.base.Close()
}

// Open initializes the set operation by opening both child operators.
func (s *SetOp) Open() error {
	if err := s.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := s.rightChild.Open(); err != nil {
		s.leftChild.Close()
		return fmt.Errorf("failed to open right child: %w", err)
	}

	s.initialized = false
	s.leftDone = false
	s.rightHashes = make(map[uint32]int)

	if !s.preserveAll && s.opType != SetIntersect {
		s.leftSeen = make(map[uint32]bool)
	}

	s.base.MarkOpened()
	return nil
}
