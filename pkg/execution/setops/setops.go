package setops

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
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
	*iterator.BinaryOperator
	opType      SetOperationType
	preserveAll bool // true for ALL variants (UNION ALL, INTERSECT ALL, etc.)

	tracker     *TupleSetTracker
	leftDone    bool // Track if left child exhausted (UNION)
	initialized bool // Track if right hash set is built
}

// NewSetOperationBase creates a new base for set operations with common validation.
// Note: The caller must set the BinaryOperator field after creating the SetOp.
func NewSetOperationBase(left, right iterator.DbIterator, opType SetOperationType, preserveAll bool) (*SetOp, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("set operation children cannot be nil")
	}

	if err := validateSchemaCompatibility(left.GetTupleDesc(), right.GetTupleDesc()); err != nil {
		return nil, err
	}

	return &SetOp{
		opType:      opType,
		preserveAll: preserveAll,
		tracker:     NewTupleSetTracker(preserveAll),
		initialized: false,
	}, nil
}

func validateSchemaCompatibility(l, r *tuple.TupleDescription) error {
	if l.NumFields() != r.NumFields() {
		return fmt.Errorf("schema mismatch: left has %d fields, right has %d fields",
			l.NumFields(), r.NumFields())
	}

	var i primitives.ColumnID
	for i = 0; i < l.NumFields(); i++ {
		lt, _ := l.TypeAtIndex(i)
		rt, _ := r.TypeAtIndex(i)
		if lt != rt {
			return fmt.Errorf("schema mismatch at field %d: left type %v, right type %v",
				i, lt, rt)
		}
	}

	return nil
}

func (s *SetOp) buildRightHashSet() error {
	if s.initialized {
		return nil
	}

	for {
		rt, err := s.FetchRight()
		if err != nil {
			return err
		}
		if rt == nil {
			break
		}

		s.tracker.AddToRight(rt)
	}

	s.initialized = true
	return nil
}

// GetTupleDesc returns the schema of the result.
func (s *SetOp) GetTupleDesc() *tuple.TupleDescription {
	return s.GetLeftChild().GetTupleDesc()
}

// Rewind resets the operator to the beginning.
func (s *SetOp) Rewind() error {
	s.initialized = false
	s.leftDone = false
	s.tracker.Clear()

	return s.BinaryOperator.Rewind()
}

// Close releases resources by closing both child operators.
func (s *SetOp) Close() error {
	return s.BinaryOperator.Close()
}

// Open initializes the set operation by opening both child operators.
func (s *SetOp) Open() error {
	if err := s.BinaryOperator.Open(); err != nil {
		return err
	}

	s.initialized = false
	s.leftDone = false
	s.tracker = NewTupleSetTracker(s.preserveAll)

	return nil
}

func (s *SetOp) setBinaryOperator(l, r iterator.DbIterator, readNext iterator.ReadNextFunc) error {
	binaryOp, err := iterator.NewBinaryOperator(l, r, readNext)
	if err != nil {
		return err
	}
	s.BinaryOperator = binaryOp
	return nil
}
