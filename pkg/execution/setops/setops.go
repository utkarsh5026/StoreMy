package setops

import (
	"fmt"
	"storemy/pkg/execution/query"
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
	base        *query.BaseIterator
	leftChild   *query.SourceIter
	rightChild  *query.SourceIter
	opType      SetOperationType
	preserveAll bool // true for ALL variants (UNION ALL, INTERSECT ALL, etc.)

	tracker     *TupleSetTracker
	leftDone    bool // Track if left child exhausted (UNION)
	initialized bool // Track if right hash set is built
}

// NewSetOperationBase creates a new base for set operations with common validation.
func NewSetOperationBase(left, right iterator.DbIterator, opType SetOperationType, preserveAll bool) (*SetOp, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("set operation children cannot be nil")
	}

	leftOp, err := query.NewSourceOperator(left)
	if err != nil {
		return nil, fmt.Errorf("failed to create left source: %w", err)
	}

	rightOp, err := query.NewSourceOperator(right)
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
		rt, err := s.rightChild.FetchNext()
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
	s.tracker.Clear()

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
	s.tracker = NewTupleSetTracker(s.preserveAll)

	s.base.MarkOpened()
	return nil
}
