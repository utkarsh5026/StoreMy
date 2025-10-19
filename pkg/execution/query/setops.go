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

	// Set abstraction for managing tuples
	tracker     *TupleSetTracker
	leftDone    bool // Track if left child exhausted (UNION)
	initialized bool // Track if right hash set is built
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
		tracker:     NewTupleSetTracker(preserveAll),
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

func (s *SetOp) buildRightHashSet() error {
	if s.initialized {
		return nil
	}

	for {
		t, err := s.rightChild.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}

		s.tracker.AddToRight(t)
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

type Union struct {
	*SetOp
}

func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	base, err := NewSetOperationBase(left, right, SetUnion, unionAll)
	if err != nil {
		return nil, err
	}

	u := &Union{SetOp: base}
	u.base = NewBaseIterator(u.readNext)
	return u, nil
}

// readNext implements the union logic.
func (u *Union) readNext() (*tuple.Tuple, error) {
	for !u.leftDone {
		t, err := u.leftChild.FetchNext()
		if err != nil {
			return nil, err
		}

		if t == nil {
			u.leftDone = true
			break
		}

		if !u.preserveAll && !u.tracker.MarkSeen(t) {
			continue // Already seen, skip it
		}
		return t, nil
	}

	// Then, process tuples from the right child
	for {
		t, err := u.rightChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if !u.preserveAll {
			// For UNION (not UNION ALL), check if we've already seen this tuple
			if !u.tracker.MarkSeen(t) {
				continue // Already seen, skip it
			}
		}

		return t, nil
	}
}

// Intersect represents an INTERSECT operator that returns only tuples that appear in both inputs.
type Intersect struct {
	*SetOp
}

// NewIntersect creates a new Intersect operator.
func NewIntersect(left, right iterator.DbIterator, intersectAll bool) (*Intersect, error) {
	base, err := NewSetOperationBase(left, right, SetIntersect, intersectAll)
	if err != nil {
		return nil, err
	}

	in := &Intersect{SetOp: base}
	in.base = NewBaseIterator(in.readNext)
	return in, nil
}

// readNext implements the intersect logic.
func (in *Intersect) readNext() (*tuple.Tuple, error) {
	if err := in.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := in.leftChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		// Check if tuple exists in right set
		if !in.tracker.ContainsInRight(t) {
			continue // Not in right set, skip
		}

		if in.preserveAll {
			// For INTERSECT ALL, decrement count and check if we can still return it
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.DecrementRight(t)
		} else {
			// For INTERSECT, mark as used to avoid duplicates
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.rightSet.Remove(t)
		}

		return t, nil
	}
}

// Except represents an EXCEPT operator that returns tuples from left not in right.
type Except struct {
	*SetOp
}

// NewExcept creates a new Except operator.
func NewExcept(left, right iterator.DbIterator, exceptAll bool) (*Except, error) {
	base, err := NewSetOperationBase(left, right, SetExcept, exceptAll)
	if err != nil {
		return nil, err
	}

	ex := &Except{SetOp: base}
	ex.base = NewBaseIterator(ex.readNext)
	return ex, nil
}

// readNext implements the except logic.
func (ex *Except) readNext() (*tuple.Tuple, error) {
	if err := ex.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := ex.leftChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if ex.preserveAll {
			count := ex.tracker.GetRightCount(t)
			if count > 0 {
				ex.tracker.DecrementRight(t)
				continue
			}
			return t, nil
		} else {
			existsInRight := ex.tracker.ContainsInRight(t)
			alreadyOutput := ex.tracker.WasSeen(t)

			if existsInRight || alreadyOutput {
				continue
			}

			ex.tracker.MarkSeen(t)
			return t, nil
		}
	}
}
