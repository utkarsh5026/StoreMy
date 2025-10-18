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

func (s *SetOp) hashTuple(t *tuple.Tuple) uint32 {
	var hash uint32 = 0
	for i := 0; i < t.TupleDesc.NumFields(); i++ {
		field, _ := t.GetField(i)
		fieldHash, _ := field.Hash()
		hash = hash*31 + fieldHash
	}
	return hash
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

		hash := s.hashTuple(t)
		if s.preserveAll && s.opType != SetUnion {
			// For INTERSECT ALL and EXCEPT ALL, count occurrences
			s.rightHashes[hash]++
		} else {
			// For set semantics or UNION, just mark as present
			s.rightHashes[hash] = 1
		}
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

type Union struct {
	*SetOp
}

func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	base, err := NewSetOperationBase(left, right, SetUnion, unionAll)
	if err != nil {
		return nil, err
	}

	u := &Union{SetOp: base}

	if !unionAll {
		u.leftSeen = make(map[uint32]bool)
	}

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

		if !u.preserveAll {
			hash := u.hashTuple(t)
			if u.leftSeen[hash] {
				continue
			}
			u.leftSeen[hash] = true
		}
		return t, nil
	}

	for {
		t, err := u.rightChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if !u.preserveAll {
			hash := u.hashTuple(t)
			if u.leftSeen[hash] {
				continue
			}
			u.leftSeen[hash] = true
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

		hash := in.hashTuple(t)
		count, exists := in.rightHashes[hash]

		if !exists || count <= 0 {
			continue
		}

		if in.preserveAll {
			in.rightHashes[hash]--
		} else {
			// For INTERSECT, mark as used to avoid duplicates
			in.rightHashes[hash] = 0
		}

		return t, nil
	}
}
