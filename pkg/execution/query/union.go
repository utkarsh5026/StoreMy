package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Union represents a UNION operator that combines tuples from two input sources.
// UNION removes duplicate tuples (set semantics), while UNION ALL preserves all tuples.
type Union struct {
	base       *BaseIterator
	lch, rch   *sourceOperator // left and right child
	unionAll   bool            // true for UNION ALL, false for UNION (with deduplication)
	seenHashes map[uint32]bool // For deduplication in UNION (not UNION ALL)
	leftDone   bool            // Track if left child is exhausted
}

// NewUnion creates a new Union operator.
// If unionAll is true, all tuples are returned (UNION ALL).
// If unionAll is false, duplicate tuples are removed (UNION).
func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("union children cannot be nil")
	}

	leftOp, err := NewSourceOperator(left)
	if err != nil {
		return nil, fmt.Errorf("failed to create left source: %v", err)
	}

	rightOp, err := NewSourceOperator(right)
	if err != nil {
		return nil, fmt.Errorf("failed to create right source: %v", err)
	}

	ld := leftOp.GetTupleDesc()
	rd := rightOp.GetTupleDesc()

	if ld.NumFields() != rd.NumFields() {
		return nil, fmt.Errorf("union schema mismatch: left has %d fields, right has %d fields",
			ld.NumFields(), rd.NumFields())
	}

	for i := 0; i < ld.NumFields(); i++ {
		leftType, _ := ld.TypeAtIndex(i)
		rightType, _ := rd.TypeAtIndex(i)
		if leftType != rightType {
			return nil, fmt.Errorf("union schema mismatch at field %d: left type %v, right type %v",
				i, leftType, rightType)
		}
	}

	u := &Union{
		lch:      leftOp,
		rch:      rightOp,
		unionAll: unionAll,
		leftDone: false,
	}

	if !unionAll {
		u.seenHashes = make(map[uint32]bool)
	}

	u.base = NewBaseIterator(u.readNext)
	return u, nil
}

// readNext implements the union logic.
// First, it returns all tuples from the left child.
// Then, it returns all tuples from the right child.
// For UNION (not UNION ALL), it skips duplicates using a hash set.
func (u *Union) readNext() (*tuple.Tuple, error) {
	if !u.leftDone {
		t, err := u.lch.FetchNext()
		if err != nil {
			return nil, err
		}

		if t != nil {
			if !u.unionAll {
				hash := hashTuple(t)
				if u.seenHashes[hash] {
					return u.readNext()
				}
				u.seenHashes[hash] = true
			}
			return t, nil
		}
		u.leftDone = true
	}

	for {
		t, err := u.rch.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if !u.unionAll {
			hash := hashTuple(t)
			if u.seenHashes[hash] {
				continue
			}
			u.seenHashes[hash] = true
		}

		return t, nil
	}
}

// hashTuple computes a hash for a tuple for deduplication.
// This is a simple hash function that combines field hashes.
func hashTuple(t *tuple.Tuple) uint32 {
	var hash uint32 = 0
	for i := 0; i < t.TupleDesc.NumFields(); i++ {
		field, _ := t.GetField(i)
		fieldHash, _ := field.Hash()
		hash = hash*31 + fieldHash
	}
	return hash
}

// Open initializes the Union operator by opening both child operators.
func (u *Union) Open() error {
	if err := u.lch.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %v", err)
	}

	if err := u.rch.Open(); err != nil {
		u.lch.Close() // Clean up on error
		return fmt.Errorf("failed to open right child: %v", err)
	}

	u.leftDone = false
	if !u.unionAll {
		u.seenHashes = make(map[uint32]bool)
	}

	u.base.MarkOpened()
	return nil
}

// Close releases resources by closing both child operators.
func (u *Union) Close() error {
	leftErr := u.lch.Close()
	rightErr := u.rch.Close()

	if leftErr != nil {
		return leftErr
	}
	if rightErr != nil {
		return rightErr
	}

	return u.base.Close()
}

// GetTupleDesc returns the schema of the union result.
// Since both children must have compatible schemas, we return the left child's schema.
func (u *Union) GetTupleDesc() *tuple.TupleDescription {
	return u.lch.GetTupleDesc()
}

// HasNext checks if there are more tuples available from the union.
func (u *Union) HasNext() (bool, error) {
	return u.base.HasNext()
}

// Next retrieves the next tuple from the union.
func (u *Union) Next() (*tuple.Tuple, error) {
	return u.base.Next()
}

// Rewind resets the Union operator to the beginning.
func (u *Union) Rewind() error {
	if err := u.lch.Rewind(); err != nil {
		return err
	}
	if err := u.rch.Rewind(); err != nil {
		return err
	}

	u.leftDone = false
	if !u.unionAll {
		clear(u.seenHashes)
	}

	u.base.ClearCache()
	return nil
}
