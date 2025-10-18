package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Intersect represents an INTERSECT operator that returns only tuples that appear
// in both input sources. INTERSECT removes duplicates (set semantics), while
// INTERSECT ALL preserves duplicates based on minimum occurrence count.
type Intersect struct {
	base         *BaseIterator
	leftChild    *sourceOperator
	rightChild   *sourceOperator
	intersectAll bool           // true for INTERSECT ALL, false for INTERSECT
	rightHashes  map[uint32]int // Hash -> count of tuples in right child
	initialized  bool
}

// NewIntersect creates a new Intersect operator.
// If intersectAll is true, duplicates are preserved based on minimum occurrence (INTERSECT ALL).
// If intersectAll is false, only distinct tuples that appear in both inputs are returned (INTERSECT).
func NewIntersect(left, right iterator.DbIterator, intersectAll bool) (*Intersect, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("intersect children cannot be nil")
	}

	leftOp, err := NewSourceOperator(left)
	if err != nil {
		return nil, fmt.Errorf("failed to create left source: %v", err)
	}

	rightOp, err := NewSourceOperator(right)
	if err != nil {
		return nil, fmt.Errorf("failed to create right source: %v", err)
	}

	leftDesc := leftOp.GetTupleDesc()
	rightDesc := rightOp.GetTupleDesc()

	if leftDesc.NumFields() != rightDesc.NumFields() {
		return nil, fmt.Errorf("intersect schema mismatch: left has %d fields, right has %d fields",
			leftDesc.NumFields(), rightDesc.NumFields())
	}

	for i := 0; i < leftDesc.NumFields(); i++ {
		leftType, _ := leftDesc.TypeAtIndex(i)
		rightType, _ := rightDesc.TypeAtIndex(i)
		if leftType != rightType {
			return nil, fmt.Errorf("intersect schema mismatch at field %d: left type %v, right type %v",
				i, leftType, rightType)
		}
	}

	in := &Intersect{
		leftChild:    leftOp,
		rightChild:   rightOp,
		intersectAll: intersectAll,
		rightHashes:  make(map[uint32]int),
		initialized:  false,
	}

	in.base = NewBaseIterator(in.readNext)
	return in, nil
}

// buildRightHashSet builds a hash set of all tuples from the right child.
// For INTERSECT ALL, it counts occurrences. For INTERSECT, it just tracks presence.
func (in *Intersect) buildRightHashSet() error {
	if in.initialized {
		return nil
	}

	// Read all tuples from right child and build hash map
	for {
		t, err := in.rightChild.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}

		hash := in.hashTuple(t)
		if in.intersectAll {
			// For INTERSECT ALL, count occurrences
			in.rightHashes[hash]++
		} else {
			// For INTERSECT, just mark as present (set to 1)
			in.rightHashes[hash] = 1
		}
	}

	in.initialized = true
	return nil
}

// readNext implements the intersect logic.
// It returns tuples from the left child that also appear in the right child.
func (in *Intersect) readNext() (*tuple.Tuple, error) {
	if !in.initialized {
		if err := in.buildRightHashSet(); err != nil {
			return nil, err
		}
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

		if in.intersectAll {
			in.rightHashes[hash]--
		} else {
			// For INTERSECT, mark as used to avoid duplicates
			in.rightHashes[hash] = 0
		}

		return t, nil
	}
}

// hashTuple computes a hash for a tuple.
func (in *Intersect) hashTuple(t *tuple.Tuple) uint32 {
	var hash uint32 = 0
	for i := 0; i < t.TupleDesc.NumFields(); i++ {
		field, _ := t.GetField(i)
		fieldHash, _ := field.Hash()
		hash = hash*31 + fieldHash
	}
	return hash
}

// Open initializes the Intersect operator by opening both child operators.
func (in *Intersect) Open() error {
	if err := in.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %v", err)
	}

	if err := in.rightChild.Open(); err != nil {
		in.leftChild.Close() // Clean up on error
		return fmt.Errorf("failed to open right child: %v", err)
	}

	in.initialized = false
	in.rightHashes = make(map[uint32]int)

	in.base.MarkOpened()
	return nil
}

// Close releases resources by closing both child operators.
func (in *Intersect) Close() error {
	leftErr := in.leftChild.Close()
	rightErr := in.rightChild.Close()

	if leftErr != nil {
		return leftErr
	}
	if rightErr != nil {
		return rightErr
	}

	return in.base.Close()
}

// GetTupleDesc returns the schema of the intersect result.
// Since both children must have compatible schemas, we return the left child's schema.
func (in *Intersect) GetTupleDesc() *tuple.TupleDescription {
	return in.leftChild.GetTupleDesc()
}

// HasNext checks if there are more tuples available from the intersect.
func (in *Intersect) HasNext() (bool, error) {
	return in.base.HasNext()
}

// Next retrieves the next tuple from the intersect.
func (in *Intersect) Next() (*tuple.Tuple, error) {
	return in.base.Next()
}

// Rewind resets the Intersect operator to the beginning.
func (in *Intersect) Rewind() error {
	if err := in.leftChild.Rewind(); err != nil {
		return err
	}
	if err := in.rightChild.Rewind(); err != nil {
		return err
	}

	in.initialized = false
	in.rightHashes = make(map[uint32]int)

	in.base.ClearCache()
	return nil
}
