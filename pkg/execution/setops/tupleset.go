package setops

import (
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// TupleSet provides a hash-based set abstraction for tuples with collision detection.
// It supports both set semantics (distinct) and bag semantics (with counts).
// Hash collisions are handled by storing actual tuple references for comparison.
type TupleSet struct {
	hashes      map[primitives.HashCode]int            // Hash -> count
	tuples      map[primitives.HashCode][]*tuple.Tuple // Hash -> list of tuples (for collision detection)
	preserveAll bool                                   // If true, tracks counts; if false, just presence
}

// NewTupleSet creates a new tuple set.
func NewTupleSet(preserveAll bool) *TupleSet {
	return &TupleSet{
		hashes:      make(map[primitives.HashCode]int),
		tuples:      make(map[primitives.HashCode][]*tuple.Tuple),
		preserveAll: preserveAll,
	}
}

// hashTuple computes a hash for a tuple based on all its fields.
func hashTuple(t *tuple.Tuple) primitives.HashCode {
	var hash primitives.HashCode = 0
	var i primitives.ColumnID
	for i = 0; i < t.TupleDesc.NumFields(); i++ {
		field, _ := t.GetField(i)
		fieldHash, _ := field.Hash()
		hash = hash*31 + fieldHash
	}
	return hash
}

// tuplesEqual compares two tuples for equality by comparing all fields.
// This is used for collision detection when two tuples have the same hash.
func tuplesEqual(t1, t2 *tuple.Tuple) bool {
	if t1.TupleDesc.NumFields() != t2.TupleDesc.NumFields() {
		return false
	}

	var i primitives.ColumnID
	for i = 0; i < t1.TupleDesc.NumFields(); i++ {
		f1, _ := t1.GetField(i)
		f2, _ := t2.GetField(i)

		if !f1.Equals(f2) {
			return false
		}
	}

	return true
}

// findTupleInList searches for a tuple in a list of tuples.
// Returns the index if found, -1 otherwise.
func findTupleInList(t *tuple.Tuple, list []*tuple.Tuple) int {
	return slices.IndexFunc(list, func(
		tup *tuple.Tuple) bool {
		return tuplesEqual(t, tup)
	})
}

// Add adds a tuple to the set with collision detection.
// Returns true if the tuple was added (or count incremented for bag semantics).
func (ts *TupleSet) Add(t *tuple.Tuple) bool {
	hash := hashTuple(t)

	existingTuples, hasHash := ts.tuples[hash]

	if ts.preserveAll {
		// Bag semantics: always add, increment count
		if hasHash && findTupleInList(t, existingTuples) >= 0 {
			ts.hashes[hash]++
			return true
		}

		ts.tuples[hash] = append(existingTuples, t)
		ts.hashes[hash]++
		return true
	} else {
		// Set semantics: only add if not present
		if hasHash && findTupleInList(t, existingTuples) >= 0 {
			return false
		}

		ts.tuples[hash] = append(existingTuples, t)
		ts.hashes[hash] = 1
		return true
	}
}

// Contains checks if a tuple exists in the set with collision detection.
// For bag semantics, returns true if count > 0.
func (ts *TupleSet) Contains(t *tuple.Tuple) bool {
	hash := hashTuple(t)
	existingTuples, hasHash := ts.tuples[hash]

	if !hasHash {
		return false
	}

	idx := findTupleInList(t, existingTuples)
	return idx >= 0 && ts.hashes[hash] > 0
}

// GetCount returns the count of a tuple in the set.
// For set semantics, returns 1 if present, 0 otherwise.
func (ts *TupleSet) GetCount(t *tuple.Tuple) int {
	hash := hashTuple(t)
	return ts.hashes[hash]
}

// Decrement reduces the count of a tuple by 1.
// Returns true if the tuple still exists after decrement.
func (ts *TupleSet) Decrement(t *tuple.Tuple) bool {
	hash := hashTuple(t)
	count, exists := ts.hashes[hash]

	if !exists || count <= 0 {
		return false
	}

	ts.hashes[hash]--
	return ts.hashes[hash] > 0
}

// Remove completely removes a tuple from the set (sets count to 0).
func (ts *TupleSet) Remove(t *tuple.Tuple) {
	hash := hashTuple(t)
	ts.hashes[hash] = 0
}

// Clear empties the set.
func (ts *TupleSet) Clear() {
	clear(ts.hashes)
	clear(ts.tuples)
}

// Size returns the number of unique hashes in the set.
func (ts *TupleSet) Size() int {
	return len(ts.hashes)
}

// TupleSetTracker combines two sets for tracking seen tuples in complex operations.
// Used for operations like UNION and EXCEPT that need to track both inputs.
type TupleSetTracker struct {
	rightSet *TupleSet
	seenSet  *TupleSet
}

// NewTupleSetTracker creates a tracker with a right set and optional seen set.
func NewTupleSetTracker(preserveAll bool) *TupleSetTracker {
	return &TupleSetTracker{
		rightSet: NewTupleSet(preserveAll),
		seenSet:  NewTupleSet(false), // seen set is always distinct
	}
}

// AddToRight adds a tuple to the right set.
func (tst *TupleSetTracker) AddToRight(t *tuple.Tuple) {
	tst.rightSet.Add(t)
}

// ContainsInRight checks if a tuple exists in the right set.
func (tst *TupleSetTracker) ContainsInRight(t *tuple.Tuple) bool {
	return tst.rightSet.Contains(t)
}

// MarkSeen marks a tuple as seen in the seen set.
// Returns true if this is the first time seeing it.
func (tst *TupleSetTracker) MarkSeen(t *tuple.Tuple) bool {
	return tst.seenSet.Add(t)
}

// WasSeen checks if a tuple was already seen.
func (tst *TupleSetTracker) WasSeen(t *tuple.Tuple) bool {
	return tst.seenSet.Contains(t)
}

// DecrementRight decrements the count in the right set.
func (tst *TupleSetTracker) DecrementRight(t *tuple.Tuple) bool {
	return tst.rightSet.Decrement(t)
}

// GetRightCount returns the count in the right set.
func (tst *TupleSetTracker) GetRightCount(t *tuple.Tuple) int {
	return tst.rightSet.GetCount(t)
}

// Clear resets both sets.
func (tst *TupleSetTracker) Clear() {
	tst.rightSet.Clear()
	tst.seenSet.Clear()
}
