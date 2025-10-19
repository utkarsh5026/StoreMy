package query

import (
	"storemy/pkg/tuple"
)

// TupleSet provides a hash-based set abstraction for tuples.
// It supports both set semantics (distinct) and bag semantics (with counts).
type TupleSet struct {
	hashes      map[uint32]int  // Hash -> count
	preserveAll bool            // If true, tracks counts; if false, just presence
}

// NewTupleSet creates a new tuple set.
func NewTupleSet(preserveAll bool) *TupleSet {
	return &TupleSet{
		hashes:      make(map[uint32]int),
		preserveAll: preserveAll,
	}
}

// hashTuple computes a hash for a tuple based on all its fields.
func hashTuple(t *tuple.Tuple) uint32 {
	var hash uint32 = 0
	for i := 0; i < t.TupleDesc.NumFields(); i++ {
		field, _ := t.GetField(i)
		fieldHash, _ := field.Hash()
		hash = hash*31 + fieldHash
	}
	return hash
}

// Add adds a tuple to the set.
// Returns true if the tuple was added (or count incremented for bag semantics).
func (ts *TupleSet) Add(t *tuple.Tuple) bool {
	hash := hashTuple(t)

	if ts.preserveAll {
		// Bag semantics: always add, increment count
		ts.hashes[hash]++
		return true
	} else {
		// Set semantics: only add if not present
		if _, exists := ts.hashes[hash]; exists {
			return false
		}
		ts.hashes[hash] = 1
		return true
	}
}

// Contains checks if a tuple exists in the set.
// For bag semantics, returns true if count > 0.
func (ts *TupleSet) Contains(t *tuple.Tuple) bool {
	hash := hashTuple(t)
	count, exists := ts.hashes[hash]
	return exists && count > 0
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
	ts.hashes = make(map[uint32]int)
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
