package query

import (
	"fmt"
	"slices"
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

// newSetOp creates a new base for set operations with common validation.
// The caller must set the BinaryOperator field after creating the SetOp.
func newSetOp(left, right iterator.DbIterator, opType SetOperationType, preserveAll bool) (*SetOp, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("set operation children cannot be nil")
	}

	ld := left.GetTupleDesc()
	rd := right.GetTupleDesc()

	if ld.NumFields() != rd.NumFields() {
		return nil, fmt.Errorf("schema mismatch: left has %d fields, right has %d fields",
			ld.NumFields(), rd.NumFields())
	}

	var i primitives.ColumnID
	for i = 0; i < ld.NumFields(); i++ {
		lt, _ := ld.TypeAtIndex(i)
		rt, _ := rd.TypeAtIndex(i)
		if lt != rt {
			return nil, fmt.Errorf("schema mismatch at field %d: left type %v, right type %v",
				i, lt, rt)
		}
	}

	return &SetOp{
		opType:      opType,
		preserveAll: preserveAll,
		tracker:     NewTupleSetTracker(preserveAll),
		initialized: false,
	}, nil
}

// buildRightHashSet reads all tuples from the right child and builds a hash set for efficient lookups.
// This is done lazily on the first call to readNext to avoid unnecessary work if the left input is empty.
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

// setBinaryOperator sets up the BinaryOperator with the given left and right children and readNext function.
func (s *SetOp) setBinaryOperator(l, r iterator.DbIterator, readNext iterator.ReadNextFunc) error {
	binaryOp, err := iterator.NewBinaryOperator(l, r, readNext)
	if err != nil {
		return err
	}
	s.BinaryOperator = binaryOp
	return nil
}

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

// findTupleInList searches for a tuple in a list of tuples.
// Returns the index if found, -1 otherwise.
func findTupleInList(t *tuple.Tuple, list []*tuple.Tuple) int {
	return slices.IndexFunc(list, func(
		tup *tuple.Tuple) bool {
		return t.Equals(tup)
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

// Distinct operator removes duplicate tuples from its input stream.
type Distinct struct {
	*iterator.UnaryOperator
	seen   *TupleSet // Tracks unique tuples already emitted
	opened bool
}

// NewDistinct creates a new Distinct operator that removes duplicates from input.
//
// Parameters:
//   - child: Input iterator providing tuples to deduplicate
//
// Returns:
//   - *Distinct operator ready to be opened and iterated
//   - error if child is nil
func NewDistinct(child iterator.DbIterator) (*Distinct, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	d := &Distinct{
		seen: NewTupleSet(false),
	}

	unaryOp, err := iterator.NewUnaryOperator(child, d.readNext)
	if err != nil {
		return nil, err
	}
	d.UnaryOperator = unaryOp

	return d, nil
}

// readNext implements the core distinct logic.
// Called by BaseIterator to fetch the next unique tuple.
//
// Process:
//  1. Fetch next tuple from source
//  2. Check if tuple was already seen (using hash + collision detection)
//  3. If not seen: mark as seen and return tuple
//  4. If seen: loop to next tuple
//  5. Continue until unique tuple found or source exhausted
func (d *Distinct) readNext() (*tuple.Tuple, error) {
	for {
		t, err := d.FetchNext()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple from source: %w", err)
		}

		if t == nil {
			return nil, nil
		}

		if d.seen.Add(t) {
			return t, nil
		}
	}
}

// Open initializes the Distinct operator and its child.
//
// Must be called before HasNext/Next can be used.
// Resets the seen set to handle multiple Open/Close cycles.
func (d *Distinct) Open() error {
	if err := d.UnaryOperator.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}

	d.seen.Clear()
	d.opened = true

	return nil
}

// Close releases resources held by the Distinct operator and its child.
func (d *Distinct) Close() error {
	d.opened = false
	d.seen.Clear()

	return d.UnaryOperator.Close()
}

// Rewind resets the Distinct operator to the beginning.
// Clears the seen set and rewinds the source operator.
func (d *Distinct) Rewind() error {
	if !d.opened {
		return fmt.Errorf("distinct operator not opened")
	}

	d.seen.Clear()
	return d.UnaryOperator.Rewind()
}

// Union represents a UNION set operation that combines tuples from left and right inputs.
//
// UNION follows SQL standard semantics:
//   - UNION (without ALL): Returns distinct tuples from both inputs (set union)
//   - UNION ALL: Returns all tuples from both inputs, preserving duplicates
//
// Example:
//
//	Left:  {1, 2, 2, 3}
//	Right: {2, 3, 3, 4}
//	UNION:     {1, 2, 3, 4}     (distinct tuples from both)
//	UNION ALL: {1, 2, 2, 3, 2, 3, 3, 4} (all tuples, duplicates preserved)
type Union struct {
	*SetOp
}

// NewUnion creates a new Union operator.
//
// Parameters:
//   - left: The left input iterator
//   - right: The right input iterator
//   - unionAll: If true, preserves duplicates (UNION ALL); if false, returns distinct tuples
func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	base, err := newSetOp(left, right, SetUnion, unionAll)
	if err != nil {
		return nil, err
	}

	u := &Union{SetOp: base}
	err = u.setBinaryOperator(left, right, u.readNext)
	return u, err
}

// readNext implements the UNION logic using streaming approach.
//
// Algorithm:
//  1. Stream all tuples from left input first
//     - For UNION: Track seen tuples, skip duplicates
//     - For UNION ALL: Return all tuples as-is
//  2. Once left exhausted, stream tuples from right input
//     - For UNION: Skip tuples already seen from left or right
//     - For UNION ALL: Return all tuples as-is
//
// Returns:
//   - Next tuple from left (if not exhausted) or right input
//   - nil when both inputs are exhausted
//   - Error if iteration fails
func (u *Union) readNext() (*tuple.Tuple, error) {
	for !u.leftDone {
		t, err := u.FetchLeft()
		if err != nil {
			return nil, err
		}

		if t == nil {
			u.leftDone = true
			break
		}

		if !u.preserveAll {
			if !u.tracker.MarkSeen(t) {
				continue
			}
		}
		return t, nil
	}

	for {
		t, err := u.FetchRight()
		if err != nil || t == nil {
			return t, err
		}

		if !u.preserveAll {
			if !u.tracker.MarkSeen(t) {
				continue
			}
		}
		return t, nil
	}
}

// Except represents an EXCEPT set operation that returns tuples from the left input
// that do not exist in the right input.
//
// EXCEPT follows SQL standard semantics:
//   - EXCEPT (without ALL): Returns distinct tuples from left not in right (set difference)
//   - EXCEPT ALL: Returns tuples from left with duplicates, removing matching occurrences from right
//
// Example:
//
//	Left:  {1, 2, 2, 3}
//	Right: {2, 3, 3, 4}
//	EXCEPT:     {1}       (distinct tuples in left not in right)
//	EXCEPT ALL: {1, 2}    (preserves duplicates, removes one '2' for the match)
type Except struct {
	*SetOp
}

// NewExcept creates a new Except operator.
//
// Parameters:
//   - left: The left input iterator (tuples to potentially return)
//   - right: The right input iterator (tuples to exclude)
//   - exceptAll: If true, preserves duplicates (EXCEPT ALL); if false, returns distinct tuples
func NewExcept(left, right iterator.DbIterator, exceptAll bool) (*Except, error) {
	base, err := newSetOp(left, right, SetExcept, exceptAll)
	if err != nil {
		return nil, err
	}

	ex := &Except{SetOp: base}
	err = ex.setBinaryOperator(left, right, ex.readNext)
	return ex, err
}

// readNext implements the EXCEPT logic using hash-based filtering.
//
// Algorithm:
//  1. Build hash set from entire right input (lazy initialization)
//  2. Scan left input tuple-by-tuple
//  3. For EXCEPT ALL:
//     - Check reference count for tuple in right set
//     - If count > 0, decrement and skip (matching occurrence found)
//     - Otherwise, return tuple (no more matches in right)
//  4. For EXCEPT:
//     - Skip if tuple exists in right set OR was already output
//     - Mark as seen and return if new
func (ex *Except) readNext() (*tuple.Tuple, error) {
	if err := ex.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := ex.FetchLeft()
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
			if ex.tracker.ContainsInRight(t) || ex.tracker.WasSeen(t) {
				continue
			}

			ex.tracker.MarkSeen(t)
			return t, nil
		}
	}
}

// Intersect represents an INTERSECT set operation that returns tuples that appear in both inputs.
//
// INTERSECT follows SQL standard semantics:
//   - INTERSECT (without ALL): Returns distinct tuples that exist in both inputs
//   - INTERSECT ALL: Returns tuples preserving duplicates based on minimum count in either input
//
// Example:
//
//	Left:  {1, 2, 2, 3}
//	Right: {2, 3, 3, 4}
//	INTERSECT:     {2, 3}    (distinct tuples in both)
//	INTERSECT ALL: {2, 3}    (min count: 2 appears once in right, 3 appears once to match left)
type Intersect struct {
	*SetOp
}

// NewIntersect creates a new Intersect operator.
//
// Parameters:
//   - left: The left input iterator
//   - right: The right input iterator
//   - intersectAll: If true, preserves duplicates (INTERSECT ALL); if false, returns distinct tuples
func NewIntersect(left, right iterator.DbIterator, intersectAll bool) (*Intersect, error) {
	base, err := newSetOp(left, right, SetIntersect, intersectAll)
	if err != nil {
		return nil, err
	}

	in := &Intersect{SetOp: base}
	err = in.setBinaryOperator(left, right, in.readNext)
	return in, err
}

// readNext implements the INTERSECT logic using hash-based matching.
//
// Algorithm:
//  1. Build hash set from entire right input with reference counts (lazy initialization)
//  2. Scan left input tuple-by-tuple
//  3. Skip tuples not found in right set
//  4. For INTERSECT ALL:
//     - Check if right count > 0 (matches remaining)
//     - Decrement count and return tuple
//  5. For INTERSECT:
//     - Check if right count > 0 (not yet output)
//     - Remove from right set to prevent duplicate output
//     - Return tuple
func (in *Intersect) readNext() (*tuple.Tuple, error) {
	if err := in.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := in.FetchLeft()
		if err != nil || t == nil {
			return t, err
		}

		if !in.tracker.ContainsInRight(t) {
			continue
		}

		if in.preserveAll {
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.DecrementRight(t)
		} else {
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.rightSet.Remove(t)
		}
		return t, nil
	}
}
