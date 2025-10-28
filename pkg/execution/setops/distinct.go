package setops

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Distinct operator removes duplicate tuples from its input stream.
//
// Implementation:
//   - Uses a TupleSet with set semantics (preserveAll=false) for deduplication
//   - Hash-based approach with collision detection for correctness
//   - Streaming operator: emits tuples as soon as they're determined to be unique
//   - Memory usage: O(distinct tuples) - stores all unique tuples seen
//
// Algorithm:
//  1. For each input tuple:
//     a. Compute hash of tuple (based on all fields)
//     b. Check if tuple exists in seen set (with collision detection)
//     c. If not seen: add to set and emit tuple
//     d. If seen: skip tuple
//
// Performance Characteristics:
//   - Time: O(n*m) where n=tuples, m=fields per tuple (for hashing)
//   - Space: O(d) where d=distinct tuples
//   - Hash lookups: O(1) average case
//   - Collision resolution: O(k) where k=tuples with same hash (typically 1)
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
