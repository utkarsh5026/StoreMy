package setops

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

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
	base, err := NewSetOperationBase(left, right, SetExcept, exceptAll)
	if err != nil {
		return nil, err
	}

	ex := &Except{SetOp: base}
	ex.base = iterator.NewBaseIterator(ex.readNext)
	return ex, nil
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
		t, err := ex.leftChild.Next()
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
