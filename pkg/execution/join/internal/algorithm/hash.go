package algorithm

import (
	"fmt"
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// HashJoin implements an in-memory hash-based equi-join operator.
//
// The operator builds a hash table from the right (inner/smaller) child using
// the join key specified by the predicate, then probes that table with tuples
// from the left (outer/larger) child to produce joined tuples.
//
// Behavior summary:
//   - Build phase: read all tuples from the right child and group them by the
//     join key into an in-memory map.
//   - Probe phase: for each tuple from the left child, extract the join key,
//     look up matching right tuples in the hash table and emit combined tuples.
//   - Null/invalid keys or tuples that fail key extraction are skipped.
//   - The operator preserves the hash table across Reset() calls; only
//     iteration state is rewound.
//
// Complexity:
//   - Time: O(|Left| + |Right|) for build + probe (amortized).
//   - Space: O(|Right|) for storing the hash table.
//
// Note: Initialize() must be called before the first Next() to populate the
// table. The operator is safe to call Initialize() multiple times (idempotent).
type HashJoin struct {
	common.BaseJoin
	hashTable   map[primitives.HashCode][]*tuple.Tuple
	currentLeft *tuple.Tuple
}

// NewHashJoin creates and returns a new HashJoin operator.
//
// Parameters:
//   - left:  outer relation iterator (probed during probe phase)
//   - right: inner relation iterator (used to build the hash table)
//   - pred:  join predicate describing join keys and operator
//   - stats: optional statistics used for cost estimation
//
// The returned operator is not initialized; call Initialize() before Next().
func NewHashJoin(left, right iterator.DbIterator, pred common.JoinPredicate, stats *common.JoinStatistics) *HashJoin {
	return &HashJoin{
		BaseJoin:  common.NewBaseJoin(left, right, pred, stats),
		hashTable: make(map[primitives.HashCode][]*tuple.Tuple),
	}
}

// Close releases resources held by the operator.
//
// This clears the in-memory hash table, resets iteration state and then
// delegates to the BaseJoin Close() for any additional cleanup.
func (hj *HashJoin) Close() error {
	clear(hj.hashTable)
	hj.currentLeft = nil
	return hj.BaseJoin.Close()
}

// Next returns the next combined tuple produced by the join.
//
// Call Initialize() before the first Next(). Returns (nil, nil) when the
// join is exhausted. Errors from child iterators or internal operations are
// propagated.
func (hj *HashJoin) Next() (*tuple.Tuple, error) {
	if !hj.IsInitialized() {
		return nil, fmt.Errorf("hash join not initialized")
	}

	if match := hj.GetMatchFromBuffer(); match != nil {
		return hj.combineWithCurrent(match)
	}

	return hj.findNextMatch()
}

// Reset rewinds the join operator so iteration can start again.
//
// The hash table built from the right child is preserved across resets;
// only the iteration state (currentLeft and BaseJoin state) is reset. The
// left child iterator is rewound to its beginning.
func (hj *HashJoin) Reset() error {
	hj.currentLeft = nil
	if err := hj.ResetCommon(); err != nil {
		return err
	}
	return hj.LeftChild().Rewind()
}

// Initialize builds the in-memory hash table from the right child.
//
// This method is idempotent: calling it multiple times has no additional
// effect. It must be invoked before the first call to Next().
func (hj *HashJoin) Initialize() error {
	if hj.IsInitialized() {
		return nil
	}

	if err := hj.buildHashTable(); err != nil {
		return fmt.Errorf("failed to build hash table: %w", err)
	}

	hj.SetInitialized()
	return nil
}

// EstimateCost returns an estimated execution cost for this operator.
//
// If statistics are unavailable a high default cost is returned. The current
// heuristic uses 3 * (|Left| + |Right|) to account for scanning and hashing.
func (hj *HashJoin) EstimateCost() float64 {
	stats := hj.Stats()
	if stats == nil {
		return common.DefaultHighCost
	}
	return 3 * float64(stats.LeftSize+stats.RightSize)
}

// SupportsPredicateType reports whether the join operator supports the given
// predicate type. HashJoin currently supports only equality predicates.
func (hj *HashJoin) SupportsPredicateType(predicate common.JoinPredicate) bool {
	return predicate.GetOP() == primitives.Equals
}

// findNextMatch advances the left child until a tuple with one or more
// matching right-side tuples is found, sets up iteration state and returns
// the first joined tuple for that left tuple.
//
// Returns (nil, nil) when the left child is exhausted.
func (hj *HashJoin) findNextMatch() (*tuple.Tuple, error) {
	lf := hj.Predicate().GetLeftField()

	for {
		lt, err := hj.nextLeftTuple()
		if err != nil {
			return nil, err
		}
		if lt == nil {
			return nil, nil // No more tuples
		}

		key, err := common.ExtractJoinKey(lt, lf)
		if err != nil {
			// skip tuples that don't produce a valid join key
			continue
		}

		matches, exists := hj.hashTable[key]
		if !exists || len(matches) == 0 {
			continue
		}

		return hj.setupMatches(lt, matches)
	}
}

// nextLeftTuple returns the next tuple from the left child or nil when
// the left child is exhausted. Errors from the child are propagated.
func (hj *HashJoin) nextLeftTuple() (*tuple.Tuple, error) {
	hasNext, err := hj.LeftChild().HasNext()
	if err != nil || !hasNext {
		return nil, err
	}
	return hj.LeftChild().Next()
}

// setupMatches initializes state when a left tuple has matching right tuples.
//
// It sets currentLeft, loads matches into the match buffer and returns the
// first combined tuple to be emitted.
func (hj *HashJoin) setupMatches(leftTuple *tuple.Tuple, matches []*tuple.Tuple) (*tuple.Tuple, error) {
	hj.currentLeft = leftTuple
	firstRight := hj.MatchBuffer().SetMatches(matches)
	if firstRight == nil {
		return nil, nil
	}

	return tuple.CombineTuples(leftTuple, firstRight)
}

// buildHashTable reads all tuples from the right child and groups them
// into the in-memory hashTable keyed by the join key.
func (h *HashJoin) buildHashTable() error {
	rf := h.Predicate().GetRightField()
	rightTuples, err := iterator.Collect(h.RightChild())

	if err != nil {
		return err
	}

	for _, rightTuple := range rightTuples {
		if err := h.addToHashTable(rightTuple, rf); err != nil {
			// Skip right tuples that cannot produce a valid join key
			continue
		}
	}

	return nil
}

// addToHashTable inserts a single right tuple into the hash table under the
// extracted join key. Duplicate keys are allowed and stored as a slice.
func (hj *HashJoin) addToHashTable(rightTuple *tuple.Tuple, fieldIndex primitives.ColumnID) error {
	joinKey, err := common.ExtractJoinKey(rightTuple, fieldIndex)
	if err != nil {
		return err
	}

	hj.hashTable[joinKey] = append(hj.hashTable[joinKey], rightTuple)
	return nil
}

// combineWithCurrent combines the stored currentLeft tuple with the provided
// right tuple and advances the match buffer. If there are no remaining
// matches the currentLeft state is cleared.
func (hj *HashJoin) combineWithCurrent(right *tuple.Tuple) (*tuple.Tuple, error) {
	result, err := tuple.CombineTuples(hj.currentLeft, right)
	if !hj.MatchBuffer().HasNext() {
		hj.currentLeft = nil
	}
	return result, err
}
