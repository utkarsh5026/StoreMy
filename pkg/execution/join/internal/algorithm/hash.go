package algorithm

import (
	"fmt"
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// HashJoin implements an equi-join using hash-based algorithm.
// It builds a hash table from the smaller relation (right child) and probes it
// with tuples from the larger relation (left child) for efficient joining.
//
// The algorithm works in two phases:
// 1. Build phase: Creates hash table from right child using join key
// 2. Probe phase: For each left tuple, looks up matches in hash table
//
// Time complexity: O(|R| + |S|) where R and S are the input relations
// Space complexity: O(|S|) for the hash table storage
type HashJoin struct {
	common.BaseJoin
	hashTable   map[string][]*tuple.Tuple
	currentLeft *tuple.Tuple
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(left, right iterator.DbIterator, pred common.JoinPredicate, stats *common.JoinStatistics) *HashJoin {
	return &HashJoin{
		BaseJoin:  common.NewBaseJoin(left, right, pred, stats),
		hashTable: make(map[string][]*tuple.Tuple),
	}
}

// Close releases all resources held by the hash join operator.
// This includes clearing the hash table and resetting all state variables.
func (hj *HashJoin) Close() error {
	clear(hj.hashTable)
	hj.currentLeft = nil
	return hj.BaseJoin.Close()
}

// Next returns the next joined tuple from the hash join operation.
// Must call Initialize() before first use.
func (hj *HashJoin) Next() (*tuple.Tuple, error) {
	if !hj.IsInitialized() {
		return nil, fmt.Errorf("hash join not initialized")
	}

	if match := hj.GetMatchFromBuffer(); match != nil {
		return hj.combineWithCurrent(match)
	}

	return hj.findNextMatch()
}

// Reset rewinds the hash join to the beginning, allowing re-iteration.
// The hash table is preserved, only the iteration state is reset.
func (hj *HashJoin) Reset() error {
	hj.currentLeft = nil
	hj.ResetCommon()
	return hj.LeftChild().Rewind()
}

// Initialize builds the hash table from the right child relation.
// This must be called before any Next() operations.
// The method is idempotent - multiple calls are safe.
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

// EstimateCost returns the estimated cost of executing this hash join.
// Uses the classic formula: 3 * (|R| + |S|) representing the cost of
// reading both relations plus the hash table operations.
func (hj *HashJoin) EstimateCost() float64 {
	stats := hj.Stats()
	if stats == nil {
		return common.DefaultHighCost
	}
	return 3 * float64(stats.LeftSize+stats.RightSize)
}

// SupportsPredicateType checks if this hash join can handle the given predicate.
func (hj *HashJoin) SupportsPredicateType(predicate common.JoinPredicate) bool {
	return predicate.GetOP() == primitives.Equals
}

// findNextJoinedTuple finds the next left tuple that has matching right tuples.
// Iterates through left tuples until finding one with matches in the hash table.
func (hj *HashJoin) findNextMatch() (*tuple.Tuple, error) {
	leftFieldIndex := hj.Predicate().GetLeftField()

	for {
		leftTuple, err := hj.nextLeftTuple()
		if err != nil {
			return nil, err
		}
		if leftTuple == nil {
			return nil, nil // No more tuples
		}

		key, err := common.ExtractJoinKey(leftTuple, leftFieldIndex)
		if err != nil {
			continue
		}

		matches, exists := hj.hashTable[key]
		if !exists || len(matches) == 0 {
			continue
		}

		return hj.setupMatches(leftTuple, matches)
	}
}

// nextLeftTuple gets next tuple from left child.
func (hj *HashJoin) nextLeftTuple() (*tuple.Tuple, error) {
	hasNext, err := hj.LeftChild().HasNext()
	if err != nil || !hasNext {
		return nil, err
	}
	return hj.LeftChild().Next()
}

// setupMatches prepares the iteration state for a left tuple with matches.
// Sets up currentLeft, matchBuffer, and returns the first joined result.
func (hj *HashJoin) setupMatches(leftTuple *tuple.Tuple, matches []*tuple.Tuple) (*tuple.Tuple, error) {
	hj.currentLeft = leftTuple
	firstRight := hj.MatchBuffer().SetMatches(matches)
	if firstRight == nil {
		return nil, nil
	}

	return tuple.CombineTuples(leftTuple, firstRight)
}

// buildHashTable constructs the hash table by reading all tuples from the right child.
// Each tuple is hashed using the join key specified in the predicate.
//
// The hash table maps string join keys to slices of tuples, allowing for
// duplicate keys (multiple tuples with the same join key value).
func (h *HashJoin) buildHashTable() error {
	rightFieldIndex := h.Predicate().GetRightField()
	rightTuples, err := iterator.Map(h.RightChild(), func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})

	if err != nil {
		return err
	}

	for _, rightTuple := range rightTuples {
		if err := h.addToHashTable(rightTuple, rightFieldIndex); err != nil {
			continue
		}
	}

	return nil
}

// addToHashTable adds a single tuple to the hash table using the specified field as key.
// Handles duplicate keys by appending to the existing slice of tuples.
func (hj *HashJoin) addToHashTable(rightTuple *tuple.Tuple, fieldIndex primitives.ColumnID) error {
	joinKey, err := common.ExtractJoinKey(rightTuple, fieldIndex)
	if err != nil {
		return err
	}

	hj.hashTable[joinKey] = append(hj.hashTable[joinKey], rightTuple)
	return nil
}

func (hj *HashJoin) combineWithCurrent(right *tuple.Tuple) (*tuple.Tuple, error) {
	result, err := tuple.CombineTuples(hj.currentLeft, right)
	if !hj.MatchBuffer().HasNext() {
		hj.currentLeft = nil
	}
	return result, err
}
