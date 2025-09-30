package join

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
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
	leftChild      iterator.DbIterator       // Left input relation (outer)
	rightChild     iterator.DbIterator       // Right input relation (inner, used for hash table)
	predicate      *JoinPredicate            // Join condition (must be equality)
	hashTable      map[string][]*tuple.Tuple // Hash table: join key -> matching right tuples
	currentMatches []*tuple.Tuple            // Current set of right tuples matching current left tuple
	currentLeft    *tuple.Tuple              // Current left tuple being processed
	matchIndex     int                       // Index into currentMatches for iteration
	initialized    bool                      // Whether hash table has been built
	stats          *JoinStatistics           // Statistics for cost estimation
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) *HashJoin {
	return &HashJoin{
		leftChild:   left,
		rightChild:  right,
		predicate:   pred,
		hashTable:   make(map[string][]*tuple.Tuple),
		matchIndex:  -1,
		stats:       stats,
		initialized: false,
	}
}

// Close releases all resources held by the hash join operator.
// This includes clearing the hash table and resetting all state variables.
func (hj *HashJoin) Close() error {
	hj.hashTable = make(map[string][]*tuple.Tuple)
	hj.currentMatches = nil
	hj.currentLeft = nil
	hj.initialized = false
	return nil
}

// Next returns the next joined tuple from the hash join operation.
// Must call Initialize() before first use.
func (hj *HashJoin) Next() (*tuple.Tuple, error) {
	if !hj.initialized {
		return nil, fmt.Errorf("hash join not initialized")
	}

	if hj.hasCurrentMatches() {
		return hj.getNextMatch()
	}

	return hj.findNextJoinedTuple()
}

// Reset rewinds the hash join to the beginning, allowing re-iteration.
// The hash table is preserved, only the iteration state is reset.
func (hj *HashJoin) Reset() error {
	hj.currentMatches = nil
	hj.currentLeft = nil
	hj.matchIndex = -1

	return hj.leftChild.Rewind()
}

// Initialize builds the hash table from the right child relation.
// This must be called before any Next() operations.
// The method is idempotent - multiple calls are safe.
func (hj *HashJoin) Initialize() error {
	if hj.initialized {
		return nil
	}

	if err := hj.buildHashTable(); err != nil {
		return fmt.Errorf("failed to build hash table: %w", err)
	}

	hj.initialized = true
	return nil
}

// EstimateCost returns the estimated cost of executing this hash join.
// Uses the classic formula: 3 * (|R| + |S|) representing the cost of
// reading both relations plus the hash table operations.
//
// Returns a high default cost if no statistics are available.
func (hj *HashJoin) EstimateCost() float64 {
	if hj.stats == nil {
		return 1000000 // High default cost
	}

	// Cost = 3 * (|R| + |S|) for build and probe phases
	buildCost := float64(hj.stats.RightSize)
	probeCost := float64(hj.stats.LeftSize)
	return 3 * (buildCost + probeCost)
}

// SupportsPredicateType checks if this hash join can handle the given predicate.
// Hash joins only support equality predicates.
func (hj *HashJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	return predicate.GetOP() == query.Equals
}

// hasCurrentMatches checks if there are remaining matches for the current left tuple.
func (hj *HashJoin) hasCurrentMatches() bool {
	return hj.currentMatches != nil &&
		hj.matchIndex >= 0 &&
		hj.matchIndex < len(hj.currentMatches)
}

// getNextMatch returns the next match for the current left tuple.
// Advances the match index and clears state when all matches are exhausted.
func (hj *HashJoin) getNextMatch() (*tuple.Tuple, error) {
	result, err := tuple.CombineTuples(hj.currentLeft, hj.currentMatches[hj.matchIndex])
	hj.matchIndex++

	// Clear current matches if we've returned them all
	if hj.matchIndex >= len(hj.currentMatches) {
		hj.currentMatches = nil
		hj.currentLeft = nil
		hj.matchIndex = -1
	}

	return result, err
}

// findNextJoinedTuple finds the next left tuple that has matching right tuples.
// Iterates through left tuples until finding one with matches in the hash table.
//
// Returns:
//   - The first joined tuple for the found match, or nil if no more matches exist
//   - Any error encountered during processing
func (hj *HashJoin) findNextJoinedTuple() (*tuple.Tuple, error) {
	leftFieldIndex := hj.predicate.GetField1()

	for {
		leftTuple, err := hj.getNextLeftTuple()
		if err != nil {
			return nil, err
		}
		if leftTuple == nil {
			return nil, nil
		}

		joinKey, err := extractJoinKey(leftTuple, leftFieldIndex)
		if err != nil {
			continue
		}

		matches := hj.findMatches(joinKey)
		if len(matches) == 0 {
			continue // No matches, try next left tuple
		}

		return hj.setupMatches(leftTuple, matches)
	}
}

// getNextLeftTuple retrieves the next tuple from the left child iterator.
// Returns nil tuple (not error) when no more tuples are available.
func (hj *HashJoin) getNextLeftTuple() (*tuple.Tuple, error) {
	hasNext, err := hj.leftChild.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, nil
	}

	return hj.leftChild.Next()
}

// findMatches looks up all right tuples that match the given join key.
// Returns nil if no matches are found.
func (hj *HashJoin) findMatches(key string) []*tuple.Tuple {
	matches, exists := hj.hashTable[key]
	if !exists {
		return nil
	}
	return matches
}

// setupMatches prepares the iteration state for a left tuple with matches.
// Sets up currentLeft, currentMatches, and returns the first joined result.
func (hj *HashJoin) setupMatches(leftTuple *tuple.Tuple, matches []*tuple.Tuple) (*tuple.Tuple, error) {
	hj.currentLeft = leftTuple
	hj.currentMatches = matches
	hj.matchIndex = 1 // We'll return index 0 now, next call starts at 1

	return tuple.CombineTuples(leftTuple, matches[0])
}

// buildHashTable constructs the hash table by reading all tuples from the right child.
// Each tuple is hashed using the join key specified in the predicate.
//
// The hash table maps string join keys to slices of tuples, allowing for
// duplicate keys (multiple tuples with the same join key value).
func (hj *HashJoin) buildHashTable() error {
	rightFieldIndex := hj.predicate.GetField2()

	for {
		hasNext, err := hj.rightChild.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		rightTuple, err := hj.rightChild.Next()
		if err != nil {
			return err
		}
		if rightTuple == nil {
			continue
		}

		if err := hj.addToHashTable(rightTuple, rightFieldIndex); err != nil {
			continue
		}
	}

	return nil
}

// addToHashTable adds a single tuple to the hash table using the specified field as key.
// Handles duplicate keys by appending to the existing slice of tuples.
func (hj *HashJoin) addToHashTable(rightTuple *tuple.Tuple, fieldIndex int) error {
	joinKey, err := extractJoinKey(rightTuple, fieldIndex)
	if err != nil {
		return err
	}

	hj.hashTable[joinKey] = append(hj.hashTable[joinKey], rightTuple)
	return nil
}

// extractJoinKey extracts the join key from a tuple at the specified field index.
// The key is converted to string for use in the hash table.
func extractJoinKey(t *tuple.Tuple, fieldIndex int) (string, error) {
	field, err := t.GetField(fieldIndex)
	if err != nil || field == nil {
		return "", fmt.Errorf("invalid join key")
	}
	return field.String(), nil
}
