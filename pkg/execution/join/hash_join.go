package join

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

type HashJoin struct {
	leftChild      iterator.DbIterator
	rightChild     iterator.DbIterator
	predicate      *JoinPredicate
	hashTable      map[string][]*tuple.Tuple
	currentMatches []*tuple.Tuple
	currentLeft    *tuple.Tuple
	matchIndex     int
	initialized    bool
	stats          *JoinStatistics
}

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

func (hj *HashJoin) Close() error {
	hj.hashTable = make(map[string][]*tuple.Tuple)
	hj.currentMatches = nil
	hj.currentLeft = nil
	hj.initialized = false
	return nil
}

func (hj *HashJoin) Next() (*tuple.Tuple, error) {
	if !hj.initialized {
		return nil, fmt.Errorf("hash join not initialized")
	}

	if hj.hasCurrentMatches() {
		result, err := tuple.CombineTuples(hj.currentLeft, hj.currentMatches[hj.matchIndex])
		hj.matchIndex++

		if hj.matchIndex >= len(hj.currentMatches) {
			hj.currentMatches = nil
			hj.currentLeft = nil
			hj.matchIndex = -1
		}

		return result, err
	}

	return hj.findNextJoinedTuple()
}

func (hj *HashJoin) Reset() error {
	hj.currentMatches = nil
	hj.currentLeft = nil
	hj.matchIndex = -1

	return hj.leftChild.Rewind()
}

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

func (hj *HashJoin) EstimateCost() float64 {
	if hj.stats == nil {
		return 1000000 // High default cost
	}

	// Cost = 3 * (|R| + |S|) for build and probe phases
	buildCost := float64(hj.stats.RightSize)
	probeCost := float64(hj.stats.LeftSize)
	return 3 * (buildCost + probeCost)
}

func (hj *HashJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	return predicate.GetOP() == query.Equals
}

func (hj *HashJoin) hasCurrentMatches() bool {
	return hj.currentMatches != nil &&
		hj.matchIndex >= 0 &&
		hj.matchIndex < len(hj.currentMatches)
}

// findNextJoinedTuple finds the next left tuple that has matching right tuples
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

// getNextLeftTuple retrieves the next tuple from the left child
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

func (hj *HashJoin) findMatches(key string) []*tuple.Tuple {
	matches, exists := hj.hashTable[key]
	if !exists {
		return nil
	}
	return matches
}

func (hj *HashJoin) setupMatches(leftTuple *tuple.Tuple, matches []*tuple.Tuple) (*tuple.Tuple, error) {
	hj.currentLeft = leftTuple
	hj.currentMatches = matches
	hj.matchIndex = 1 // We'll return index 0 now, next call starts at 1

	return tuple.CombineTuples(leftTuple, matches[0])
}

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

// addToHashTable adds a tuple to the hash table with the given field as key
func (hj *HashJoin) addToHashTable(rightTuple *tuple.Tuple, fieldIndex int) error {
	joinKey, err := extractJoinKey(rightTuple, fieldIndex)
	if err != nil {
		return err
	}

	hj.hashTable[joinKey] = append(hj.hashTable[joinKey], rightTuple)
	return nil
}

func extractJoinKey(t *tuple.Tuple, fieldIndex int) (string, error) {
	field, err := t.GetField(fieldIndex)
	if err != nil || field == nil {
		return "", fmt.Errorf("invalid join key")
	}
	return field.String(), nil
}
