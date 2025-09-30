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

	leftFieldIndex := hj.predicate.GetField1()
	for {
		hasNext, err := hj.leftChild.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, nil
		}

		leftTuple, err := hj.leftChild.Next()
		if err != nil {
			return nil, err
		}
		if leftTuple == nil {
			continue
		}

		field, err := leftTuple.GetField(leftFieldIndex)
		if err != nil || field == nil {
			continue
		}

		key := field.String()
		matches, exists := hj.hashTable[key]
		if !exists || len(matches) == 0 {
			continue
		}

		// Found matches, buffer them
		hj.currentLeft = leftTuple
		hj.currentMatches = matches
		hj.matchIndex = 0

		result, err := tuple.CombineTuples(leftTuple, matches[0])
		hj.matchIndex++
		return result, err
	}
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

		field, err := rightTuple.GetField(rightFieldIndex)
		if err != nil || field == nil {
			continue
		}

		key := field.String()
		hj.hashTable[key] = append(hj.hashTable[key], rightTuple)
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
