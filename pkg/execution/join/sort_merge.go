package join

import (
	"fmt"
	"sort"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Comparison result constants for better readability
const (
	ComparisonEqual        = "equal"
	ComparisonLeftSmaller  = "leftSmaller"
	ComparisonRightSmaller = "rightSmaller"
)

// SortMergeJoin implements the sort-merge join algorithm
// Efficient for equality joins when data is pre-sorted or when sorting cost is acceptable
type SortMergeJoin struct {
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	leftSorted  []*tuple.Tuple
	rightSorted []*tuple.Tuple
	leftIndex   int
	rightIndex  int
	rightStart  int // For handling duplicates
	initialized bool
	stats       *JoinStatistics
	matchBuffer *JoinMatchBuffer
}

func NewSortMergeJoin(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) *SortMergeJoin {
	return &SortMergeJoin{
		leftChild:   left,
		rightChild:  right,
		predicate:   pred,
		stats:       stats,
		leftIndex:   0,
		rightIndex:  0,
		rightStart:  0,
		matchBuffer: NewJoinMatchBuffer(),
	}
}

func (smj *SortMergeJoin) Initialize() error {
	if smj.initialized {
		return nil
	}

	if err := smj.loadAndSortLeft(); err != nil {
		return err
	}

	if err := smj.loadAndSortRight(); err != nil {
		return err
	}

	smj.initialized = true
	return nil
}

func (smj *SortMergeJoin) loadAndSortLeft() error {
	tuples, err := iterator.LoadAllTuples(smj.leftChild)
	if err != nil {
		return err
	}

	smj.leftSorted = tuples
	return sortTuples(smj.leftSorted, smj.predicate.GetField1())
}

func (smj *SortMergeJoin) loadAndSortRight() error {
	tuples, err := iterator.LoadAllTuples(smj.rightChild)
	if err != nil {
		return err
	}

	smj.rightSorted = tuples
	return sortTuples(smj.rightSorted, smj.predicate.GetField2())
}

func (smj *SortMergeJoin) Next() (*tuple.Tuple, error) {
	if !smj.initialized {
		return nil, fmt.Errorf("sort-merge join not initialized")
	}

	if smj.matchBuffer.HasNext() {
		return smj.matchBuffer.Next(), nil
	}

	smj.matchBuffer.StartNew()

	for smj.leftIndex < len(smj.leftSorted) && smj.rightIndex < len(smj.rightSorted) {
		comparison, err := smj.compareCurrentTuples()
		if err != nil {
			smj.leftIndex++
			continue
		}

		switch comparison {
		case ComparisonEqual:
			if err := smj.processEqualKeys(); err != nil {
				return nil, err
			}

			// Return first match if any were found
			if result := smj.matchBuffer.GetFirstAndAdvance(); result != nil {
				return result, nil
			}

		case ComparisonLeftSmaller:
			smj.leftIndex++

		case ComparisonRightSmaller:
			smj.rightIndex++
		}
	}

	return nil, nil
}

func (smj *SortMergeJoin) Reset() error {
	smj.leftIndex = 0
	smj.rightIndex = 0
	smj.rightStart = 0
	smj.matchBuffer.Reset()
	return nil
}

func (smj *SortMergeJoin) Close() error {
	smj.leftSorted = nil
	smj.rightSorted = nil
	smj.matchBuffer.Reset()
	smj.initialized = false
	return nil
}

func (smj *SortMergeJoin) EstimateCost() float64 {
	if smj.stats == nil {
		return 1000000
	}

	totalCost := 0.0

	if !smj.stats.LeftSorted {
		leftSize := float64(smj.stats.LeftSize)
		totalCost += leftSize * 2 * logBase2(leftSize)
	}

	if !smj.stats.RightSorted {
		rightSize := float64(smj.stats.RightSize)
		totalCost += rightSize * 2 * logBase2(rightSize)
	}

	mergeCost := float64(smj.stats.LeftSize + smj.stats.RightSize)
	totalCost += mergeCost
	return totalCost
}

func (smj *SortMergeJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	op := predicate.GetOP()
	return op == query.Equals || op == query.LessThan ||
		op == query.LessThanOrEqual || op == query.GreaterThan ||
		op == query.GreaterThanOrEqual
}

func logBase2(n float64) float64 {
	if n <= 1 {
		return 0
	}
	result := 0.0
	for n > 1 {
		n /= 2
		result++
	}
	return result
}

func sortTuples(tuples []*tuple.Tuple, fieldIndex int) error {
	sort.Slice(tuples, func(i, j int) bool {
		return isLessThan(tuples[i], tuples[j], fieldIndex)
	})
	return nil
}

func isLessThan(t1, t2 *tuple.Tuple, fieldIndex int) bool {
	field1, err1 := t1.GetField(fieldIndex)
	field2, err2 := t2.GetField(fieldIndex)

	if err1 != nil || err2 != nil || field1 == nil || field2 == nil {
		return false
	}

	result, err := field1.Compare(types.LessThan, field2)
	return err == nil && result
}

func (smj *SortMergeJoin) compareCurrentTuples() (string, error) {
	leftTuple := smj.leftSorted[smj.leftIndex]
	rightTuple := smj.rightSorted[smj.rightIndex]

	leftField, err := leftTuple.GetField(smj.predicate.GetField1())
	if err != nil || leftField == nil {
		return "", fmt.Errorf("invalid left field")
	}

	rightField, err := rightTuple.GetField(smj.predicate.GetField2())
	if err != nil || rightField == nil {
		return "", fmt.Errorf("invalid right field")
	}

	equals, err := leftField.Compare(types.Equals, rightField)
	if err != nil {
		return "", err
	}
	if equals {
		return ComparisonEqual, nil
	}

	less, err := leftField.Compare(types.LessThan, rightField)
	if err != nil {
		return "", err
	}

	if less {
		return ComparisonLeftSmaller, nil
	}

	return ComparisonRightSmaller, nil
}

// processEqualKeys handles all tuples with equal join keys
//
// When keys are equal, we need to find all tuples on both sides with the same key
// and create the cross product of matches. This handles duplicate keys correctly.
func (smj *SortMergeJoin) processEqualKeys() error {
	leftTuple := smj.leftSorted[smj.leftIndex]
	leftField, _ := leftTuple.GetField(smj.predicate.GetField1())

	rightStart := smj.rightIndex

	for smj.rightIndex < len(smj.rightSorted) {
		rightTuple := smj.rightSorted[smj.rightIndex]
		rightField, err := rightTuple.GetField(smj.predicate.GetField2())
		if err != nil || rightField == nil {
			smj.rightIndex++
			continue
		}

		equals, err := leftField.Compare(types.Equals, rightField)
		if err != nil || !equals {
			break
		}

		joinedTuple, err := tuple.CombineTuples(leftTuple, rightTuple)
		if err == nil {
			smj.matchBuffer.Add(joinedTuple)
		}

		smj.rightIndex++
	}

	smj.rightIndex = rightStart
	smj.leftIndex++
	return nil
}
