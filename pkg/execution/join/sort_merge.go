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

func (s *SortMergeJoin) Initialize() error {
	if s.initialized {
		return nil
	}

	if err := s.loadAndSortLeft(); err != nil {
		return err
	}

	if err := s.loadAndSortRight(); err != nil {
		return err
	}

	s.initialized = true
	return nil
}

func (s *SortMergeJoin) loadAndSortLeft() error {
	tuples, err := iterator.LoadAllTuples(s.leftChild)
	if err != nil {
		return err
	}

	s.leftSorted = tuples
	return sortTuples(s.leftSorted, s.predicate.GetField1())
}

func (s *SortMergeJoin) loadAndSortRight() error {
	tuples, err := iterator.LoadAllTuples(s.rightChild)
	if err != nil {
		return err
	}

	s.rightSorted = tuples
	return sortTuples(s.rightSorted, s.predicate.GetField2())
}

func (s *SortMergeJoin) Next() (*tuple.Tuple, error) {
	if !s.initialized {
		return nil, fmt.Errorf("sort-merge join not initialized")
	}

	if s.matchBuffer.HasNext() {
		return s.matchBuffer.Next(), nil
	}

	s.matchBuffer.StartNew()

	for s.leftIndex < len(s.leftSorted) && s.rightIndex < len(s.rightSorted) {
		comparison, err := s.compareCurrentTuples()
		if err != nil {
			s.leftIndex++
			continue
		}

		switch comparison {
		case ComparisonEqual:
			if err := s.processEqualKeys(); err != nil {
				return nil, err
			}

			if result := s.matchBuffer.GetFirstAndAdvance(); result != nil {
				return result, nil
			}

		case ComparisonLeftSmaller:
			s.leftIndex++

		case ComparisonRightSmaller:
			s.rightIndex++
		}
	}

	return nil, nil
}

func (s *SortMergeJoin) Reset() error {
	s.leftIndex = 0
	s.rightIndex = 0
	s.rightStart = 0
	s.matchBuffer.Reset()
	return nil
}

func (s *SortMergeJoin) Close() error {
	s.leftSorted = nil
	s.rightSorted = nil
	s.matchBuffer.Reset()
	s.initialized = false
	return nil
}

func (s *SortMergeJoin) EstimateCost() float64 {
	if s.stats == nil {
		return 1000000
	}

	totalCost := 0.0

	if !s.stats.LeftSorted {
		leftSize := float64(s.stats.LeftSize)
		totalCost += leftSize * 2 * logBase2(leftSize)
	}

	if !s.stats.RightSorted {
		rightSize := float64(s.stats.RightSize)
		totalCost += rightSize * 2 * logBase2(rightSize)
	}

	mergeCost := float64(s.stats.LeftSize + s.stats.RightSize)
	totalCost += mergeCost
	return totalCost
}

func (s *SortMergeJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
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

func (s *SortMergeJoin) compareCurrentTuples() (string, error) {
	leftTuple := s.leftSorted[s.leftIndex]
	rightTuple := s.rightSorted[s.rightIndex]

	leftField, err := leftTuple.GetField(s.predicate.GetField1())
	if err != nil || leftField == nil {
		return "", fmt.Errorf("invalid left field")
	}

	rightField, err := rightTuple.GetField(s.predicate.GetField2())
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
func (s *SortMergeJoin) processEqualKeys() error {
	leftTuple := s.leftSorted[s.leftIndex]
	leftField, _ := leftTuple.GetField(s.predicate.GetField1())

	rightStart := s.rightIndex

	for s.rightIndex < len(s.rightSorted) {
		rightTuple := s.rightSorted[s.rightIndex]
		rightField, err := rightTuple.GetField(s.predicate.GetField2())
		if err != nil || rightField == nil {
			s.rightIndex++
			continue
		}

		equals, err := leftField.Compare(types.Equals, rightField)
		if err != nil || !equals {
			break
		}

		joinedTuple, err := tuple.CombineTuples(leftTuple, rightTuple)
		if err == nil {
			s.matchBuffer.Add(joinedTuple)
		}

		s.rightIndex++
	}

	s.rightIndex = rightStart
	s.leftIndex++
	return nil
}
