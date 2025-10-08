package join

import (
	"fmt"
	"sort"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

const (
	ComparisonEqual        = "equal"
	ComparisonLeftSmaller  = "leftSmaller"
	ComparisonRightSmaller = "rightSmaller"
)

// SortMergeJoin implements the sort-merge join algorithm.
//
// This join algorithm is particularly efficient for equality joins when:
// - Data is already sorted on join keys
// - The cost of sorting is acceptable relative to the join operation
// - Memory is sufficient to hold sorted data
//
// The algorithm works in three phases:
// 1. Sort both input relations by their join keys
// 2. Merge the sorted relations by advancing pointers
// 3. Handle duplicate keys by creating cross products
//
// Time Complexity: O(n log n + m log m + n + m) where n, m are input sizes
// Space Complexity: O(n + m) for storing sorted tuples
type SortMergeJoin struct {
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	leftSorted  []*tuple.Tuple
	rightSorted []*tuple.Tuple
	leftIndex   int
	rightIndex  int
	rightStart  int
	initialized bool
	stats       *JoinStatistics
	matchBuffer *JoinMatchBuffer
}

// NewSortMergeJoin creates a new sort-merge join operator.
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

// Initialize prepares the sort-merge join for execution.
//
// This method must be called before Next() can be used. It performs:
// 1. Loading all tuples from both input iterators
// 2. Sorting both relations by their respective join keys
// 3. Setting up internal state for the merge phase
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

// loadAndSortLeft loads all tuples from the left iterator and sorts them by the join key.
func (s *SortMergeJoin) loadAndSortLeft() error {
	tuples, err := iterator.LoadAllTuples(s.leftChild)
	if err != nil {
		return err
	}

	s.leftSorted = tuples
	return sortTuples(s.leftSorted, s.predicate.GetField1())
}

// loadAndSortRight loads all tuples from the right iterator and sorts them by the join key.
func (s *SortMergeJoin) loadAndSortRight() error {
	tuples, err := iterator.LoadAllTuples(s.rightChild)
	if err != nil {
		return err
	}

	s.rightSorted = tuples
	return sortTuples(s.rightSorted, s.predicate.GetField2())
}

// Next returns the next joined tuple from the sort-merge join.
//
// The algorithm maintains two pointers (leftIndex, rightIndex) that traverse
// the sorted relations. For each comparison:
// - If keys are equal: process all matching tuples (handle duplicates)
// - If left key < right key: advance left pointer
// - If right key < left key: advance right pointer
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

// Reset resets the join to its initial state, allowing it to be re-executed.
func (s *SortMergeJoin) Reset() error {
	s.leftIndex = 0
	s.rightIndex = 0
	s.rightStart = 0
	s.matchBuffer.Reset()
	return nil
}

// Close releases all resources used by the sort-merge join.
func (s *SortMergeJoin) Close() error {
	s.leftSorted = nil
	s.rightSorted = nil
	s.matchBuffer.Reset()
	s.initialized = false
	return nil
}

// EstimateCost estimates the cost of executing this sort-merge join.
//
// The cost model includes:
// - Sorting cost: O(n log n) for each unsorted relation
// - Merge cost: O(n + m) for the merge phase
//
// Returns:
//   - float64: Estimated cost in abstract units
func (s *SortMergeJoin) EstimateCost() float64 {
	if s.stats == nil {
		return DefaultHighCost
	}

	totalCost := 0.0

	if !s.stats.LeftSorted {
		leftSize := float64(s.stats.LeftSize)
		totalCost += leftSize * 2 * logBase2(leftSize) // 2 * n * log(n) for sorting
	}

	if !s.stats.RightSorted {
		rightSize := float64(s.stats.RightSize)
		totalCost += rightSize * 2 * logBase2(rightSize) // 2 * m * log(m) for sorting
	}

	mergeCost := float64(s.stats.LeftSize + s.stats.RightSize)
	totalCost += mergeCost
	return totalCost
}

// SupportsPredicateType checks if the sort-merge join supports the given predicate type.
//
// Sort-merge join supports all comparison operators because it works on sorted data.
func (s *SortMergeJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	op := predicate.GetOP()
	return op == primitives.Equals || op == primitives.LessThan ||
		op == primitives.LessThanOrEqual || op == primitives.GreaterThan ||
		op == primitives.GreaterThanOrEqual
}

// logBase2 computes the base-2 logarithm of n using integer arithmetic.
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

// sortTuples sorts a slice of tuples by the specified field index.
func sortTuples(tuples []*tuple.Tuple, fieldIndex int) error {
	sort.Slice(tuples, func(i, j int) bool {
		return isLessThan(tuples[i], tuples[j], fieldIndex)
	})
	return nil
}

// isLessThan compares two tuples by a specific field to determine ordering.
func isLessThan(t1, t2 *tuple.Tuple, fieldIndex int) bool {
	field1, err1 := t1.GetField(fieldIndex)
	field2, err2 := t2.GetField(fieldIndex)

	if err1 != nil || err2 != nil || field1 == nil || field2 == nil {
		return false
	}

	result, err := field1.Compare(primitives.LessThan, field2)
	return err == nil && result
}

// compareCurrentTuples compares the current tuples pointed to by leftIndex and rightIndex.
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

	equals, err := leftField.Compare(primitives.Equals, rightField)
	if err != nil {
		return "", err
	}
	if equals {
		return ComparisonEqual, nil
	}

	less, err := leftField.Compare(primitives.LessThan, rightField)
	if err != nil {
		return "", err
	}

	if less {
		return ComparisonLeftSmaller, nil
	}

	return ComparisonRightSmaller, nil
}

// processEqualKeys handles all tuples with equal join keys.
//
// When keys are equal, we need to find all tuples on both sides with the same key
// and create the cross product of matches. This correctly handles duplicate keys
// by ensuring all combinations are generated.
//
// Algorithm:
// 1. Fix the current left tuple
// 2. Find all right tuples with the same key
// 3. Create joined tuples for all combinations
// 4. Buffer all results for subsequent Next() calls
// 5. Advance to next left tuple
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

		equals, err := leftField.Compare(primitives.Equals, rightField)
		if err != nil || !equals {
			break
		}

		combined, err := tuple.CombineTuples(leftTuple, rightTuple)
		if err == nil {
			s.matchBuffer.Add(combined)
		}

		s.rightIndex++
	}

	s.rightIndex = rightStart
	s.leftIndex++
	return nil
}
