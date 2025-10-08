package join

import (
	"fmt"
	"math"
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
	BaseJoin
	leftSorted  []*tuple.Tuple
	rightSorted []*tuple.Tuple
	leftIndex   int
	rightIndex  int
}

// NewSortMergeJoin creates a new sort-merge join operator.
func NewSortMergeJoin(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) *SortMergeJoin {
	return &SortMergeJoin{
		BaseJoin: NewBaseJoin(left, right, pred, stats),
	}
}

// Initialize prepares the sort-merge join for execution.
//
// This method must be called before Next() can be used. It performs:
// 1. Loading all tuples from both input iterators
// 2. Sorting both relations by their respective join keys
// 3. Setting up internal state for the merge phase
func (s *SortMergeJoin) Initialize() error {
	if s.IsInitialized() {
		return nil
	}

	var err error
	s.leftSorted, err = loadAndSort(s.leftChild, s.predicate.GetField1())
	if err != nil {
		return err
	}

	s.rightSorted, err = loadAndSort(s.rightChild, s.predicate.GetField2())
	if err != nil {
		return err
	}

	s.SetInitialized()
	return nil
}

// Next returns the next joined tuple from the sort-merge join.
//
// The algorithm maintains two pointers (leftIndex, rightIndex) that traverse
// the sorted relations. For each comparison:
// - If keys are equal: process all matching tuples (handle duplicates)
// - If left key < right key: advance left pointer
// - If right key < left key: advance right pointer
func (s *SortMergeJoin) Next() (*tuple.Tuple, error) {
	if !s.IsInitialized() {
		return nil, fmt.Errorf("sort-merge join not initialized")
	}

	if match := s.GetMatchFromBuffer(); match != nil {
		return match, nil
	}

	s.matchBuffer.StartNew()

	for s.leftIndex < len(s.leftSorted) && s.rightIndex < len(s.rightSorted) {
		cmp, err := s.compareCurrentTuples()
		if err != nil {
			s.leftIndex++
			continue
		}

		switch cmp {
		case 0:
			if err := s.processEqualKeys(); err != nil {
				return nil, err
			}
			if result := s.matchBuffer.GetFirstAndAdvance(); result != nil {
				return result, nil
			}

		case -1: // Left < Right
			s.leftIndex++

		case 1: // Left > Right
			s.rightIndex++
		}
	}

	return nil, nil
}

// Reset resets the join to its initial state, allowing it to be re-executed.
func (s *SortMergeJoin) Reset() error {
	s.leftIndex = 0
	s.rightIndex = 0
	return s.ResetCommon()
}

// Close releases all resources used by the sort-merge join.
func (s *SortMergeJoin) Close() error {
	s.leftSorted = nil
	s.rightSorted = nil
	return s.BaseJoin.Close()
}

// EstimateCost estimates the cost of executing this sort-merge join.
//
// The cost model includes:
// - Sorting cost: O(n log n) for each unsorted relation
// - Merge cost: O(n + m) for the merge phase
func (s *SortMergeJoin) EstimateCost() float64 {
	if s.stats == nil {
		return DefaultHighCost
	}

	totalCost := 0.0

	if !s.stats.LeftSorted {
		leftSize := float64(s.stats.LeftSize)
		totalCost += leftSize * 2 * math.Log2(leftSize) // 2 * n * log(n) for sorting
	}

	if !s.stats.RightSorted {
		rightSize := float64(s.stats.RightSize)
		totalCost += rightSize * 2 * math.Log2(rightSize) // 2 * m * log(m) for sorting
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

// compareCurrentTuples compares the current tuples pointed to by leftIndex and rightIndex.
func (s *SortMergeJoin) compareCurrentTuples() (int, error) {
	eq, err := compareTuples(s.leftSorted[s.leftIndex], s.rightSorted[s.rightIndex],
		s.predicate.GetField1(), s.predicate.GetField2(), primitives.Equals)
	if err != nil {
		return 0, err
	}
	if eq {
		return 0, nil
	}

	lt, err := compareTuples(s.leftSorted[s.leftIndex], s.rightSorted[s.rightIndex],
		s.predicate.GetField1(), s.predicate.GetField2(), primitives.LessThan)
	if err != nil {
		return 0, err
	}

	if lt {
		return -1, nil
	}
	return 1, nil
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

// LoadAndSort loads all tuples from an iterator and sorts by field index.
func loadAndSort(iter iterator.DbIterator, fieldIndex int) ([]*tuple.Tuple, error) {
	tuples, err := iterator.LoadAllTuples(iter)
	if err != nil {
		return nil, err
	}

	sortTuplesByField(tuples, fieldIndex)
	return tuples, nil
}

// sortTuplesByField sorts tuples by comparing values at the specified field.
func sortTuplesByField(tuples []*tuple.Tuple, fieldIndex int) {
	defer func() {
		if r := recover(); r != nil {
		}
	}()

	sort.Slice(tuples, func(i, j int) bool {
		f1, err1 := tuples[i].GetField(fieldIndex)
		f2, err2 := tuples[j].GetField(fieldIndex)

		if err1 != nil || err2 != nil || f1 == nil || f2 == nil {
			return false
		}

		result, err := f1.Compare(primitives.LessThan, f2)
		return err == nil && result
	})
}

// CompareTuples compares two tuples at specified fields using the given operator.
func compareTuples(t1, t2 *tuple.Tuple, field1, field2 int, op primitives.Predicate) (bool, error) {
	f1, err := t1.GetField(field1)
	if err != nil || f1 == nil {
		return false, fmt.Errorf("invalid field %d in left tuple", field1)
	}

	f2, err := t2.GetField(field2)
	if err != nil || f2 == nil {
		return false, fmt.Errorf("invalid field %d in right tuple", field2)
	}

	return f1.Compare(op, f2)
}
