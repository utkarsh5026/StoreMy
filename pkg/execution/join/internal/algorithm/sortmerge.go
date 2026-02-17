package algorithm

import (
	"fmt"
	"math"
	"sort"
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
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
	common.BaseJoin
	leftIterator  *iterator.SliceIterator[*tuple.Tuple]
	rightIterator *iterator.SliceIterator[*tuple.Tuple]
}

// NewSortMergeJoin creates a new sort-merge join operator.
func NewSortMergeJoin(left, right iterator.DbIterator, pred common.JoinPredicate, stats *common.JoinStatistics) *SortMergeJoin {
	return &SortMergeJoin{
		BaseJoin: common.NewBaseJoin(left, right, pred, stats),
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
	leftSorted, err := loadAndSort(s.LeftChild(), s.Predicate().GetLeftField())
	if err != nil {
		return err
	}
	s.leftIterator = iterator.NewSliceIterator(leftSorted)

	rightSorted, err := loadAndSort(s.RightChild(), s.Predicate().GetRightField())
	if err != nil {
		return err
	}
	s.rightIterator = iterator.NewSliceIterator(rightSorted)

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

	s.MatchBuffer().StartNew()

	for s.leftIterator.HasNext() && s.rightIterator.HasNext() {
		cmp, err := s.compareCurrentTuples()
		if err != nil {
			_, _ = s.leftIterator.Next()
			continue
		}

		switch cmp {
		case 0:
			if err := s.processEqualKeys(); err != nil {
				return nil, err
			}
			if result := s.MatchBuffer().GetFirstAndAdvance(); result != nil {
				return result, nil
			}

		case -1: // Left < Right
			_, _ = s.leftIterator.Next()

		case 1: // Left > Right
			_, _ = s.rightIterator.Next()
		}
	}

	return nil, nil
}

// Reset resets the join to its initial state, allowing it to be re-executed.
func (s *SortMergeJoin) Reset() error {
	if s.leftIterator != nil {
		if err := s.leftIterator.Rewind(); err != nil {
			return err
		}
	}
	if s.rightIterator != nil {
		if err := s.rightIterator.Rewind(); err != nil {
			return err
		}
	}
	return s.ResetCommon()
}

// Close releases all resources used by the sort-merge join.
func (s *SortMergeJoin) Close() error {
	// Release iterator data for GC
	s.leftIterator = nil
	s.rightIterator = nil
	return s.BaseJoin.Close()
}

// EstimateCost estimates the cost of executing this sort-merge join.
//
// The cost model includes:
// - Sorting cost: O(n log n) for each unsorted relation
// - Merge cost: O(n + m) for the merge phase
func (s *SortMergeJoin) EstimateCost() float64 {
	stats := s.Stats()
	if stats == nil {
		return common.DefaultHighCost
	}

	totalCost := 0.0

	if !stats.LeftSorted {
		leftSize := float64(stats.LeftSize)
		totalCost += leftSize * 2 * math.Log2(leftSize) // 2 * n * log(n) for sorting
	}

	if !stats.RightSorted {
		rightSize := float64(stats.RightSize)
		totalCost += rightSize * 2 * math.Log2(rightSize) // 2 * m * log(m) for sorting
	}

	mergeCost := float64(stats.LeftSize + stats.RightSize)
	totalCost += mergeCost
	return totalCost
}

// SupportsPredicateType checks if the sort-merge join supports the given predicate type.
//
// Sort-merge join supports all comparison operators because it works on sorted data.
func (s *SortMergeJoin) SupportsPredicateType(predicate common.JoinPredicate) bool {
	op := predicate.GetOP()
	return op == primitives.Equals || op == primitives.LessThan ||
		op == primitives.LessThanOrEqual || op == primitives.GreaterThan ||
		op == primitives.GreaterThanOrEqual
}

// compareCurrentTuples compares the current tuples from both iterators.
func (s *SortMergeJoin) compareCurrentTuples() (int, error) {
	left, err := s.leftIterator.Peek()
	if err != nil {
		return 0, err
	}

	right, err := s.rightIterator.Peek()
	if err != nil {
		return 0, err
	}

	lf, rf := s.Predicate().GetLeftField(), s.Predicate().GetRightField()

	eq, err := compareTuples(left, right, lf, rf, primitives.Equals)
	if err != nil {
		return 0, err
	}
	if eq {
		return 0, nil
	}

	lt, err := compareTuples(left, right, lf, rf, primitives.LessThan)
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
	leftTuple, err := s.leftIterator.Peek()
	if err != nil {
		return err
	}
	leftField, _ := leftTuple.GetField(s.Predicate().GetLeftField())

	rightStart := s.rightIterator.CurrentIndex()

	for s.rightIterator.HasNext() {
		rt, err := s.rightIterator.Peek()
		if err != nil {
			_, _ = s.rightIterator.Next()
			continue
		}

		rf, err := rt.GetField(s.Predicate().GetRightField())
		if err != nil || rf == nil {
			_, _ = s.rightIterator.Next()
			continue
		}

		equals, err := leftField.Compare(primitives.Equals, rf)
		if err != nil || !equals {
			break
		}

		combined, err := tuple.CombineTuples(leftTuple, rt)
		if err == nil {
			s.MatchBuffer().Add(combined)
		}

		_, _ = s.rightIterator.Next()
	}

	// Reset right iterator to the saved position
	_ = s.rightIterator.Rewind()
	for i := 0; i < rightStart; i++ {
		_, _ = s.rightIterator.Next()
	}

	_, _ = s.leftIterator.Next()
	return nil
}

// loadAndSort loads all tuples from an iterator and sorts by field index.
func loadAndSort(iter iterator.DbIterator, fieldIndex primitives.ColumnID) ([]*tuple.Tuple, error) {
	tuples, err := iterator.Map(iter, func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})
	if err != nil {
		return nil, err
	}

	sortTuplesByField(tuples, fieldIndex)
	return tuples, nil
}

// sortTuplesByField sorts tuples by comparing values at the specified field.
func sortTuplesByField(tuples []*tuple.Tuple, fieldIndex primitives.ColumnID) {
	defer func() {
		_ = recover()
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

// compareTuples compares two tuples at specified fields using the given operator.
func compareTuples(t1, t2 *tuple.Tuple, field1, field2 primitives.ColumnID, op primitives.Predicate) (bool, error) {
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
