package join

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// SORT MERGE JOIN TESTS
// ============================================================================

// TestNewSortMergeJoin tests the SortMergeJoin constructor
func TestNewSortMergeJoin(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	stats := &JoinStatistics{
		LeftSize:  10,
		RightSize: 20,
	}

	smj := NewSortMergeJoin(leftChild, rightChild, predicate, stats)

	if smj == nil {
		t.Fatal("expected non-nil SortMergeJoin")
	}
	if smj.leftChild != leftChild {
		t.Error("left child not set correctly")
	}
	if smj.rightChild != rightChild {
		t.Error("right child not set correctly")
	}
	if smj.predicate != predicate {
		t.Error("predicate not set correctly")
	}
	if smj.stats != stats {
		t.Error("stats not set correctly")
	}
	if smj.leftIndex != 0 {
		t.Error("leftIndex should be initialized to 0")
	}
	if smj.rightIndex != 0 {
		t.Error("rightIndex should be initialized to 0")
	}
	if smj.rightStart != 0 {
		t.Error("rightStart should be initialized to 0")
	}
	if smj.matchBuffer == nil {
		t.Error("matchBuffer should be initialized")
	}
	if smj.initialized {
		t.Error("initialized should be false")
	}
}

// TestSortMergeJoinInitialize tests the Initialize method
func TestSortMergeJoinInitialize(t *testing.T) {
	tests := []struct {
		name        string
		leftTuples  []*tuple.Tuple
		rightTuples []*tuple.Tuple
	}{
		{
			name: "basic initialization",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(2)},
				),
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(1)},
				),
			},
			rightTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(3)},
				),
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(1)},
				),
			},
		},
		{
			name:        "empty inputs",
			leftTuples:  []*tuple.Tuple{},
			rightTuples: []*tuple.Tuple{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
			rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

			leftChild := newMockIterator(tt.leftTuples, leftTupleDesc)
			rightChild := newMockIterator(tt.rightTuples, rightTupleDesc)
			leftChild.Open()
			rightChild.Open()

			predicate, _ := NewJoinPredicate(0, 0, query.Equals)
			smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

			err := smj.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			if !smj.initialized {
				t.Error("initialized flag should be true after Initialize")
			}

			// Verify tuples were loaded
			if len(smj.leftSorted) != len(tt.leftTuples) {
				t.Errorf("expected %d left tuples, got %d", len(tt.leftTuples), len(smj.leftSorted))
			}
			if len(smj.rightSorted) != len(tt.rightTuples) {
				t.Errorf("expected %d right tuples, got %d", len(tt.rightTuples), len(smj.rightSorted))
			}
		})
	}
}

// TestSortMergeJoinInitializeIdempotent tests that Initialize can be called multiple times
func TestSortMergeJoinInitializeIdempotent(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
	}
	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	// First initialization
	err := smj.Initialize()
	if err != nil {
		t.Fatalf("first Initialize failed: %v", err)
	}

	// Second initialization should not error and should not reload
	err = smj.Initialize()
	if err != nil {
		t.Fatalf("second Initialize failed: %v", err)
	}

	if !smj.initialized {
		t.Error("should remain initialized")
	}
}

// TestSortMergeJoinSorting tests that tuples are properly sorted
func TestSortMergeJoinSorting(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Create unsorted data
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(3)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(2)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(2)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(3)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Verify left is sorted
	for i := 0; i < len(smj.leftSorted)-1; i++ {
		field1, _ := smj.leftSorted[i].GetField(0)
		field2, _ := smj.leftSorted[i+1].GetField(0)
		less, _ := field1.Compare(types.LessThan, field2)
		equals, _ := field1.Compare(types.Equals, field2)
		if !less && !equals {
			t.Error("left tuples not sorted correctly")
		}
	}

	// Verify right is sorted
	for i := 0; i < len(smj.rightSorted)-1; i++ {
		field1, _ := smj.rightSorted[i].GetField(0)
		field2, _ := smj.rightSorted[i+1].GetField(0)
		less, _ := field1.Compare(types.LessThan, field2)
		equals, _ := field1.Compare(types.Equals, field2)
		if !less && !equals {
			t.Error("right tuples not sorted correctly")
		}
	}
}

// TestSortMergeJoinNext tests the Next method
func TestSortMergeJoinNext(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "dept"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "Alice"}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(2), "Bob"}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(3), "Charlie"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1), "Engineering"}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(2), "Marketing"}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(4), "Sales"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 2 matches (id=1 and id=2)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify joined tuples have correct number of fields
	if len(results) > 0 && results[0].TupleDesc.NumFields() != 4 {
		t.Errorf("expected 4 fields in joined tuple, got %d", results[0].TupleDesc.NumFields())
	}
}

// TestSortMergeJoinNextBeforeInitialize tests calling Next before Initialize
func TestSortMergeJoinNextBeforeInitialize(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	// Try to call Next without Initialize
	_, err := smj.Next()
	if err == nil {
		t.Error("expected error when calling Next before Initialize")
	}
}

// TestSortMergeJoinDuplicates tests handling of duplicate join keys
func TestSortMergeJoinDuplicates(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "left_val"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "right_val"})

	// Multiple tuples with same key on both sides
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "L1"}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "L2"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1), "R1"}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1), "R2"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 4 results (2x2 cartesian product for matching keys)
	if len(results) != 4 {
		t.Errorf("expected 4 results for duplicates, got %d", len(results))
	}
}

// TestSortMergeJoinOneToMany tests one-to-many joins
func TestSortMergeJoinOneToMany(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "emp_name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "dept_name"})

	// Multiple employees with same department
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "Alice"}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "Bob"}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "Charlie"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1), "Engineering"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 3 results (one for each employee)
	if len(results) != 3 {
		t.Errorf("expected 3 results for one-to-many join, got %d", len(results))
	}
}

// TestSortMergeJoinNoMatches tests when there are no matching tuples
func TestSortMergeJoinNoMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(2)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(3)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(4)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Should return nil immediately
	result, err := smj.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when there are no matches")
	}
}

// TestSortMergeJoinEmptyInputs tests sort merge join with empty inputs
func TestSortMergeJoinEmptyInputs(t *testing.T) {
	tests := []struct {
		name        string
		leftTuples  []*tuple.Tuple
		rightTuples []*tuple.Tuple
	}{
		{
			name:        "both empty",
			leftTuples:  []*tuple.Tuple{},
			rightTuples: []*tuple.Tuple{},
		},
		{
			name:       "left empty",
			leftTuples: []*tuple.Tuple{},
			rightTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(1)},
				),
			},
		},
		{
			name: "right empty",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]interface{}{int32(1)},
				),
			},
			rightTuples: []*tuple.Tuple{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
			rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

			leftChild := newMockIterator(tt.leftTuples, leftTupleDesc)
			rightChild := newMockIterator(tt.rightTuples, rightTupleDesc)
			leftChild.Open()
			rightChild.Open()

			predicate, _ := NewJoinPredicate(0, 0, query.Equals)
			smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

			err := smj.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			result, err := smj.Next()
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			if result != nil {
				t.Error("expected nil result for empty inputs")
			}
		})
	}
}

// TestSortMergeJoinWithNullFields tests sort merge join behavior with null fields
func TestSortMergeJoinWithNullFields(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "dept"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1), "Alice"}),
		tuple.NewTuple(leftTupleDesc), // Tuple with null fields
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1), "Engineering"}),
		tuple.NewTuple(rightTupleDesc), // Tuple with null fields
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should only match non-null tuples
	if len(results) != 1 {
		t.Errorf("expected 1 result (null fields should be skipped), got %d", len(results))
	}
}

// TestSortMergeJoinReset tests the Reset method
func TestSortMergeJoinReset(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(2)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(2)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Consume first result
	result, err := smj.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected at least one result")
	}

	// Reset
	err = smj.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify internal state is reset
	if smj.leftIndex != 0 {
		t.Error("leftIndex should be 0 after reset")
	}
	if smj.rightIndex != 0 {
		t.Error("rightIndex should be 0 after reset")
	}
	if smj.rightStart != 0 {
		t.Error("rightStart should be 0 after reset")
	}

	// Should be able to iterate again
	var count int
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next after reset failed: %v", err)
		}
		if result == nil {
			break
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 results after reset, got %d", count)
	}
}

// TestSortMergeJoinClose tests the Close method
func TestSortMergeJoinClose(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Close
	err = smj.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify resources are cleaned up
	if smj.leftSorted != nil {
		t.Error("leftSorted should be nil after close")
	}
	if smj.rightSorted != nil {
		t.Error("rightSorted should be nil after close")
	}
	if smj.initialized {
		t.Error("initialized should be false after close")
	}
}

// TestSortMergeJoinEstimateCost tests the EstimateCost method
func TestSortMergeJoinEstimateCost(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)

	tests := []struct {
		name         string
		stats        *JoinStatistics
		expectedCost float64
	}{
		{
			name:         "nil stats",
			stats:        nil,
			expectedCost: 1000000,
		},
		{
			name: "both sorted",
			stats: &JoinStatistics{
				LeftSize:    100,
				RightSize:   200,
				LeftSorted:  true,
				RightSorted: true,
			},
			expectedCost: 300, // Just merge cost: 100 + 200
		},
		{
			name: "neither sorted",
			stats: &JoinStatistics{
				LeftSize:    8,
				RightSize:   4,
				LeftSorted:  false,
				RightSorted: false,
			},
			// Sort cost for left: 8 * 2 * log2(8) = 8 * 2 * 3 = 48
			// Sort cost for right: 4 * 2 * log2(4) = 4 * 2 * 2 = 16
			// Merge cost: 8 + 4 = 12
			// Total: 48 + 16 + 12 = 76
			expectedCost: 76,
		},
		{
			name: "left sorted only",
			stats: &JoinStatistics{
				LeftSize:    10,
				RightSize:   8,
				LeftSorted:  true,
				RightSorted: false,
			},
			// Sort cost for right: 8 * 2 * log2(8) = 8 * 2 * 3 = 48
			// Merge cost: 10 + 8 = 18
			// Total: 48 + 18 = 66
			expectedCost: 66,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			smj := NewSortMergeJoin(leftChild, rightChild, predicate, tt.stats)
			cost := smj.EstimateCost()

			if cost != tt.expectedCost {
				t.Errorf("expected cost %f, got %f", tt.expectedCost, cost)
			}
		})
	}
}

// TestSortMergeJoinSupportsPredicateType tests the SupportsPredicateType method
func TestSortMergeJoinSupportsPredicateType(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	tests := []struct {
		name           string
		op             query.PredicateOp
		expectedResult bool
	}{
		{
			name:           "supports equals",
			op:             query.Equals,
			expectedResult: true,
		},
		{
			name:           "supports less than",
			op:             query.LessThan,
			expectedResult: true,
		},
		{
			name:           "supports greater than",
			op:             query.GreaterThan,
			expectedResult: true,
		},
		{
			name:           "supports less than or equal",
			op:             query.LessThanOrEqual,
			expectedResult: true,
		},
		{
			name:           "supports greater than or equal",
			op:             query.GreaterThanOrEqual,
			expectedResult: true,
		},
		{
			name:           "does not support not equals",
			op:             query.NotEqual,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, _ := NewJoinPredicate(0, 0, tt.op)
			result := smj.SupportsPredicateType(pred)

			if result != tt.expectedResult {
				t.Errorf("expected %v for %s, got %v", tt.expectedResult, tt.op, result)
			}
		})
	}
}

// TestSortMergeJoinStringKeys tests sort merge join with string keys
func TestSortMergeJoinStringKeys(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.StringType, types.IntType}, []string{"name", "age"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.StringType, types.StringType}, []string{"name", "city"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{"Bob", int32(25)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{"Alice", int32(30)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{"Charlie", int32(35)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{"Charlie", "LA"}),
		createJoinTestTuple(rightTupleDesc, []interface{}{"Alice", "NYC"}),
		createJoinTestTuple(rightTupleDesc, []interface{}{"David", "SF"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := smj.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 2 matches (Alice and Charlie)
	if len(results) != 2 {
		t.Errorf("expected 2 results for string key join, got %d", len(results))
	}
}

// TestSortMergeJoinErrorHandling tests error conditions
func TestSortMergeJoinErrorHandling(t *testing.T) {
	t.Run("error during left child iteration", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
		rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
		leftChild.hasError = true
		leftChild.Open()

		predicate, _ := NewJoinPredicate(0, 0, query.Equals)
		smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

		err := smj.Initialize()
		if err == nil {
			t.Error("expected error during initialization with failing left child")
		}
	})

	t.Run("error during right child iteration", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
		rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
		rightChild.hasError = true
		leftChild.Open()
		rightChild.Open()

		predicate, _ := NewJoinPredicate(0, 0, query.Equals)
		smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

		err := smj.Initialize()
		if err == nil {
			t.Error("expected error during initialization with failing right child")
		}
	})
}

// TestSortMergeJoinBufferMatches tests that matches are properly buffered
func TestSortMergeJoinBufferMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Setup data that will produce buffered matches
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	smj := NewSortMergeJoin(leftChild, rightChild, predicate, nil)

	err := smj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// First Next should return a result and buffer remaining
	result1, err := smj.Next()
	if err != nil {
		t.Fatalf("First Next failed: %v", err)
	}
	if result1 == nil {
		t.Fatal("expected first result")
	}

	// Second Next should return buffered result
	result2, err := smj.Next()
	if err != nil {
		t.Fatalf("Second Next failed: %v", err)
	}
	if result2 == nil {
		t.Fatal("expected second result from buffer")
	}
}

// TestLogBase2 tests the logBase2 helper function
func TestLogBase2(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{4, 2},
		{8, 3},
		{16, 4},
		{32, 5},
	}

	for _, tt := range tests {
		result := logBase2(tt.input)
		if result != tt.expected {
			t.Errorf("logBase2(%f) = %f, expected %f", tt.input, result, tt.expected)
		}
	}
}
