package algorithm

import (
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// NESTED LOOP JOIN TESTS
// ============================================================================

// TestNewNestedLoopJoin tests the NestedLoopJoin constructor
func TestNewNestedLoopJoin(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)

	tests := []struct {
		name              string
		stats             *common.JoinStatistics
		expectedBlockSize int
	}{
		{
			name:              "nil stats",
			stats:             nil,
			expectedBlockSize: 100, // Default block size
		},
		{
			name: "with stats memory size",
			stats: &common.JoinStatistics{
				MemorySize: 5,
			},
			expectedBlockSize: 500, // 5 * 100
		},
		{
			name: "zero memory size",
			stats: &common.JoinStatistics{
				MemorySize: 0,
			},
			expectedBlockSize: 100, // Default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nl := NewNestedLoopJoin(leftChild, rightChild, predicate, tt.stats)

			if nl == nil {
				t.Fatal("expected non-nil NestedLoopJoin")
			}
			if nl.LeftChildField != leftChild {
				t.Error("left child not set correctly")
			}
			if nl.RightChildField != rightChild {
				t.Error("right child not set correctly")
			}
			if nl.PredicateField != predicate {
				t.Error("predicate not set correctly")
			}
			if nl.blockSize != tt.expectedBlockSize {
				t.Errorf("expected block size %d, got %d", tt.expectedBlockSize, nl.blockSize)
			}
			if nl.blockIndex != 0 {
				t.Error("blockIndex should be initialized to 0")
			}
			if nl.MatchBufferField == nil {
				t.Error("matchBuffer should be initialized")
			}
			if nl.Initialized {
				t.Error("initialized should be false")
			}
		})
	}
}

// TestNestedLoopJoinInitialize tests the Initialize method
func TestNestedLoopJoinInitialize(t *testing.T) {
	tests := []struct {
		name               string
		leftTuples         []*tuple.Tuple
		expectedBlockSize  int
		expectedInitialize bool
	}{
		{
			name: "basic initialization",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(2)},
				),
			},
			expectedBlockSize:  2,
			expectedInitialize: true,
		},
		{
			name:               "empty left side",
			leftTuples:         []*tuple.Tuple{},
			expectedBlockSize:  0,
			expectedInitialize: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
			rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

			leftChild := newMockIterator(tt.leftTuples, leftTupleDesc)
			rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
			leftChild.Open()
			rightChild.Open()

			predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
			nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

			err := nl.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			if nl.Initialized != tt.expectedInitialize {
				t.Errorf("expected initialized=%v, got %v", tt.expectedInitialize, nl.Initialized)
			}

			if len(nl.leftBlock) != tt.expectedBlockSize {
				t.Errorf("expected block size %d, got %d", tt.expectedBlockSize, len(nl.leftBlock))
			}
		})
	}
}

// TestNestedLoopJoinInitializeIdempotent tests that Initialize can be called multiple times
func TestNestedLoopJoinInitializeIdempotent(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	// First initialization
	err := nl.Initialize()
	if err != nil {
		t.Fatalf("first Initialize failed: %v", err)
	}

	// Second initialization should not error
	err = nl.Initialize()
	if err != nil {
		t.Fatalf("second Initialize failed: %v", err)
	}

	if !nl.Initialized {
		t.Error("should remain initialized")
	}
}

// TestNestedLoopJoinNext tests the Next method
func TestNestedLoopJoinNext(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "dept"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "Alice"}),
		createJoinTestTuple(leftTupleDesc, []any{int32(2), "Bob"}),
		createJoinTestTuple(leftTupleDesc, []any{int32(3), "Charlie"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1), "Engineering"}),
		createJoinTestTuple(rightTupleDesc, []any{int32(2), "Marketing"}),
		createJoinTestTuple(rightTupleDesc, []any{int32(4), "Sales"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
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

// TestNestedLoopJoinNextBeforeInitialize tests calling Next before Initialize
func TestNestedLoopJoinNextBeforeInitialize(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	// Try to call Next without Initialize
	_, err := nl.Next()
	if err == nil {
		t.Error("expected error when calling Next before Initialize")
	}
}

// TestNestedLoopJoinMultipleBlocks tests join with multiple blocks
func TestNestedLoopJoinMultipleBlocks(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Create more tuples than block size to force multiple blocks
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(2)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(3)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
		createJoinTestTuple(rightTupleDesc, []any{int32(2)}),
		createJoinTestTuple(rightTupleDesc, []any{int32(3)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	// Set small block size to force multiple blocks
	stats := &common.JoinStatistics{MemorySize: 0} // Will use default 100
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, stats)
	nl.blockSize = 2 // Override to force multiple blocks

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 3 matches (all ids match)
	if len(results) != 3 {
		t.Errorf("expected 3 results with multiple blocks, got %d", len(results))
	}
}

// TestNestedLoopJoinNonEquality tests joins with non-equality predicates
func TestNestedLoopJoinNonEquality(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"score"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"threshold"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(85)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(90)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(75)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(80)}),
		createJoinTestTuple(rightTupleDesc, []any{int32(95)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	// Greater than predicate
	predicate, _ := common.NewJoinPredicate(0, 0, primitives.GreaterThan)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 2 results: (85 > 80) and (90 > 80)
	expectedResults := 2
	if len(results) != expectedResults {
		t.Errorf("expected %d results for greater than join, got %d", expectedResults, len(results))
	}
}

// TestNestedLoopJoinMultipleMatches tests one-to-many joins
func TestNestedLoopJoinMultipleMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "emp_name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "dept_name"})

	// Multiple employees with same department
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "Alice"}),
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "Bob"}),
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "Charlie"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1), "Engineering"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
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

// TestNestedLoopJoinManyToMany tests many-to-many joins
func TestNestedLoopJoinManyToMany(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "left_val"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "right_val"})

	// Multiple tuples with same key on both sides
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "L1"}),
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "L2"}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1), "R1"}),
		createJoinTestTuple(rightTupleDesc, []any{int32(1), "R2"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 4 results (2x2 cartesian product)
	if len(results) != 4 {
		t.Errorf("expected 4 results for many-to-many join, got %d", len(results))
	}
}

// TestNestedLoopJoinNoMatches tests when there are no matching tuples
func TestNestedLoopJoinNoMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(2)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(3)}),
		createJoinTestTuple(rightTupleDesc, []any{int32(4)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Should return nil immediately
	result, err := nl.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when there are no matches")
	}
}

// TestNestedLoopJoinEmptyInputs tests nested loop join with empty inputs
func TestNestedLoopJoinEmptyInputs(t *testing.T) {
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
					[]any{int32(1)},
				),
			},
		},
		{
			name: "right empty",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
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

			predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
			nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

			err := nl.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			result, err := nl.Next()
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			if result != nil {
				t.Error("expected nil result for empty inputs")
			}
		})
	}
}

// TestNestedLoopJoinWithNullFields tests nested loop join behavior with null fields
func TestNestedLoopJoinWithNullFields(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "dept"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1), "Alice"}),
		tuple.NewTuple(leftTupleDesc), // Tuple with null fields
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1), "Engineering"}),
		tuple.NewTuple(rightTupleDesc), // Tuple with null fields
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
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

// TestNestedLoopJoinReset tests the Reset method
func TestNestedLoopJoinReset(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(2)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
		createJoinTestTuple(rightTupleDesc, []any{int32(2)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Consume first result
	result, err := nl.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected at least one result")
	}

	// Reset
	err = nl.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify internal state is reset
	if nl.blockIndex != 0 {
		t.Error("blockIndex should be 0 after reset")
	}

	// Should be able to iterate again
	var count int
	for {
		result, err := nl.Next()
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

// TestNestedLoopJoinClose tests the Close method
func TestNestedLoopJoinClose(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Close
	err = nl.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify resources are cleaned up
	if nl.leftBlock != nil {
		t.Error("leftBlock should be nil after close")
	}
	if nl.Initialized {
		t.Error("initialized should be false after close")
	}
}

// TestNestedLoopJoinEstimateCost tests the EstimateCost method
func TestNestedLoopJoinEstimateCost(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)

	tests := []struct {
		name         string
		stats        *common.JoinStatistics
		blockSize    int
		expectedCost float64
	}{
		{
			name:         "nil stats",
			stats:        nil,
			blockSize:    100,
			expectedCost: 1000000,
		},
		{
			name: "with stats",
			stats: &common.JoinStatistics{
				LeftSize:  100,
				RightSize: 200,
			},
			blockSize:    100,
			expectedCost: 100 + 1*200, // leftSize + (leftSize/blockSize) * rightSize
		},
		{
			name: "multiple blocks",
			stats: &common.JoinStatistics{
				LeftSize:  200,
				RightSize: 100,
			},
			blockSize:    50,
			expectedCost: 200 + 4*100, // leftSize + (200/50) * rightSize
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nl := NewNestedLoopJoin(leftChild, rightChild, predicate, tt.stats)
			nl.blockSize = tt.blockSize
			cost := nl.EstimateCost()

			if cost != tt.expectedCost {
				t.Errorf("expected cost %f, got %f", tt.expectedCost, cost)
			}
		})
	}
}

// TestNestedLoopJoinSupportsPredicateType tests the SupportsPredicateType method
func TestNestedLoopJoinSupportsPredicateType(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	// Nested loop join supports all predicate types
	tests := []struct {
		name string
		op   primitives.Predicate
	}{
		{"equals", primitives.Equals},
		{"less than", primitives.LessThan},
		{"greater than", primitives.GreaterThan},
		{"not equals", primitives.NotEqual},
		{"less than or equal", primitives.LessThanOrEqual},
		{"greater than or equal", primitives.GreaterThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, _ := common.NewJoinPredicate(0, 0, tt.op)
			result := nl.SupportsPredicateType(pred)

			if !result {
				t.Errorf("nested loop join should support %s predicate", tt.op)
			}
		})
	}
}

// TestNestedLoopJoinStringKeys tests nested loop join with string keys
func TestNestedLoopJoinStringKeys(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.StringType, types.IntType}, []string{"name", "age"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.StringType, types.StringType}, []string{"name", "city"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{"Alice", int32(30)}),
		createJoinTestTuple(leftTupleDesc, []any{"Bob", int32(25)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{"Alice", "NYC"}),
		createJoinTestTuple(rightTupleDesc, []any{"Charlie", "LA"}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := nl.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if result == nil {
			break
		}
		results = append(results, result)
	}

	// Should have 1 match (Alice)
	if len(results) != 1 {
		t.Errorf("expected 1 result for string key join, got %d", len(results))
	}
}

// TestNestedLoopJoinErrorHandling tests error conditions
func TestNestedLoopJoinErrorHandling(t *testing.T) {
	t.Run("error during left child iteration in Initialize", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
		rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
		leftChild.hasError = true
		leftChild.Open()

		predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
		nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

		err := nl.Initialize()
		if err == nil {
			t.Error("expected error during initialization with failing left child")
		}
	})

	t.Run("error during right child iteration", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		leftTuples := []*tuple.Tuple{
			createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
		}

		leftChild := newMockIterator(leftTuples, leftTupleDesc)
		rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
		rightChild.hasError = true
		leftChild.Open()
		rightChild.Open()

		predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
		nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

		err := nl.Initialize()
		if err != nil {
			t.Fatalf("Initialize should succeed: %v", err)
		}

		_, err = nl.Next()
		if err == nil {
			t.Error("expected error during Next with failing right child")
		}
	})
}

// TestNestedLoopJoinBufferMatches tests that matches are properly buffered
func TestNestedLoopJoinBufferMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Setup data that will produce buffered matches
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	nl := NewNestedLoopJoin(leftChild, rightChild, predicate, nil)

	err := nl.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// First Next should return a result and buffer remaining
	result1, err := nl.Next()
	if err != nil {
		t.Fatalf("First Next failed: %v", err)
	}
	if result1 == nil {
		t.Fatal("expected first result")
	}

	// Second Next should return buffered result
	result2, err := nl.Next()
	if err != nil {
		t.Fatalf("Second Next failed: %v", err)
	}
	if result2 == nil {
		t.Fatal("expected second result from buffer")
	}
}
