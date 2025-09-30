package join

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// HASH JOIN TESTS
// ============================================================================

// TestNewHashJoin tests the HashJoin constructor
func TestNewHashJoin(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	stats := &JoinStatistics{
		LeftSize:  10,
		RightSize: 20,
	}

	hj := NewHashJoin(leftChild, rightChild, predicate, stats)

	if hj == nil {
		t.Fatal("expected non-nil HashJoin")
	}
	if hj.leftChild != leftChild {
		t.Error("left child not set correctly")
	}
	if hj.rightChild != rightChild {
		t.Error("right child not set correctly")
	}
	if hj.predicate != predicate {
		t.Error("predicate not set correctly")
	}
	if hj.stats != stats {
		t.Error("stats not set correctly")
	}
	if hj.hashTable == nil {
		t.Error("hash table should be initialized")
	}
	if hj.matchBuffer == nil {
		t.Error("matchBuffer should be initialized")
	}
	if hj.initialized {
		t.Error("initialized should be false")
	}
}

// TestHashJoinInitialize tests the Initialize method
func TestHashJoinInitialize(t *testing.T) {
	tests := []struct {
		name         string
		leftTuples   []*tuple.Tuple
		rightTuples  []*tuple.Tuple
		expectedSize int // Expected number of keys in hash table
	}{
		{
			name: "basic initialization",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
			},
			rightTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(2)},
				),
			},
			expectedSize: 2,
		},
		{
			name: "empty right side",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
			},
			rightTuples:  []*tuple.Tuple{},
			expectedSize: 0,
		},
		{
			name: "duplicate keys in hash table",
			leftTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
			},
			rightTuples: []*tuple.Tuple{
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
				createJoinTestTuple(
					createTestTupleDesc([]types.Type{types.IntType}, []string{"id"}),
					[]any{int32(1)},
				),
			},
			expectedSize: 1, // Same key, both tuples should be in the list
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
			hj := NewHashJoin(leftChild, rightChild, predicate, nil)

			err := hj.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			if !hj.initialized {
				t.Error("initialized flag should be true after Initialize")
			}

			if len(hj.hashTable) != tt.expectedSize {
				t.Errorf("expected hash table size %d, got %d", tt.expectedSize, len(hj.hashTable))
			}
		})
	}
}

// TestHashJoinInitializeIdempotent tests that Initialize can be called multiple times
func TestHashJoinInitializeIdempotent(t *testing.T) {
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	// First initialization
	err := hj.Initialize()
	if err != nil {
		t.Fatalf("first Initialize failed: %v", err)
	}

	// Second initialization should not error and should not rebuild
	err = hj.Initialize()
	if err != nil {
		t.Fatalf("second Initialize failed: %v", err)
	}

	if !hj.initialized {
		t.Error("should remain initialized")
	}
}

// TestHashJoinNext tests the Next method
func TestHashJoinNext(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := hj.Next()
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

// TestHashJoinNextBeforeInitialize tests calling Next before Initialize
func TestHashJoinNextBeforeInitialize(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	// Try to call Next without Initialize
	_, err := hj.Next()
	if err == nil {
		t.Error("expected error when calling Next before Initialize")
	}
}

// TestHashJoinMultipleMatches tests one-to-many joins
func TestHashJoinMultipleMatches(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "emp_name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "dept_name"})

	// One department with multiple employees
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := hj.Next()
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

// TestHashJoinManyToMany tests many-to-many joins
func TestHashJoinManyToMany(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := hj.Next()
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

// TestHashJoinNoMatches tests when there are no matching tuples
func TestHashJoinNoMatches(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Should return nil immediately
	result, err := hj.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when there are no matches")
	}
}

// TestHashJoinEmptyInputs tests hash join with empty inputs
func TestHashJoinEmptyInputs(t *testing.T) {
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

			predicate, _ := NewJoinPredicate(0, 0, query.Equals)
			hj := NewHashJoin(leftChild, rightChild, predicate, nil)

			err := hj.Initialize()
			if err != nil {
				t.Fatalf("Initialize failed: %v", err)
			}

			result, err := hj.Next()
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			if result != nil {
				t.Error("expected nil result for empty inputs")
			}
		})
	}
}

// TestHashJoinWithNullFields tests hash join behavior with null fields
func TestHashJoinWithNullFields(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := hj.Next()
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

// TestHashJoinReset tests the Reset method
func TestHashJoinReset(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Consume first result
	result, err := hj.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected at least one result")
	}

	// Reset
	err = hj.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify internal state is reset
	if hj.currentLeft != nil {
		t.Error("currentLeft should be nil after reset")
	}

	// Should be able to iterate again
	var count int
	for {
		result, err := hj.Next()
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

// TestHashJoinClose tests the Close method
func TestHashJoinClose(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	leftChild.Open()
	rightChild.Open()

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Close
	err = hj.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify resources are cleaned up
	if len(hj.hashTable) != 0 {
		t.Error("hash table should be empty after close")
	}
	if hj.currentLeft != nil {
		t.Error("currentLeft should be nil after close")
	}
	if hj.initialized {
		t.Error("initialized should be false after close")
	}
}

// TestHashJoinEstimateCost tests the EstimateCost method
func TestHashJoinEstimateCost(t *testing.T) {
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
			name: "with stats",
			stats: &JoinStatistics{
				LeftSize:  10,
				RightSize: 20,
			},
			expectedCost: 3 * (10 + 20), // 3 * (leftSize + rightSize)
		},
		{
			name: "zero sizes",
			stats: &JoinStatistics{
				LeftSize:  0,
				RightSize: 0,
			},
			expectedCost: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hj := NewHashJoin(leftChild, rightChild, predicate, tt.stats)
			cost := hj.EstimateCost()

			if cost != tt.expectedCost {
				t.Errorf("expected cost %f, got %f", tt.expectedCost, cost)
			}
		})
	}
}

// TestHashJoinSupportsPredicateType tests the SupportsPredicateType method
func TestHashJoinSupportsPredicateType(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

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
			name:           "does not support less than",
			op:             query.LessThan,
			expectedResult: false,
		},
		{
			name:           "does not support greater than",
			op:             query.GreaterThan,
			expectedResult: false,
		},
		{
			name:           "does not support not equals",
			op:             query.NotEqual,
			expectedResult: false,
		},
		{
			name:           "does not support less than or equals",
			op:             query.LessThanOrEqual,
			expectedResult: false,
		},
		{
			name:           "does not support greater than or equals",
			op:             query.GreaterThanOrEqual,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, _ := NewJoinPredicate(0, 0, tt.op)
			result := hj.SupportsPredicateType(pred)

			if result != tt.expectedResult {
				t.Errorf("expected %v for %s, got %v", tt.expectedResult, tt.op, result)
			}
		})
	}
}

// TestHashJoinStringKeys tests hash join with string keys
func TestHashJoinStringKeys(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, query.Equals)
	hj := NewHashJoin(leftChild, rightChild, predicate, nil)

	err := hj.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Collect all results
	var results []*tuple.Tuple
	for {
		result, err := hj.Next()
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

// TestHashJoinErrorHandling tests error conditions
func TestHashJoinErrorHandling(t *testing.T) {
	t.Run("error during right child iteration", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
		rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
		rightChild.hasError = true
		rightChild.Open()

		predicate, _ := NewJoinPredicate(0, 0, query.Equals)
		hj := NewHashJoin(leftChild, rightChild, predicate, nil)

		err := hj.Initialize()
		if err == nil {
			t.Error("expected error during initialization with failing right child")
		}
	})

	t.Run("error during left child iteration", func(t *testing.T) {
		leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

		rightTuples := []*tuple.Tuple{
			createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
		}

		leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
		rightChild := newMockIterator(rightTuples, rightTupleDesc)
		leftChild.hasError = true
		leftChild.Open()
		rightChild.Open()

		predicate, _ := NewJoinPredicate(0, 0, query.Equals)
		hj := NewHashJoin(leftChild, rightChild, predicate, nil)

		err := hj.Initialize()
		if err != nil {
			t.Fatalf("Initialize should succeed: %v", err)
		}

		_, err = hj.Next()
		if err == nil {
			t.Error("expected error during Next with failing left child")
		}
	})
}
