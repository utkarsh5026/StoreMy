package join

import (
	"fmt"
	"storemy/pkg/execution"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// JOIN TESTS
// ============================================================================

// Mock iterator for testing Join operator
type mockIterator struct {
	tuples     []*tuple.Tuple
	tupleDesc  *tuple.TupleDescription
	index      int
	isOpen     bool
	hasError   bool
	rewindable bool
}

func newMockIterator(tuples []*tuple.Tuple, tupleDesc *tuple.TupleDescription) *mockIterator {
	return &mockIterator{
		tuples:     tuples,
		tupleDesc:  tupleDesc,
		index:      -1,
		rewindable: true,
	}
}

func (m *mockIterator) Open() error {
	if m.hasError {
		return fmt.Errorf("mock open error")
	}
	m.isOpen = true
	m.index = -1
	return nil
}

func (m *mockIterator) HasNext() (bool, error) {
	if !m.isOpen {
		return false, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return false, fmt.Errorf("mock has next error")
	}
	return m.index+1 < len(m.tuples), nil
}

func (m *mockIterator) Next() (*tuple.Tuple, error) {
	if !m.isOpen {
		return nil, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return nil, fmt.Errorf("mock next error")
	}
	m.index++
	if m.index >= len(m.tuples) {
		return nil, fmt.Errorf("no more tuples")
	}
	return m.tuples[m.index], nil
}

func (m *mockIterator) Rewind() error {
	if !m.isOpen {
		return fmt.Errorf("iterator not open")
	}
	if !m.rewindable {
		return fmt.Errorf("rewind not supported")
	}
	m.index = -1
	return nil
}

func (m *mockIterator) Close() error {
	m.isOpen = false
	return nil
}

func (m *mockIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

// Helper functions for creating test data

func createTestTupleDesc(fieldTypes []types.Type, fieldNames []string) *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return td
}

func createJoinTestTuple(tupleDesc *tuple.TupleDescription, values []interface{}) *tuple.Tuple {
	tup := tuple.NewTuple(tupleDesc)
	for i, val := range values {
		var field types.Field
		switch v := val.(type) {
		case int32:
			field = types.NewIntField(v)
		case string:
			field = types.NewStringField(v, types.StringMaxSize)
		}
		tup.SetField(i, field)
	}
	return tup
}

// TestNewJoin tests the constructor
func TestNewJoin(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)

	tests := []struct {
		name        string
		predicate   *JoinPredicate
		leftChild   iterator.DbIterator
		rightChild  iterator.DbIterator
		expectError bool
	}{
		{
			name:        "valid join",
			predicate:   predicate,
			leftChild:   leftChild,
			rightChild:  rightChild,
			expectError: false,
		},
		{
			name:        "nil predicate",
			predicate:   nil,
			leftChild:   leftChild,
			rightChild:  rightChild,
			expectError: true,
		},
		{
			name:        "nil left child",
			predicate:   predicate,
			leftChild:   nil,
			rightChild:  rightChild,
			expectError: true,
		},
		{
			name:        "nil right child",
			predicate:   predicate,
			leftChild:   leftChild,
			rightChild:  nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			join, err := NewJoin(tt.predicate, tt.leftChild, tt.rightChild)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				if join != nil {
					t.Errorf("expected nil join on error")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if join == nil {
					t.Errorf("expected non-nil join")
				}
				if join.GetTupleDesc() == nil {
					t.Errorf("expected non-nil tuple descriptor")
				}
			}
		})
	}
}

// TestJoinGetTupleDesc tests the tuple descriptor getter
func TestJoinGetTupleDesc(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"left_id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.StringType}, []string{"right_name"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	tupleDesc := join.GetTupleDesc()
	if tupleDesc == nil {
		t.Errorf("expected non-nil tuple descriptor")
	}

	// The combined descriptor should have fields from both sides
	if tupleDesc.NumFields() != 2 {
		t.Errorf("expected 2 fields, got %d", tupleDesc.NumFields())
	}
}

// TestHashJoinBasic tests basic hash join functionality
func TestHashJoinBasic(t *testing.T) {
	// Create test data
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

	// Create equality predicate for hash join
	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	// Open the join
	if err := join.Open(); err != nil {
		t.Fatalf("failed to open join: %v", err)
	}
	defer join.Close()

	// Collect results
	var results []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		nextTuple, err := join.Next()
		if err != nil {
			t.Fatalf("error getting Next: %v", err)
		}
		results = append(results, nextTuple)
	}

	// Should have 2 matching tuples (id=1 and id=2)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify the joined tuples have correct number of fields
	if len(results) > 0 && results[0].TupleDesc.NumFields() != 4 {
		t.Errorf("expected 4 fields in joined tuple, got %d", results[0].TupleDesc.NumFields())
	}
}

// TestNestedLoopJoin tests nested loop join functionality
func TestNestedLoopJoin(t *testing.T) {
	// Create test data
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"score"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"threshold"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(85)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(90)}),
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(75)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(80)}),
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(95)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	// Create greater than predicate for nested loop join
	predicate, _ := NewJoinPredicate(0, 0, execution.GreaterThan)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	// Open the join
	if err := join.Open(); err != nil {
		t.Fatalf("failed to open join: %v", err)
	}
	defer join.Close()

	// Collect results
	var results []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		nextTuple, err := join.Next()
		if err != nil {
			t.Fatalf("error getting Next: %v", err)
		}
		results = append(results, nextTuple)
	}

	// Should have 2 results: (85 > 80) and (90 > 80)
	expectedResults := 2
	if len(results) != expectedResults {
		t.Errorf("expected %d results, got %d", expectedResults, len(results))
	}
}

// TestJoinRewind tests the rewind functionality
func TestJoinRewind(t *testing.T) {
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

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	// Open and consume some results
	if err := join.Open(); err != nil {
		t.Fatalf("failed to open join: %v", err)
	}
	defer join.Close()

	// Get first result
	hasNext, _ := join.HasNext()
	if !hasNext {
		t.Fatalf("expected at least one result")
	}
	join.Next()

	// Rewind and check if we can get results again
	if err := join.Rewind(); err != nil {
		t.Fatalf("failed to rewind join: %v", err)
	}

	// Count results after rewind
	var count int
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("error checking HasNext after rewind: %v", err)
		}
		if !hasNext {
			break
		}

		_, err = join.Next()
		if err != nil {
			t.Fatalf("error getting Next after rewind: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 results after rewind, got %d", count)
	}
}

// TestJoinEmptyInputs tests join with empty inputs
func TestJoinEmptyInputs(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

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
			name:        "left empty",
			leftTuples:  []*tuple.Tuple{},
			rightTuples: []*tuple.Tuple{createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)})},
		},
		{
			name:        "right empty",
			leftTuples:  []*tuple.Tuple{createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)})},
			rightTuples: []*tuple.Tuple{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftChild := newMockIterator(tt.leftTuples, leftTupleDesc)
			rightChild := newMockIterator(tt.rightTuples, rightTupleDesc)

			predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
			join, err := NewJoin(predicate, leftChild, rightChild)

			if err != nil {
				t.Fatalf("failed to create join: %v", err)
			}

			if err := join.Open(); err != nil {
				t.Fatalf("failed to open join: %v", err)
			}
			defer join.Close()

			// Should have no results
			hasNext, err := join.HasNext()
			if err != nil {
				t.Fatalf("error checking HasNext: %v", err)
			}
			if hasNext {
				t.Errorf("expected no results for empty inputs")
			}
		})
	}
}

// TestJoinWithNullValues tests join behavior with null values
func TestJoinWithNullValues(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Create tuples with null values (empty tuples)
	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []interface{}{int32(1)}),
		tuple.NewTuple(leftTupleDesc), // null tuple
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []interface{}{int32(1)}),
		tuple.NewTuple(rightTupleDesc), // null tuple
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	if err := join.Open(); err != nil {
		t.Fatalf("failed to open join: %v", err)
	}
	defer join.Close()

	// Count results - only non-null pairs should match
	var count int
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		_, err = join.Next()
		if err != nil {
			t.Fatalf("error getting Next: %v", err)
		}
		count++
	}

	// Should only match the non-null pair (1, 1)
	if count != 1 {
		t.Errorf("expected 1 result with null handling, got %d", count)
	}
}

// TestJoinErrorHandling tests error conditions
func TestJoinErrorHandling(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Create iterator that fails on open
	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	leftChild.hasError = true

	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	// Opening should fail
	if err := join.Open(); err == nil {
		t.Errorf("expected error when opening join with failing child")
	}
}

// TestJoinClose tests the close functionality
func TestJoinClose(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	predicate, _ := NewJoinPredicate(0, 0, execution.Equals)
	join, err := NewJoin(predicate, leftChild, rightChild)

	if err != nil {
		t.Fatalf("failed to create join: %v", err)
	}

	if err := join.Open(); err != nil {
		t.Fatalf("failed to open join: %v", err)
	}

	// Close should not error
	if err := join.Close(); err != nil {
		t.Errorf("unexpected error when closing join: %v", err)
	}

	// Operations after close should fail
	hasNext, err := join.HasNext()
	if err == nil {
		t.Errorf("expected error when calling HasNext after close")
	}
	if hasNext {
		t.Errorf("expected false from HasNext after close")
	}
}
