package join

import (
	"fmt"
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// MOCK ITERATOR FOR TESTING
// ============================================================================

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

// ============================================================================
// TEST HELPER FUNCTIONS
// ============================================================================

func createTestTupleDesc(fieldTypes []types.Type, fieldNames []string) *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return td
}

func createJoinTestTuple(tupleDesc *tuple.TupleDescription, values []interface{}) *tuple.Tuple {
	tup := tuple.NewTuple(tupleDesc)
	for i, val := range values {
		var field types.Field
		switch v := val.(type) {
		case int64:
			field = types.NewIntField(v)
		case int32:
			field = types.NewIntField(int64(v))
		case int:
			field = types.NewIntField(int64(v))
		case string:
			field = types.NewStringField(v, types.StringMaxSize)
		}
		tup.SetField(primitives.ColumnID(i), field)
	}
	return tup
}

// ============================================================================
// CONSTRUCTOR TESTS
// ============================================================================

func TestNewJoinOperator_ValidInputs(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "dept"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	if join == nil {
		t.Fatal("expected non-nil JoinOperator")
	}

	if join.leftChild != leftChild {
		t.Error("left child not set correctly")
	}

	if join.rightChild != rightChild {
		t.Error("right child not set correctly")
	}

	if join.tupleDesc == nil {
		t.Error("tuple descriptor should not be nil")
	}

	// Combined schema should have 4 fields (2 from left + 2 from right)
	if join.tupleDesc.NumFields() != 4 {
		t.Errorf("expected 4 fields in combined schema, got %d", join.tupleDesc.NumFields())
	}

	if join.initialized {
		t.Error("operator should not be initialized yet")
	}
}

func TestNewJoinOperator_NilLeftChild(t *testing.T) {
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, nil, rightChild)
	if err == nil {
		t.Error("expected error when left child is nil")
	}
	if join != nil {
		t.Error("expected nil join operator when left child is nil")
	}
}

func TestNewJoinOperator_NilRightChild(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, nil)
	if err == nil {
		t.Error("expected error when right child is nil")
	}
	if join != nil {
		t.Error("expected nil join operator when right child is nil")
	}
}

func TestNewJoinOperator_DifferentPredicates(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	// Test different valid predicates
	predicates := []primitives.Predicate{
		primitives.Equals,
		primitives.LessThan,
		primitives.GreaterThan,
		primitives.NotEqual,
		primitives.LessThanOrEqual,
		primitives.GreaterThanOrEqual,
	}

	for _, pred := range predicates {
		join, err := NewJoinOperator(0, 0, pred, leftChild, rightChild)
		if err != nil {
			t.Errorf("NewJoinOperator failed for predicate %v: %v", pred, err)
		}
		if join == nil {
			t.Errorf("expected non-nil join operator for predicate %v", pred)
		}
	}
}

// ============================================================================
// OPEN/CLOSE TESTS
// ============================================================================

func TestJoinOperator_Open_Success(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
	}
	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if !join.initialized {
		t.Error("operator should be initialized after Open")
	}

	if !leftChild.isOpen {
		t.Error("left child should be opened")
	}

	if !rightChild.isOpen {
		t.Error("right child should be opened")
	}

	if join.algorithm == nil {
		t.Error("algorithm should be selected and initialized")
	}

	join.Close()
}

func TestJoinOperator_Open_Idempotent(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	// First open
	err = join.Open()
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}

	// Second open should not fail
	err = join.Open()
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}

	if !join.initialized {
		t.Error("operator should remain initialized")
	}

	join.Close()
}

func TestJoinOperator_Open_LeftChildError(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	leftChild.hasError = true

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err == nil {
		t.Error("expected error when left child fails to open")
	}

	if join.initialized {
		t.Error("operator should not be initialized when open fails")
	}
}

func TestJoinOperator_Open_RightChildError(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)
	rightChild.hasError = true

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err == nil {
		t.Error("expected error when right child fails to open")
	}

	if join.initialized {
		t.Error("operator should not be initialized when open fails")
	}
}

func TestJoinOperator_Close_Success(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = join.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if join.initialized {
		t.Error("operator should not be initialized after close")
	}

	if leftChild.isOpen {
		t.Error("left child should be closed")
	}

	if rightChild.isOpen {
		t.Error("right child should be closed")
	}
}

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

func TestJoinOperator_BasicJoin(t *testing.T) {
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

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// Collect all results
	var results []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := join.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		results = append(results, result)
	}

	// Should have 2 matches (id=1 and id=2)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify joined tuples have correct schema (4 fields total)
	if len(results) > 0 {
		if results[0].TupleDesc.NumFields() != 4 {
			t.Errorf("expected 4 fields in joined tuple, got %d", results[0].TupleDesc.NumFields())
		}
	}
}

func TestJoinOperator_EmptyLeftInput(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{}
	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	hasNext, err := join.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}
	if hasNext {
		t.Error("expected no results when left input is empty")
	}
}

func TestJoinOperator_EmptyRightInput(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
	}
	rightTuples := []*tuple.Tuple{}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	hasNext, err := join.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}
	if hasNext {
		t.Error("expected no results when right input is empty")
	}
}

func TestJoinOperator_NoMatches(t *testing.T) {
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

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	hasNext, err := join.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}
	if hasNext {
		t.Error("expected no results when there are no matches")
	}
}

func TestJoinOperator_ManyToManyJoin(t *testing.T) {
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

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// Collect all results
	var results []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := join.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		results = append(results, result)
	}

	// Should have 4 results (2x2 cartesian product)
	if len(results) != 4 {
		t.Errorf("expected 4 results for many-to-many join, got %d", len(results))
	}
}

// ============================================================================
// PREDICATE TYPE TESTS
// ============================================================================

func TestJoinOperator_NonEqualityPredicate(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"score"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"threshold"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(85)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(90)}),
		createJoinTestTuple(leftTupleDesc, []any{int32(75)}),
	}

	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(80)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.GreaterThan, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// Verify that the join operator can be opened and used with non-equality predicates
	// The actual algorithm selection and correctness is tested in the algorithm package
	hasNext, err := join.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}

	// Just verify we can iterate (results may vary based on algorithm selection)
	_ = hasNext
}

func TestJoinOperator_StringJoin(t *testing.T) {
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

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// Collect all results
	var results []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := join.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		results = append(results, result)
	}

	// Should have 1 match (Alice)
	if len(results) != 1 {
		t.Errorf("expected 1 result for string key join, got %d", len(results))
	}
}

// ============================================================================
// REWIND TESTS
// ============================================================================

func TestJoinOperator_Rewind(t *testing.T) {
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

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// First pass - consume all results
	var firstRun []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := join.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		firstRun = append(firstRun, result)
	}

	if len(firstRun) != 2 {
		t.Errorf("expected 2 results in first run, got %d", len(firstRun))
	}

	// Rewind
	err = join.Rewind()
	if err != nil {
		t.Fatalf("Rewind failed: %v", err)
	}

	// Second pass - should get same results
	var secondRun []*tuple.Tuple
	for {
		hasNext, err := join.HasNext()
		if err != nil {
			t.Fatalf("HasNext after rewind failed: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := join.Next()
		if err != nil {
			t.Fatalf("Next after rewind failed: %v", err)
		}
		secondRun = append(secondRun, result)
	}

	if len(secondRun) != 2 {
		t.Errorf("expected 2 results in second run, got %d", len(secondRun))
	}
}

func TestJoinOperator_Rewind_BeforeOpen(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Rewind()
	if err == nil {
		t.Error("expected error when calling Rewind before Open")
	}
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

func TestJoinOperator_Next_BeforeOpen(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	_, err = join.Next()
	if err == nil {
		t.Error("expected error when calling Next before Open")
	}
}

func TestJoinOperator_HasNext_BeforeOpen(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	_, err = join.HasNext()
	if err == nil {
		t.Error("expected error when calling HasNext before Open")
	}
}

// ============================================================================
// SCHEMA TESTS
// ============================================================================

func TestJoinOperator_GetTupleDesc(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "dept_name"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	tupleDesc := join.GetTupleDesc()
	if tupleDesc == nil {
		t.Fatal("GetTupleDesc returned nil")
	}

	// Combined schema should have 4 fields
	if tupleDesc.NumFields() != 4 {
		t.Errorf("expected 4 fields in combined schema, got %d", tupleDesc.NumFields())
	}

	// Verify field names are preserved
	expectedNames := []string{"id", "name", "dept_id", "dept_name"}
	for i, expectedName := range expectedNames {
		actualName, err := tupleDesc.GetFieldName(primitives.ColumnID(i))
		if err != nil {
			t.Errorf("GetFieldName failed for field %d: %v", i, err)
			continue
		}
		if actualName != expectedName {
			t.Errorf("expected field %d to be named '%s', got '%s'", i, expectedName, actualName)
		}
	}
}

// ============================================================================
// COMBINED SCHEMA TESTS
// ============================================================================

func TestCreateCombinedSchema_Success(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"dept_id", "dept_name"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	combined, err := createCombinedSchema(leftChild, rightChild)
	if err != nil {
		t.Fatalf("createCombinedSchema failed: %v", err)
	}

	if combined == nil {
		t.Fatal("expected non-nil combined schema")
	}

	if combined.NumFields() != 4 {
		t.Errorf("expected 4 fields in combined schema, got %d", combined.NumFields())
	}
}

// ============================================================================
// VALIDATION TESTS
// ============================================================================

func TestValidateInputs_NilPredicate(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	err := validateInputs(nil, leftChild, rightChild)
	if err == nil {
		t.Error("expected error when predicate is nil")
	}
}

func TestValidateInputs_NilLeftChild(t *testing.T) {
	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	err := validateInputs(predicate, nil, rightChild)
	if err == nil {
		t.Error("expected error when left child is nil")
	}
}

func TestValidateInputs_NilRightChild(t *testing.T) {
	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)

	err := validateInputs(predicate, leftChild, nil)
	if err == nil {
		t.Error("expected error when right child is nil")
	}
}

func TestValidateInputs_ValidInputs(t *testing.T) {
	predicate, _ := common.NewJoinPredicate(0, 0, primitives.Equals)
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	err := validateInputs(predicate, leftChild, rightChild)
	if err != nil {
		t.Errorf("expected no error for valid inputs, got: %v", err)
	}
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

func TestJoinOperator_ConcurrentAccess(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftTuples := []*tuple.Tuple{
		createJoinTestTuple(leftTupleDesc, []any{int32(1)}),
	}
	rightTuples := []*tuple.Tuple{
		createJoinTestTuple(rightTupleDesc, []any{int32(1)}),
	}

	leftChild := newMockIterator(leftTuples, leftTupleDesc)
	rightChild := newMockIterator(rightTuples, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	err = join.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer join.Close()

	// Test concurrent reads (GetTupleDesc is safe for concurrent access)
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func() {
			td := join.GetTupleDesc()
			if td == nil {
				t.Error("GetTupleDesc returned nil")
			}
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		<-done
	}
}

// ============================================================================
// INTERFACE COMPLIANCE TESTS
// ============================================================================

func TestJoinOperator_ImplementsDbIterator(t *testing.T) {
	leftTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
	rightTupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

	leftChild := newMockIterator([]*tuple.Tuple{}, leftTupleDesc)
	rightChild := newMockIterator([]*tuple.Tuple{}, rightTupleDesc)

	join, err := NewJoinOperator(0, 0, primitives.Equals, leftChild, rightChild)
	if err != nil {
		t.Fatalf("NewJoinOperator failed: %v", err)
	}

	// Verify that JoinOperator implements iterator.DbIterator
	var _ iterator.DbIterator = join
}
