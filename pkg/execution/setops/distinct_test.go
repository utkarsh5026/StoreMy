package setops

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// mockDistinctIterator implements DbIterator for testing
type mockDistinctIterator struct {
	tuples    []*tuple.Tuple
	tupleDesc *tuple.TupleDescription
	pos       int
	opened    bool
}

func newMockDistinctIterator(tuples []*tuple.Tuple, desc *tuple.TupleDescription) *mockDistinctIterator {
	return &mockDistinctIterator{
		tuples:    tuples,
		tupleDesc: desc,
		pos:       -1,
		opened:    false,
	}
}

func (m *mockDistinctIterator) Open() error {
	m.opened = true
	m.pos = -1
	return nil
}

func (m *mockDistinctIterator) Close() error {
	m.opened = false
	return nil
}

func (m *mockDistinctIterator) HasNext() (bool, error) {
	if !m.opened {
		return false, nil
	}
	return m.pos+1 < len(m.tuples), nil
}

func (m *mockDistinctIterator) Next() (*tuple.Tuple, error) {
	hasNext, _ := m.HasNext()
	if !hasNext {
		return nil, nil
	}
	m.pos++
	return m.tuples[m.pos], nil
}

func (m *mockDistinctIterator) Rewind() error {
	m.pos = -1
	return nil
}

func (m *mockDistinctIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

// Helper function to create test tuples
func createDistinctTestTuple(desc *tuple.TupleDescription, values ...interface{}) *tuple.Tuple {
	tup := tuple.NewTuple(desc)
	for i, v := range values {
		var field types.Field
		switch val := v.(type) {
		case int:
			field = types.NewIntField(int64(val))
		case string:
			field = types.NewStringField(val, 50)
		}
		tup.SetField(i, field)
	}
	return tup
}

// Helper function to collect all tuples from an iterator
func collectDistinctTuples(iter iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := iter.Open(); err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		t, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if t != nil {
			results = append(results, t)
		}
	}
	return results, nil
}

// TestDistinctBasic tests basic distinct functionality
func TestDistinctBasic(t *testing.T) {
	// Create schema: (id INT, name STRING)
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Input dataset with duplicates: {(1, "Alice"), (2, "Bob"), (1, "Alice"), (3, "Charlie"), (2, "Bob")}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice"),
		createDistinctTestTuple(desc, 2, "Bob"),
		createDistinctTestTuple(desc, 1, "Alice"), // duplicate
		createDistinctTestTuple(desc, 3, "Charlie"),
		createDistinctTestTuple(desc, 2, "Bob"), // duplicate
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 3 unique tuples
	if len(results) != 3 {
		t.Errorf("Expected 3 unique tuples, got %d", len(results))
	}
}

// TestDistinctNoDuplicates tests distinct with no duplicates in input
func TestDistinctNoDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Input dataset with no duplicates
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice"),
		createDistinctTestTuple(desc, 2, "Bob"),
		createDistinctTestTuple(desc, 3, "Charlie"),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: all 3 tuples (no duplicates to remove)
	if len(results) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(results))
	}
}

// TestDistinctAllDuplicates tests distinct when all tuples are identical
func TestDistinctAllDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Input dataset with all duplicates
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice"),
		createDistinctTestTuple(desc, 1, "Alice"),
		createDistinctTestTuple(desc, 1, "Alice"),
		createDistinctTestTuple(desc, 1, "Alice"),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 1 unique tuple
	if len(results) != 1 {
		t.Errorf("Expected 1 unique tuple, got %d", len(results))
	}
}

// TestDistinctSingleColumn tests distinct on single column
func TestDistinctSingleColumn(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Input: {1, 2, 1, 3, 2, 1, 4}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 4),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 4 unique values (1, 2, 3, 4)
	if len(results) != 4 {
		t.Errorf("Expected 4 unique values, got %d", len(results))
	}
}

// TestDistinctMultiColumn tests distinct on multiple columns
func TestDistinctMultiColumn(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType},
		[]string{"id", "name", "age"},
	)

	// Input with partial duplicates (same id but different name/age)
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice", 25),
		createDistinctTestTuple(desc, 1, "Bob", 30),   // Different: same id, different name
		createDistinctTestTuple(desc, 1, "Alice", 25), // Duplicate of first
		createDistinctTestTuple(desc, 2, "Charlie", 35),
		createDistinctTestTuple(desc, 1, "Alice", 26), // Different: same id/name, different age
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 4 unique tuples (all fields must match for duplicate)
	if len(results) != 4 {
		t.Errorf("Expected 4 unique tuples, got %d", len(results))
	}
}

// TestDistinctEmpty tests distinct with empty input
func TestDistinctEmpty(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	emptyTuples := []*tuple.Tuple{}

	mockIter := newMockDistinctIterator(emptyTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 0 tuples
	if len(results) != 0 {
		t.Errorf("Expected 0 tuples, got %d", len(results))
	}
}

// TestDistinctSingleTuple tests distinct with single tuple
func TestDistinctSingleTuple(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice"),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 1 tuple
	if len(results) != 1 {
		t.Errorf("Expected 1 tuple, got %d", len(results))
	}
}

// TestDistinctStringData tests distinct with string-only data
func TestDistinctStringData(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType},
		[]string{"word"},
	)

	// Input: {"apple", "banana", "apple", "cherry", "banana", "apple"}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, "apple"),
		createDistinctTestTuple(desc, "banana"),
		createDistinctTestTuple(desc, "apple"),
		createDistinctTestTuple(desc, "cherry"),
		createDistinctTestTuple(desc, "banana"),
		createDistinctTestTuple(desc, "apple"),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 3 unique strings
	if len(results) != 3 {
		t.Errorf("Expected 3 unique strings, got %d", len(results))
	}
}

// TestDistinctLargeDataset tests distinct with larger dataset
func TestDistinctLargeDataset(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Create dataset with 1000 tuples, but only 100 unique values (each value repeated 10 times)
	var inputTuples []*tuple.Tuple
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			inputTuples = append(inputTuples, createDistinctTestTuple(desc, j))
		}
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 100 unique values
	if len(results) != 100 {
		t.Errorf("Expected 100 unique values, got %d", len(results))
	}
}

// TestDistinctRewind tests that distinct can be rewound and re-executed
func TestDistinctRewind(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Input: {1, 2, 1, 3, 2}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 2),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	// First pass
	results1, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("First pass failed: %v", err)
	}
	if len(results1) != 3 {
		t.Errorf("First pass: Expected 3 unique tuples, got %d", len(results1))
	}

	// Rewind and second pass
	if err := distinctOp.Open(); err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	if err := distinctOp.Rewind(); err != nil {
		t.Fatalf("Rewind failed: %v", err)
	}

	var results2 []*tuple.Tuple
	for {
		hasNext, err := distinctOp.HasNext()
		if err != nil {
			t.Fatalf("Second pass error: %v", err)
		}
		if !hasNext {
			break
		}
		tup, err := distinctOp.Next()
		if err != nil {
			t.Fatalf("Second pass next error: %v", err)
		}
		if tup != nil {
			results2 = append(results2, tup)
		}
	}
	distinctOp.Close()

	if len(results2) != 3 {
		t.Errorf("Second pass: Expected 3 unique tuples, got %d", len(results2))
	}
}

// TestDistinctNilChild tests error handling for nil child
func TestDistinctNilChild(t *testing.T) {
	_, err := NewDistinct(nil)
	if err == nil {
		t.Error("Expected error for nil child, got nil")
	}
}

// TestDistinctOpenClose tests proper open/close lifecycle
func TestDistinctOpenClose(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	// Try to use without opening
	_, err = distinctOp.HasNext()
	if err == nil {
		t.Error("Expected error when using unopened iterator")
	}

	// Open and use
	if err := distinctOp.Open(); err != nil {
		t.Fatalf("Failed to open: %v", err)
	}

	hasNext, err := distinctOp.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}
	if !hasNext {
		t.Error("Expected tuples available")
	}

	// Close
	if err := distinctOp.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Try to use after closing
	_, err = distinctOp.HasNext()
	if err == nil {
		t.Error("Expected error when using closed iterator")
	}
}

// TestDistinctManyDuplicates tests distinct with high duplicate ratio
func TestDistinctManyDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Create 100 tuples but only 5 unique values
	var inputTuples []*tuple.Tuple
	values := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "David"},
		{5, "Eve"},
	}

	for i := 0; i < 20; i++ {
		for _, v := range values {
			inputTuples = append(inputTuples, createDistinctTestTuple(desc, v.id, v.name))
		}
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 5 unique tuples
	if len(results) != 5 {
		t.Errorf("Expected 5 unique tuples, got %d", len(results))
	}
}

// TestDistinctGetTupleDesc tests that schema is preserved
func TestDistinctGetTupleDesc(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType},
		[]string{"id", "name", "age"},
	)

	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1, "Alice", 25),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	resultDesc := distinctOp.GetTupleDesc()

	// Verify schema is unchanged
	if resultDesc.NumFields() != 3 {
		t.Errorf("Expected 3 fields, got %d", resultDesc.NumFields())
	}

	fieldName, _ := resultDesc.GetFieldName(0)
	if fieldName != "id" {
		t.Errorf("Expected field 'id', got '%s'", fieldName)
	}

	fieldName, _ = resultDesc.GetFieldName(1)
	if fieldName != "name" {
		t.Errorf("Expected field 'name', got '%s'", fieldName)
	}

	fieldName, _ = resultDesc.GetFieldName(2)
	if fieldName != "age" {
		t.Errorf("Expected field 'age', got '%s'", fieldName)
	}
}

// TestDistinctConsecutiveDuplicates tests when duplicates appear consecutively
func TestDistinctConsecutiveDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Input: {1, 1, 1, 2, 2, 3, 3, 3, 3}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 3),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 3 unique values
	if len(results) != 3 {
		t.Errorf("Expected 3 unique values, got %d", len(results))
	}
}

// TestDistinctInterleavedDuplicates tests when duplicates are spread throughout
func TestDistinctInterleavedDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Input: {1, 2, 3, 1, 2, 3, 1, 2, 3}
	inputTuples := []*tuple.Tuple{
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 3),
		createDistinctTestTuple(desc, 1),
		createDistinctTestTuple(desc, 2),
		createDistinctTestTuple(desc, 3),
	}

	mockIter := newMockDistinctIterator(inputTuples, desc)
	distinctOp, err := NewDistinct(mockIter)
	if err != nil {
		t.Fatalf("Failed to create DISTINCT: %v", err)
	}

	results, err := collectDistinctTuples(distinctOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 3 unique values
	if len(results) != 3 {
		t.Errorf("Expected 3 unique values, got %d", len(results))
	}
}
