package execution

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// FILTER TESTS
// ============================================================================

// Mock child iterator for testing Filter
type mockChildIterator struct {
	tuples   []*tuple.Tuple
	index    int
	isOpen   bool
	hasError bool
	td       *tuple.TupleDescription
}

func newMockChildIterator(tuples []*tuple.Tuple, td *tuple.TupleDescription) *mockChildIterator {
	return &mockChildIterator{
		tuples: tuples,
		index:  -1,
		td:     td,
	}
}

func (m *mockChildIterator) Open() error {
	if m.hasError {
		return fmt.Errorf("mock open error")
	}
	m.isOpen = true
	m.index = -1
	return nil
}

func (m *mockChildIterator) Close() error {
	m.isOpen = false
	return nil
}

func (m *mockChildIterator) HasNext() (bool, error) {
	if !m.isOpen {
		return false, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return false, fmt.Errorf("mock has next error")
	}
	return m.index+1 < len(m.tuples), nil
}

func (m *mockChildIterator) Next() (*tuple.Tuple, error) {
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

func (m *mockChildIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.td
}

func (m *mockChildIterator) Rewind() error {
	if !m.isOpen {
		return fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return fmt.Errorf("mock rewind error")
	}
	m.index = -1
	return nil
}

// Helper functions for creating test data

func mustCreateFilterTupleDesc() *tuple.TupleDescription {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create TupleDescription: %v", err))
	}
	return td
}

func createFilterTestTuple(td *tuple.TupleDescription, id int32, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	intField := types.NewIntField(id)
	stringField := types.NewStringField(name, 128)
	
	err := t.SetField(0, intField)
	if err != nil {
		panic(fmt.Sprintf("Failed to set int field: %v", err))
	}
	
	err = t.SetField(1, stringField)
	if err != nil {
		panic(fmt.Sprintf("Failed to set string field: %v", err))
	}
	
	return t
}

// ============================================================================
// FILTER CONSTRUCTOR TESTS
// ============================================================================

func TestNewFilter_ValidInputs(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter returned error: %v", err)
	}

	if filter == nil {
		t.Fatal("NewFilter returned nil")
	}

	if filter.predicate != predicate {
		t.Errorf("Expected predicate %v, got %v", predicate, filter.predicate)
	}

	if filter.child != child {
		t.Errorf("Expected child %v, got %v", child, filter.child)
	}

	if filter.base == nil {
		t.Error("Expected base iterator to be initialized")
	}
}

func TestNewFilter_NilPredicate(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	filter, err := NewFilter(nil, child)
	if err == nil {
		t.Error("Expected error when predicate is nil")
	}
	if filter != nil {
		t.Error("Expected nil filter when predicate is nil")
	}
}

func TestNewFilter_NilChild(t *testing.T) {
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, nil)
	if err == nil {
		t.Error("Expected error when child is nil")
	}
	if filter != nil {
		t.Error("Expected nil filter when child is nil")
	}
}

// ============================================================================
// FILTER OPEN/CLOSE TESTS
// ============================================================================

func TestFilter_Open_Success(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Errorf("Open returned error: %v", err)
	}

	if !child.isOpen {
		t.Error("Expected child iterator to be opened")
	}
}

func TestFilter_Open_ChildError(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	child.hasError = true
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err == nil {
		t.Error("Expected error when child fails to open")
	}
}

func TestFilter_Close(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = filter.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if child.isOpen {
		t.Error("Expected child iterator to be closed")
	}
}

// ============================================================================
// FILTER SCHEMA TESTS
// ============================================================================

func TestFilter_GetTupleDesc(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	result := filter.GetTupleDesc()
	if result != td {
		t.Errorf("Expected tuple desc %v, got %v", td, result)
	}
}

// ============================================================================
// FILTER FUNCTIONALITY TESTS
// ============================================================================

func TestFilter_EmptyInput(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	hasNext, err := filter.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false for empty input")
	}
}

func TestFilter_AllTuplesMatch(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 5, "first"),
		createFilterTestTuple(td, 5, "second"),
		createFilterTestTuple(td, 5, "third"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(5)) // id = 5

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	if len(results) != len(tuples) {
		t.Errorf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	for i, expected := range tuples {
		if i >= len(results) {
			t.Errorf("Missing tuple at index %d", i)
			continue
		}
		if results[i] != expected {
			t.Errorf("Expected tuple %v at index %d, got %v", expected, i, results[i])
		}
	}
}

func TestFilter_NoTuplesMatch(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "first"),
		createFilterTestTuple(td, 2, "second"),
		createFilterTestTuple(td, 3, "third"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(99)) // id = 99 (no matches)

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	hasNext, err := filter.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false when no tuples match")
	}
}

func TestFilter_SomeTuplesMatch(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "first"),
		createFilterTestTuple(td, 5, "second"),
		createFilterTestTuple(td, 3, "third"),
		createFilterTestTuple(td, 5, "fourth"),
		createFilterTestTuple(td, 7, "fifth"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(5)) // id = 5

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	expectedCount := 2 // tuples[1] and tuples[3] should match
	if len(results) != expectedCount {
		t.Errorf("Expected %d tuples, got %d", expectedCount, len(results))
	}

	// Verify the correct tuples were returned
	if len(results) >= 1 && results[0] != tuples[1] {
		t.Errorf("Expected first result to be tuples[1], got %v", results[0])
	}
	if len(results) >= 2 && results[1] != tuples[3] {
		t.Errorf("Expected second result to be tuples[3], got %v", results[1])
	}
}

// ============================================================================
// PREDICATE OPERATION TESTS
// ============================================================================

func TestFilter_LessThanPredicate(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "first"),
		createFilterTestTuple(td, 5, "second"),
		createFilterTestTuple(td, 10, "third"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, LessThan, types.NewIntField(5)) // id < 5

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	expectedCount := 1 // Only tuples[0] should match (id=1 < 5)
	if len(results) != expectedCount {
		t.Errorf("Expected %d tuples, got %d", expectedCount, len(results))
	}

	if len(results) >= 1 && results[0] != tuples[0] {
		t.Errorf("Expected result to be tuples[0], got %v", results[0])
	}
}

func TestFilter_GreaterThanPredicate(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "first"),
		createFilterTestTuple(td, 5, "second"),
		createFilterTestTuple(td, 10, "third"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, GreaterThan, types.NewIntField(5)) // id > 5

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	expectedCount := 1 // Only tuples[2] should match (id=10 > 5)
	if len(results) != expectedCount {
		t.Errorf("Expected %d tuples, got %d", expectedCount, len(results))
	}

	if len(results) >= 1 && results[0] != tuples[2] {
		t.Errorf("Expected result to be tuples[2], got %v", results[0])
	}
}

func TestFilter_StringPredicate(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "apple"),
		createFilterTestTuple(td, 2, "banana"),
		createFilterTestTuple(td, 3, "apple"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(1, Equals, types.NewStringField("apple", 128)) // name = "apple"

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	expectedCount := 2 // tuples[0] and tuples[2] should match
	if len(results) != expectedCount {
		t.Errorf("Expected %d tuples, got %d", expectedCount, len(results))
	}

	if len(results) >= 1 && results[0] != tuples[0] {
		t.Errorf("Expected first result to be tuples[0], got %v", results[0])
	}
	if len(results) >= 2 && results[1] != tuples[2] {
		t.Errorf("Expected second result to be tuples[2], got %v", results[1])
	}
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

func TestFilter_HasNext_NotOpened(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	hasNext, err := filter.HasNext()
	if err == nil {
		t.Error("Expected error when calling HasNext on unopened filter")
	}
	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

func TestFilter_Next_NotOpened(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	tuple, err := filter.Next()
	if err == nil {
		t.Error("Expected error when calling Next on unopened filter")
	}
	if tuple != nil {
		t.Error("Expected nil tuple when error occurs")
	}
}

func TestFilter_ChildIteratorError(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "test"),
	}
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	// Force child iterator to return error
	child.hasError = true

	hasNext, err := filter.HasNext()
	if err == nil {
		t.Error("Expected error when child iterator fails")
	}
	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

func TestFilter_InvalidFieldIndex(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "test"),
	}
	child := newMockChildIterator(tuples, td)
	
	// Use invalid field index (schema has fields 0,1 but we use index 5)
	predicate := NewPredicate(5, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	hasNext, err := filter.HasNext()
	if err == nil {
		t.Error("Expected error when predicate uses invalid field index")
	}
	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

// ============================================================================
// REWIND TESTS
// ============================================================================

func TestFilter_Rewind(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	
	tuples := []*tuple.Tuple{
		createFilterTestTuple(td, 1, "first"),
		createFilterTestTuple(td, 2, "second"),
	}
	
	child := newMockChildIterator(tuples, td)
	predicate := NewPredicate(0, LessThan, types.NewIntField(3)) // Both tuples match

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	// Consume all tuples
	var firstRun []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		firstRun = append(firstRun, tuple)
	}

	if len(firstRun) != 2 {
		t.Errorf("Expected 2 tuples in first run, got %d", len(firstRun))
	}

	// Rewind and consume again
	err = filter.Rewind()
	if err != nil {
		t.Errorf("Rewind returned error: %v", err)
	}

	var secondRun []*tuple.Tuple
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := filter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		secondRun = append(secondRun, tuple)
	}

	if len(secondRun) != 2 {
		t.Errorf("Expected 2 tuples in second run, got %d", len(secondRun))
	}

	// Results should be identical
	for i, expected := range firstRun {
		if i >= len(secondRun) {
			t.Errorf("Missing tuple at index %d in second run", i)
			continue
		}
		if secondRun[i] != expected {
			t.Errorf("Expected tuple %v at index %d in second run, got %v", expected, i, secondRun[i])
		}
	}
}

func TestFilter_Rewind_ChildError(t *testing.T) {
	td := mustCreateFilterTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	predicate := NewPredicate(0, Equals, types.NewIntField(1))

	filter, err := NewFilter(predicate, child)
	if err != nil {
		t.Fatalf("NewFilter failed: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer filter.Close()

	// Force child rewind to fail
	child.hasError = true

	err = filter.Rewind()
	if err == nil {
		t.Error("Expected error when child rewind fails")
	}
}