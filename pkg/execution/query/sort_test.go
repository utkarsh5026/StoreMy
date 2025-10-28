package query

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// SORT CONSTRUCTOR TESTS
// ============================================================================

func TestNewSort_ValidInputs(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort returned error: %v", err)
	}

	if sort == nil {
		t.Fatal("NewSort returned nil")
	}

	if sort.sortField != 0 {
		t.Errorf("Expected sortFieldIdx 0, got %d", sort.sortField)
	}

	if !sort.ascending {
		t.Error("Expected ascending=true")
	}
}

func TestNewSort_NilChild(t *testing.T) {
	sort, err := NewSort(nil, 0, true)
	if err == nil {
		t.Error("Expected error when child is nil")
	}
	if sort != nil {
		t.Error("Expected nil sort when child is nil")
	}
}

func TestNewSort_FieldIndexOutOfBounds(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 99, true)
	if err == nil {
		t.Error("Expected error when field index is out of bounds")
	}
	if sort != nil {
		t.Error("Expected nil sort when field index is out of bounds")
	}
}

func TestNewSort_NilTupleDesc(t *testing.T) {
	child := newMockChildIterator([]*tuple.Tuple{}, nil)

	sort, err := NewSort(child, 0, true)
	if err == nil {
		t.Error("Expected error when child has nil tuple desc")
	}
	if sort != nil {
		t.Error("Expected nil sort when child has nil tuple desc")
	}
}

// ============================================================================
// SORT OPEN/CLOSE TESTS
// ============================================================================

func TestSort_Open_Success(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Errorf("Open returned error: %v", err)
	}

	if !child.isOpen {
		t.Error("Expected child iterator to be opened")
	}

	if !sort.opened {
		t.Error("Expected sort operator to be marked as opened")
	}
}

func TestSort_Open_ChildError(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)
	child.hasError = true

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err == nil {
		t.Error("Expected error when child fails to open")
	}
}

func TestSort_Close(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = sort.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if child.isOpen {
		t.Error("Expected child iterator to be closed")
	}

	if sort.opened {
		t.Error("Expected sort operator to be marked as closed")
	}
}

// ============================================================================
// SORT SCHEMA TESTS
// ============================================================================

func TestSort_GetTupleDesc(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	result := sort.GetTupleDesc()
	if result != td {
		t.Errorf("Expected tuple desc %v, got %v", td, result)
	}
}

// ============================================================================
// SORT FUNCTIONALITY TESTS - ASCENDING
// ============================================================================

func TestSort_EmptyInput(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	hasNext, err := sort.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false for empty input")
	}
}

func TestSort_SingleTuple(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 5, "single"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != 1 {
		t.Errorf("Expected 1 tuple, got %d", len(results))
	}

	if len(results) >= 1 && results[0] != tuples[0] {
		t.Errorf("Expected tuple %v, got %v", tuples[0], results[0])
	}
}

func TestSort_IntegerAscending(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 5, "five"),
		createSortTestTuple(td, 1, "one"),
		createSortTestTuple(td, 9, "nine"),
		createSortTestTuple(td, 3, "three"),
		createSortTestTuple(td, 7, "seven"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true) // Sort by id (field 0) ascending
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify ascending order: 1, 3, 5, 7, 9
	expectedIds := []int64{1, 3, 5, 7, 9}
	for i, expectedId := range expectedIds {
		field, err := results[i].GetField(0)
		if err != nil {
			t.Errorf("Failed to get field from result %d: %v", i, err)
			continue
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Errorf("Expected IntField at position %d", i)
			continue
		}

		if intField.Value != expectedId {
			t.Errorf("Expected id %d at position %d, got %d", expectedId, i, intField.Value)
		}
	}
}

func TestSort_IntegerDescending(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 5, "five"),
		createSortTestTuple(td, 1, "one"),
		createSortTestTuple(td, 9, "nine"),
		createSortTestTuple(td, 3, "three"),
		createSortTestTuple(td, 7, "seven"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, false) // Sort by id (field 0) descending
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify descending order: 9, 7, 5, 3, 1
	expectedIds := []int64{9, 7, 5, 3, 1}
	for i, expectedId := range expectedIds {
		field, err := results[i].GetField(0)
		if err != nil {
			t.Errorf("Failed to get field from result %d: %v", i, err)
			continue
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Errorf("Expected IntField at position %d", i)
			continue
		}

		if intField.Value != expectedId {
			t.Errorf("Expected id %d at position %d, got %d", expectedId, i, intField.Value)
		}
	}
}

func TestSort_StringAscending(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 1, "zebra"),
		createSortTestTuple(td, 2, "apple"),
		createSortTestTuple(td, 3, "mango"),
		createSortTestTuple(td, 4, "banana"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 1, true) // Sort by name (field 1) ascending
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify ascending order: apple, banana, mango, zebra
	expectedNames := []string{"apple", "banana", "mango", "zebra"}
	for i, expectedName := range expectedNames {
		field, err := results[i].GetField(1)
		if err != nil {
			t.Errorf("Failed to get field from result %d: %v", i, err)
			continue
		}

		stringField, ok := field.(*types.StringField)
		if !ok {
			t.Errorf("Expected StringField at position %d", i)
			continue
		}

		if stringField.Value != expectedName {
			t.Errorf("Expected name '%s' at position %d, got '%s'", expectedName, i, stringField.Value)
		}
	}
}

func TestSort_StringDescending(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 1, "zebra"),
		createSortTestTuple(td, 2, "apple"),
		createSortTestTuple(td, 3, "mango"),
		createSortTestTuple(td, 4, "banana"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 1, false) // Sort by name (field 1) descending
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify descending order: zebra, mango, banana, apple
	expectedNames := []string{"zebra", "mango", "banana", "apple"}
	for i, expectedName := range expectedNames {
		field, err := results[i].GetField(1)
		if err != nil {
			t.Errorf("Failed to get field from result %d: %v", i, err)
			continue
		}

		stringField, ok := field.(*types.StringField)
		if !ok {
			t.Errorf("Expected StringField at position %d", i)
			continue
		}

		if stringField.Value != expectedName {
			t.Errorf("Expected name '%s' at position %d, got '%s'", expectedName, i, stringField.Value)
		}
	}
}

func TestSort_DuplicateValues(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 5, "first"),
		createSortTestTuple(td, 3, "second"),
		createSortTestTuple(td, 5, "third"),
		createSortTestTuple(td, 3, "fourth"),
		createSortTestTuple(td, 5, "fifth"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true) // Sort by id ascending
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify sorted order: all 3's should come before all 5's
	for i := 0; i < 2; i++ {
		field, _ := results[i].GetField(0)
		intField := field.(*types.IntField)
		if intField.Value != 3 {
			t.Errorf("Expected id 3 at position %d, got %d", i, intField.Value)
		}
	}

	for i := 2; i < 5; i++ {
		field, _ := results[i].GetField(0)
		intField := field.(*types.IntField)
		if intField.Value != 5 {
			t.Errorf("Expected id 5 at position %d, got %d", i, intField.Value)
		}
	}
}

func TestSort_AlreadySorted(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 1, "one"),
		createSortTestTuple(td, 2, "two"),
		createSortTestTuple(td, 3, "three"),
		createSortTestTuple(td, 4, "four"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify order is maintained
	expectedIds := []int64{1, 2, 3, 4}
	for i, expectedId := range expectedIds {
		field, _ := results[i].GetField(0)
		intField := field.(*types.IntField)
		if intField.Value != expectedId {
			t.Errorf("Expected id %d at position %d, got %d", expectedId, i, intField.Value)
		}
	}
}

func TestSort_ReverseSorted(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 4, "four"),
		createSortTestTuple(td, 3, "three"),
		createSortTestTuple(td, 2, "two"),
		createSortTestTuple(td, 1, "one"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true) // Ascending sort
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	results := collectResults(t, sort)
	if len(results) != len(tuples) {
		t.Fatalf("Expected %d tuples, got %d", len(tuples), len(results))
	}

	// Verify it's now sorted in ascending order
	expectedIds := []int64{1, 2, 3, 4}
	for i, expectedId := range expectedIds {
		field, _ := results[i].GetField(0)
		intField := field.(*types.IntField)
		if intField.Value != expectedId {
			t.Errorf("Expected id %d at position %d, got %d", expectedId, i, intField.Value)
		}
	}
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

func TestSort_HasNext_NotOpened(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	hasNext, err := sort.HasNext()
	if err == nil {
		t.Error("Expected error when calling HasNext on unopened sort")
	}
	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

func TestSort_Next_NotOpened(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	tuple, err := sort.Next()
	if err == nil {
		t.Error("Expected error when calling Next on unopened sort")
	}
	if tuple != nil {
		t.Error("Expected nil tuple when error occurs")
	}
}

func TestSort_Rewind_NotOpened(t *testing.T) {
	td := mustCreateSortTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Rewind()
	if err == nil {
		t.Error("Expected error when calling Rewind on unopened sort")
	}
}

// ============================================================================
// REWIND TESTS
// ============================================================================

func TestSort_Rewind(t *testing.T) {
	td := mustCreateSortTupleDesc()
	tuples := []*tuple.Tuple{
		createSortTestTuple(td, 3, "three"),
		createSortTestTuple(td, 1, "one"),
		createSortTestTuple(td, 2, "two"),
	}

	child := newMockChildIterator(tuples, td)
	sort, err := NewSort(child, 0, true)
	if err != nil {
		t.Fatalf("NewSort failed: %v", err)
	}

	err = sort.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sort.Close()

	// First iteration
	firstRun := collectResults(t, sort)
	if len(firstRun) != 3 {
		t.Errorf("Expected 3 tuples in first run, got %d", len(firstRun))
	}

	// Rewind
	err = sort.Rewind()
	if err != nil {
		t.Errorf("Rewind returned error: %v", err)
	}

	// Second iteration
	secondRun := collectResults(t, sort)
	if len(secondRun) != 3 {
		t.Errorf("Expected 3 tuples in second run, got %d", len(secondRun))
	}

	// Results should be identical
	for i := range firstRun {
		if i >= len(secondRun) {
			t.Errorf("Missing tuple at index %d in second run", i)
			continue
		}
		if secondRun[i] != firstRun[i] {
			t.Errorf("Tuples differ at position %d", i)
		}
	}
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func mustCreateSortTupleDesc() *tuple.TupleDescription {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		panic("Failed to create TupleDescription")
	}
	return td
}

func createSortTestTuple(td *tuple.TupleDescription, id int32, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	intField := types.NewIntField(int64(id))
	stringField := types.NewStringField(name, 128)

	if err := t.SetField(0, intField); err != nil {
		panic("Failed to set int field")
	}

	if err := t.SetField(1, stringField); err != nil {
		panic("Failed to set string field")
	}

	return t
}

func collectResults(t *testing.T, iter *Sort) []*tuple.Tuple {
	var results []*tuple.Tuple
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := iter.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}
	return results
}
