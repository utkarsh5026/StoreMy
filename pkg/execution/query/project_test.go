package query

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// PROJECT TESTS
// ============================================================================

// Mock PageID for testing
type mockPageID struct {
	pageID primitives.PageNumber
}

func (m *mockPageID) GetTableID() primitives.TableID           { return 0 }
func (m *mockPageID) PageNo() primitives.PageNumber            { return m.pageID }
func (m *mockPageID) Serialize() []byte                        { return []byte{byte(m.pageID)} }
func (m *mockPageID) Equals(other primitives.PageID) bool      { return other.PageNo() == m.pageID }
func (m *mockPageID) String() string                           { return fmt.Sprintf("Page(%d)", m.pageID) }
func (m *mockPageID) HashCode() primitives.HashCode            { return primitives.HashCode(m.pageID) }

func mustCreateProjectTupleDesc() *tuple.TupleDescription {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType, types.StringType},
		[]string{"id", "name", "age", "email"},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create TupleDescription: %v", err))
	}
	return td
}

func createProjectTestTuple(td *tuple.TupleDescription, id int64, name string, age int64, email string) *tuple.Tuple {
	t := tuple.NewTuple(td)

	err := t.SetField(0, types.NewIntField(id))
	if err != nil {
		panic(fmt.Sprintf("Failed to set id field: %v", err))
	}

	err = t.SetField(1, types.NewStringField(name, 128))
	if err != nil {
		panic(fmt.Sprintf("Failed to set name field: %v", err))
	}

	err = t.SetField(2, types.NewIntField(age))
	if err != nil {
		panic(fmt.Sprintf("Failed to set age field: %v", err))
	}

	err = t.SetField(3, types.NewStringField(email, 128))
	if err != nil {
		panic(fmt.Sprintf("Failed to set email field: %v", err))
	}

	return t
}

// ============================================================================
// PROJECT CONSTRUCTOR TESTS
// ============================================================================

func TestNewProject_ValidInputs(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 2}
	typesList := []types.Type{types.IntType, types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject returned error: %v", err)
	}

	if project == nil {
		t.Fatal("NewProject returned nil")
	}

	if len(project.projectedCols) != len(projectedCols) {
		t.Errorf("Expected projectedCols length %d, got %d", len(projectedCols), len(project.projectedCols))
	}

	for i, expected := range projectedCols {
		if project.projectedCols[i] != expected {
			t.Errorf("Expected projectedCols[%d] = %d, got %d", i, expected, project.projectedCols[i])
		}
	}

	if project.source == nil {
		t.Error("Expected child to be set")
	}

	if project.base == nil {
		t.Error("Expected base iterator to be initialized")
	}

	if project.tupleDesc == nil {
		t.Error("Expected tuple descriptor to be initialized")
	}
}

func TestNewProject_NilChild(t *testing.T) {
	projectedCols := []primitives.ColumnID{0, 1}
	typesList := []types.Type{types.IntType, types.StringType}

	project, err := NewProject(projectedCols, typesList, nil)
	if err == nil {
		t.Error("Expected error when child is nil")
	}
	if project != nil {
		t.Error("Expected nil project when child is nil")
	}
}

func TestNewProject_FieldTypesLengthMismatch(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 1}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when projectedCols and typesList lengths don't match")
	}
	if project != nil {
		t.Error("Expected nil project when projectedCols and typesList lengths don't match")
	}
}

func TestNewProject_EmptyFieldList(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{}
	typesList := []types.Type{}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when projectedCols is empty")
	}
	if project != nil {
		t.Error("Expected nil project when projectedCols is empty")
	}
}

func TestNewProject_NilChildTupleDesc(t *testing.T) {
	child := &mockChildIterator{
		tuples: []*tuple.Tuple{},
		td:     nil,
	}

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when child has nil tuple descriptor")
	}
	if project != nil {
		t.Error("Expected nil project when child has nil tuple descriptor")
	}
}

func TestNewProject_FieldIndexOutOfBounds(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 10}
	typesList := []types.Type{types.IntType, types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when field index is out of bounds")
	}
	if project != nil {
		t.Error("Expected nil project when field index is out of bounds")
	}
}

func TestNewProject_NegativeFieldIndex(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	// Use max uint32 value to test invalid field index (since ColumnID is unsigned)
	projectedCols := []primitives.ColumnID{^primitives.ColumnID(0), 1}
	typesList := []types.Type{types.IntType, types.StringType}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when field index is invalid")
	}
	if project != nil {
		t.Error("Expected nil project when field index is invalid")
	}
}

func TestNewProject_TypeMismatch(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 1}
	typesList := []types.Type{types.StringType, types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err == nil {
		t.Error("Expected error when types don't match child schema")
	}
	if project != nil {
		t.Error("Expected nil project when types don't match child schema")
	}
}

// ============================================================================
// PROJECT SCHEMA TESTS
// ============================================================================

func TestProject_GetTupleDesc(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 2}
	typesList := []types.Type{types.IntType, types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	result := project.GetTupleDesc()
	if result == nil {
		t.Fatal("GetTupleDesc returned nil")
	}

	if result.NumFields() != 2 {
		t.Errorf("Expected 2 fields, got %d", result.NumFields())
	}

	fieldName, err := result.GetFieldName(0)
	if err != nil {
		t.Errorf("Failed to get field name 0: %v", err)
	}
	if fieldName != "id" {
		t.Errorf("Expected field name 'id', got '%s'", fieldName)
	}

	fieldName, err = result.GetFieldName(1)
	if err != nil {
		t.Errorf("Failed to get field name 1: %v", err)
	}
	if fieldName != "age" {
		t.Errorf("Expected field name 'age', got '%s'", fieldName)
	}
}

// ============================================================================
// PROJECT FUNCTIONALITY TESTS
// ============================================================================

func TestProject_EmptyInput(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 1}
	typesList := []types.Type{types.IntType, types.StringType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	hasNext, err := project.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false for empty input")
	}
}

func TestProject_SingleFieldProjection(t *testing.T) {
	td := mustCreateProjectTupleDesc()

	tuples := []*tuple.Tuple{
		createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com"),
		createProjectTestTuple(td, 2, "Bob", 30, "bob@example.com"),
	}

	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{1}
	typesList := []types.Type{types.StringType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := project.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := project.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 tuples, got %d", len(results))
	}

	if len(results) >= 1 {
		if results[0].TupleDesc.NumFields() != 1 {
			t.Errorf("Expected 1 field in projected tuple, got %d", results[0].TupleDesc.NumFields())
		}

		field, err := results[0].GetField(0)
		if err != nil {
			t.Errorf("Failed to get field 0: %v", err)
		} else {
			stringField, ok := field.(*types.StringField)
			if !ok {
				t.Errorf("Expected StringField, got %T", field)
			} else if stringField.Value != "Alice" {
				t.Errorf("Expected 'Alice', got '%s'", stringField.Value)
			}
		}
	}
}

func TestProject_MultipleFieldProjection(t *testing.T) {
	td := mustCreateProjectTupleDesc()

	tuples := []*tuple.Tuple{
		createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com"),
		createProjectTestTuple(td, 2, "Bob", 30, "bob@example.com"),
	}

	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{0, 2}
	typesList := []types.Type{types.IntType, types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := project.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := project.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tuple)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 tuples, got %d", len(results))
	}

	if len(results) >= 1 {
		if results[0].TupleDesc.NumFields() != 2 {
			t.Errorf("Expected 2 fields in projected tuple, got %d", results[0].TupleDesc.NumFields())
		}

		field0, err := results[0].GetField(0)
		if err != nil {
			t.Errorf("Failed to get field 0: %v", err)
		} else {
			intField, ok := field0.(*types.IntField)
			if !ok {
				t.Errorf("Expected IntField, got %T", field0)
			} else if intField.Value != 1 {
				t.Errorf("Expected 1, got %d", intField.Value)
			}
		}

		field1, err := results[0].GetField(1)
		if err != nil {
			t.Errorf("Failed to get field 1: %v", err)
		} else {
			intField, ok := field1.(*types.IntField)
			if !ok {
				t.Errorf("Expected IntField, got %T", field1)
			} else if intField.Value != 25 {
				t.Errorf("Expected 25, got %d", intField.Value)
			}
		}
	}
}

func TestProject_ReorderedFields(t *testing.T) {
	td := mustCreateProjectTupleDesc()

	tuples := []*tuple.Tuple{
		createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com"),
	}

	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{3, 0, 1}
	typesList := []types.Type{types.StringType, types.IntType, types.StringType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	hasNext, err := project.HasNext()
	if err != nil {
		t.Fatalf("HasNext returned error: %v", err)
	}
	if !hasNext {
		t.Fatal("Expected tuple to be available")
	}

	result, err := project.Next()
	if err != nil {
		t.Fatalf("Next returned error: %v", err)
	}

	if result.TupleDesc.NumFields() != 3 {
		t.Errorf("Expected 3 fields in projected tuple, got %d", result.TupleDesc.NumFields())
	}

	field0, err := result.GetField(0)
	if err != nil {
		t.Errorf("Failed to get field 0: %v", err)
	} else {
		stringField, ok := field0.(*types.StringField)
		if !ok {
			t.Errorf("Expected StringField, got %T", field0)
		} else if stringField.Value != "alice@example.com" {
			t.Errorf("Expected 'alice@example.com', got '%s'", stringField.Value)
		}
	}

	field1, err := result.GetField(1)
	if err != nil {
		t.Errorf("Failed to get field 1: %v", err)
	} else {
		intField, ok := field1.(*types.IntField)
		if !ok {
			t.Errorf("Expected IntField, got %T", field1)
		} else if intField.Value != 1 {
			t.Errorf("Expected 1, got %d", intField.Value)
		}
	}

	field2, err := result.GetField(2)
	if err != nil {
		t.Errorf("Failed to get field 2: %v", err)
	} else {
		stringField, ok := field2.(*types.StringField)
		if !ok {
			t.Errorf("Expected StringField, got %T", field2)
		} else if stringField.Value != "Alice" {
			t.Errorf("Expected 'Alice', got '%s'", stringField.Value)
		}
	}
}

func TestProject_RecordIDPreserved(t *testing.T) {
	td := mustCreateProjectTupleDesc()

	testTuple := createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com")
	testTuple.RecordID = &tuple.TupleRecordID{PageID: &mockPageID{pageID: 1}, TupleNum: 5}

	tuples := []*tuple.Tuple{testTuple}
	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	hasNext, err := project.HasNext()
	if err != nil {
		t.Fatalf("HasNext returned error: %v", err)
	}
	if !hasNext {
		t.Fatal("Expected tuple to be available")
	}

	result, err := project.Next()
	if err != nil {
		t.Fatalf("Next returned error: %v", err)
	}

	if result.RecordID == nil {
		t.Error("Expected RecordID to be preserved")
	} else {
		if result.RecordID.PageID.PageNo() != 1 {
			t.Errorf("Expected PageID 1, got %d", result.RecordID.PageID.PageNo())
		}
		if result.RecordID.TupleNum != 5 {
			t.Errorf("Expected TupleNum 5, got %d", result.RecordID.TupleNum)
		}
	}
}

// ============================================================================
// PROJECT ITERATOR OPERATIONS TESTS
// ============================================================================

func TestProject_Close(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0, 1}
	typesList := []types.Type{types.IntType, types.StringType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if child.isOpen {
		t.Error("Expected child iterator to be closed")
	}
}

func TestProject_Rewind(t *testing.T) {
	td := mustCreateProjectTupleDesc()

	tuples := []*tuple.Tuple{
		createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com"),
		createProjectTestTuple(td, 2, "Bob", 30, "bob@example.com"),
	}

	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	err = project.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer project.Close()

	var firstRun []*tuple.Tuple
	for {
		hasNext, err := project.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := project.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		firstRun = append(firstRun, tuple)
	}

	if len(firstRun) != 2 {
		t.Errorf("Expected 2 tuples in first run, got %d", len(firstRun))
	}

	err = project.Rewind()
	if err != nil {
		t.Errorf("Rewind returned error: %v", err)
	}

	var secondRun []*tuple.Tuple
	for {
		hasNext, err := project.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := project.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		secondRun = append(secondRun, tuple)
	}

	if len(secondRun) != 2 {
		t.Errorf("Expected 2 tuples in second run, got %d", len(secondRun))
	}

	for i, expected := range firstRun {
		if i >= len(secondRun) {
			t.Errorf("Missing tuple at index %d in second run", i)
			continue
		}

		expectedField, _ := expected.GetField(0)
		actualField, _ := secondRun[i].GetField(0)

		expectedInt := expectedField.(*types.IntField).Value
		actualInt := actualField.(*types.IntField).Value

		if actualInt != expectedInt {
			t.Errorf("Expected tuple with id %d at index %d in second run, got %d", expectedInt, i, actualInt)
		}
	}
}

func TestProject_Rewind_ChildError(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	child.hasError = true

	err = project.Rewind()
	if err == nil {
		t.Error("Expected error when child rewind fails")
	}
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

func TestProject_ChildError_HasNext(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	child.hasError = true

	hasNext, err := project.HasNext()
	if err == nil {
		t.Error("Expected error when child HasNext fails")
	}
	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

func TestProject_ChildError_Next(t *testing.T) {
	td := mustCreateProjectTupleDesc()
	tuples := []*tuple.Tuple{
		createProjectTestTuple(td, 1, "Alice", 25, "alice@example.com"),
	}
	child := newMockChildIterator(tuples, td)

	projectedCols := []primitives.ColumnID{0}
	typesList := []types.Type{types.IntType}

	project, err := NewProject(projectedCols, typesList, child)
	if err != nil {
		t.Fatalf("NewProject failed: %v", err)
	}

	child.hasError = true

	tuple, err := project.Next()
	if err == nil {
		t.Error("Expected error when child Next fails")
	}
	if tuple != nil {
		t.Error("Expected nil tuple when error occurs")
	}
}
