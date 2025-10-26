package operations

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// mockCatalogAccess implements both CatalogReader and CatalogWriter for testing
type mockCatalogAccess struct {
	tuples map[int][]*tuple.Tuple // tableID -> tuples
}

func newMockCatalogAccess() *mockCatalogAccess {
	return &mockCatalogAccess{
		tuples: make(map[int][]*tuple.Tuple),
	}
}

func (m *mockCatalogAccess) IterateTable(tableID int, tx TxContext, fn func(*tuple.Tuple) error) error {
	for _, t := range m.tuples[tableID] {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockCatalogAccess) InsertRow(tableID int, tx TxContext, t *tuple.Tuple) error {
	m.tuples[tableID] = append(m.tuples[tableID], t)
	return nil
}

func (m *mockCatalogAccess) DeleteRow(tableID int, tx TxContext, t *tuple.Tuple) error {
	tuples := m.tuples[tableID]
	for i, existing := range tuples {
		if tuplesEqual(existing, t) {
			m.tuples[tableID] = append(tuples[:i], tuples[i+1:]...)
			return nil
		}
	}
	return nil
}

func tuplesEqual(t1, t2 *tuple.Tuple) bool {
	numFields := t1.TupleDesc.NumFields()
	if numFields != t2.TupleDesc.NumFields() {
		return false
	}
	for i := 0; i < numFields; i++ {
		f1, _ := t1.GetField(i)
		f2, _ := t2.GetField(i)
		if f1.Type() != f2.Type() {
			return false
		}
		// Simple comparison - in real implementation would compare values
		if f1.String() != f2.String() {
			return false
		}
	}
	return true
}

func createColumnTuple(tableID int, name string, position int, fieldType types.Type, isAutoInc bool, nextAutoValue int) *tuple.Tuple {
	col := schema.ColumnMetadata{
		TableID:       tableID,
		Name:          name,
		Position:      position,
		FieldType:     fieldType,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
	}
	return systemtable.Columns.CreateTuple(col)
}

func TestGetAutoIncrementColumn_NoAutoIncrement(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add regular columns without auto-increment
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, false, 0),
		createColumnTuple(100, "name", 1, types.StringType, false, 0),
	}

	result, err := co.GetAutoIncrementColumn(nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != nil {
		t.Errorf("expected nil, got %+v", result)
	}
}

func TestGetAutoIncrementColumn_SingleAutoIncrement(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add auto-increment column
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 42),
		createColumnTuple(100, "name", 1, types.StringType, false, 0),
	}

	result, err := co.GetAutoIncrementColumn(nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if result.ColumnName != "id" {
		t.Errorf("expected column name 'id', got '%s'", result.ColumnName)
	}

	if result.ColumnIndex != 0 {
		t.Errorf("expected column index 0, got %d", result.ColumnIndex)
	}

	if result.NextValue != 42 {
		t.Errorf("expected next value 42, got %d", result.NextValue)
	}
}

func TestGetAutoIncrementColumn_MVCCMultipleVersions(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add multiple versions of the same auto-increment column (MVCC scenario)
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 10),
		createColumnTuple(100, "id", 0, types.IntType, true, 15),
		createColumnTuple(100, "id", 0, types.IntType, true, 20), // Latest version
		createColumnTuple(100, "name", 1, types.StringType, false, 0),
	}

	result, err := co.GetAutoIncrementColumn(nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	// Should return the version with highest NextAutoValue
	if result.NextValue != 20 {
		t.Errorf("expected next value 20 (latest version), got %d", result.NextValue)
	}
}

func TestGetAutoIncrementColumn_DifferentTables(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add columns from different tables
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 10),
		createColumnTuple(200, "id", 0, types.IntType, true, 99), // Different table
		createColumnTuple(100, "name", 1, types.StringType, false, 0),
	}

	result, err := co.GetAutoIncrementColumn(nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	// Should only return column from table 100
	if result.NextValue != 10 {
		t.Errorf("expected next value 10, got %d", result.NextValue)
	}
}

func TestFindLatestAutoIncrementColumn(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 5),
		createColumnTuple(100, "id", 0, types.IntType, true, 10),
		createColumnTuple(100, "id", 0, types.IntType, true, 15),
	}

	result, err := co.findLatestAutoIncrementColumn(nil, 100, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if result.NextAutoValue != 15 {
		t.Errorf("expected next auto value 15, got %d", result.NextAutoValue)
	}
}

func TestFindLatestAutoIncrementColumn_NotFound(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "name", 0, types.StringType, false, 0),
	}

	result, err := co.findLatestAutoIncrementColumn(nil, 100, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != nil {
		t.Errorf("expected nil, got %+v", result)
	}
}

func TestIncrementAutoIncrementValue(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add initial auto-increment column
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 10),
	}

	tx := &transaction.TransactionContext{}
	err := co.IncrementAutoIncrementValue(tx, 100, "id", 11)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the old tuple was deleted and new one inserted
	tuples := mock.tuples[1]
	if len(tuples) != 1 {
		t.Errorf("expected 1 tuple, got %d", len(tuples))
	}

	// Parse and verify the new tuple
	col, err := systemtable.Columns.Parse(tuples[0])
	if err != nil {
		t.Fatalf("failed to parse column: %v", err)
	}

	if col.NextAutoValue != 11 {
		t.Errorf("expected next auto value 11, got %d", col.NextAutoValue)
	}
}

func TestIncrementAutoIncrementValue_ColumnNotFound(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "name", 0, types.StringType, false, 0),
	}

	tx := &transaction.TransactionContext{}
	err := co.IncrementAutoIncrementValue(tx, 100, "id", 11)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLoadColumnMetadata(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 1)

	// Add columns for multiple tables
	mock.tuples[1] = []*tuple.Tuple{
		createColumnTuple(100, "id", 0, types.IntType, true, 1),
		createColumnTuple(100, "name", 1, types.StringType, false, 0),
		createColumnTuple(200, "id", 0, types.IntType, true, 1), // Different table
	}

	tx := &transaction.TransactionContext{}
	columns, err := co.LoadColumnMetadata(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(columns) != 2 {
		t.Errorf("expected 2 columns for table 100, got %d", len(columns))
	}

	// Verify correct columns were loaded
	for _, col := range columns {
		if col.TableID != 100 {
			t.Errorf("expected table ID 100, got %d", col.TableID)
		}
	}
}

func TestNewColumnOperations(t *testing.T) {
	mock := newMockCatalogAccess()
	co := NewColumnOperations(mock, 42)

	if co == nil {
		t.Fatal("expected ColumnOperations, got nil")
	}

	if co.TableID() != 42 {
		t.Errorf("expected columnsTableID 42, got %d", co.TableID())
	}
}
