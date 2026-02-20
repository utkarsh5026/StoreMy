package systable

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

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
	col, err := ColumnsTableDescriptor.ParseTuple(tuples[0])
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

// TestColumnsTable_RoundTrip tests CreateTuple and Parse for ColumnsTable
func TestColumnsTable_RoundTrip(t *testing.T) {

	colMeta := schema.ColumnMetadata{
		TableID:       1,
		Name:          "id",
		FieldType:     types.IntType,
		Position:      0,
		IsPrimary:     true,
		IsAutoInc:     true,
		NextAutoValue: 100,
	}

	// Create tuple
	tup := ColumnsTableDescriptor.CreateTuple(colMeta)
	if tup == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := ColumnsTableDescriptor.ParseTuple(tup)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.TableID != colMeta.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", colMeta.TableID, parsed.TableID)
	}
	if parsed.Name != colMeta.Name {
		t.Errorf("Name mismatch: expected %s, got %s", colMeta.Name, parsed.Name)
	}
	if parsed.FieldType != colMeta.FieldType {
		t.Errorf("FieldType mismatch: expected %v, got %v", colMeta.FieldType, parsed.FieldType)
	}
	if parsed.Position != colMeta.Position {
		t.Errorf("Position mismatch: expected %d, got %d", colMeta.Position, parsed.Position)
	}
	if parsed.IsPrimary != colMeta.IsPrimary {
		t.Errorf("IsPrimary mismatch: expected %v, got %v", colMeta.IsPrimary, parsed.IsPrimary)
	}
	if parsed.IsAutoInc != colMeta.IsAutoInc {
		t.Errorf("IsAutoInc mismatch: expected %v, got %v", colMeta.IsAutoInc, parsed.IsAutoInc)
	}
	if parsed.NextAutoValue != colMeta.NextAutoValue {
		t.Errorf("NextAutoValue mismatch: expected %d, got %d", colMeta.NextAutoValue, parsed.NextAutoValue)
	}
}
