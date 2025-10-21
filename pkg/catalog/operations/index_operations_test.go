package operations

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"testing"
)

// MockCatalogAccess implements CatalogAccess for testing
type MockCatalogAccess struct {
	tables map[int][]*tuple.Tuple // tableID -> tuples
}

func NewMockCatalogAccess() *MockCatalogAccess {
	return &MockCatalogAccess{
		tables: make(map[int][]*tuple.Tuple),
	}
}

// IterateTable implements CatalogReader
func (m *MockCatalogAccess) IterateTable(
	tableID int,
	tx *transaction.TransactionContext,
	processFunc func(*tuple.Tuple) error,
) error {
	tuples, ok := m.tables[tableID]
	if !ok {
		return fmt.Errorf("table %d not found", tableID)
	}

	for _, tup := range tuples {
		if err := processFunc(tup); err != nil {
			// Allow "found" pattern for early termination
			if err.Error() == "found" {
				return err
			}
			return err
		}
	}
	return nil
}

// InsertRow implements CatalogWriter
func (m *MockCatalogAccess) InsertRow(
	tableID int,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	m.tables[tableID] = append(m.tables[tableID], tup)
	return nil
}

// DeleteRow implements CatalogWriter
func (m *MockCatalogAccess) DeleteRow(
	tableID int,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	tuples, ok := m.tables[tableID]
	if !ok {
		return fmt.Errorf("table %d not found", tableID)
	}

	// Find and remove the tuple
	for i, t := range tuples {
		if t == tup {
			m.tables[tableID] = append(tuples[:i], tuples[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("tuple not found")
}

// Helper to create index metadata tuple
func createIndexTuple(indexID, tableID int, indexName, columnName string, indexType index.IndexType) *tuple.Tuple {
	metadata := systemtable.IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexName,
		TableID:    tableID,
		ColumnName: columnName,
		IndexType:  indexType,
		FilePath:   fmt.Sprintf("/data/idx_%d.dat", indexID),
		CreatedAt:  1234567890,
	}
	return systemtable.Indexes.CreateTuple(metadata)
}

func TestGetIndexesByTable(t *testing.T) {
	// Setup mock catalog
	mock := NewMockCatalogAccess()
	indexTableID := 100

	// Add index tuples for different tables
	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_users_email", "email", index.BTreeIndex),
		createIndexTuple(2, 10, "idx_users_name", "name", index.BTreeIndex),
		createIndexTuple(3, 20, "idx_products_sku", "sku", index.BTreeIndex),
	}

	// Create operations handler
	ops := NewIndexOperations(mock, indexTableID)

	// Test: Get indexes for table 10
	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes for table 10, got %d", len(indexes))
	}

	// Verify index names
	expectedNames := map[string]bool{
		"idx_users_email": false,
		"idx_users_name":  false,
	}
	for _, idx := range indexes {
		if _, ok := expectedNames[idx.IndexName]; ok {
			expectedNames[idx.IndexName] = true
		}
	}
	for name, found := range expectedNames {
		if !found {
			t.Errorf("Expected index %s not found", name)
		}
	}
}

func TestGetIndexByName(t *testing.T) {
	mock := NewMockCatalogAccess()
	indexTableID := 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_users_email", "email", index.BTreeIndex),
		createIndexTuple(2, 10, "idx_users_name", "name", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Test: Find by name (case-insensitive)
	idx, err := ops.GetIndexByName(nil, "IDX_USERS_EMAIL")
	if err != nil {
		t.Fatalf("Failed to get index by name: %v", err)
	}

	if idx.IndexID != 1 {
		t.Errorf("Expected index ID 1, got %d", idx.IndexID)
	}
	if idx.ColumnName != "email" {
		t.Errorf("Expected column 'email', got '%s'", idx.ColumnName)
	}

	// Test: Non-existent index
	_, err = ops.GetIndexByName(nil, "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestGetIndexByID(t *testing.T) {
	mock := NewMockCatalogAccess()
	indexTableID := 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_users_email", "email", index.BTreeIndex),
		createIndexTuple(2, 10, "idx_users_name", "name", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Test: Find by ID
	idx, err := ops.GetIndexByID(nil, 2)
	if err != nil {
		t.Fatalf("Failed to get index by ID: %v", err)
	}

	if idx.IndexName != "idx_users_name" {
		t.Errorf("Expected index 'idx_users_name', got '%s'", idx.IndexName)
	}

	// Test: Non-existent ID
	_, err = ops.GetIndexByID(nil, 999)
	if err == nil {
		t.Error("Expected error for non-existent index ID")
	}
}

func TestDeleteIndexFromCatalog(t *testing.T) {
	mock := NewMockCatalogAccess()
	indexTableID := 100

	// Add test data
	tup1 := createIndexTuple(1, 10, "idx_users_email", "email", index.BTreeIndex)
	tup2 := createIndexTuple(2, 10, "idx_users_name", "name", index.BTreeIndex)
	tup3 := createIndexTuple(3, 20, "idx_products_sku", "sku", index.BTreeIndex)

	mock.tables[indexTableID] = []*tuple.Tuple{tup1, tup2, tup3}

	ops := NewIndexOperations(mock, indexTableID)

	// Test: Delete index with ID 2
	err := ops.DeleteIndexFromCatalog(nil, 2)
	if err != nil {
		t.Fatalf("Failed to delete index: %v", err)
	}

	// Verify deletion
	if len(mock.tables[indexTableID]) != 2 {
		t.Errorf("Expected 2 tuples after deletion, got %d", len(mock.tables[indexTableID]))
	}

	// Verify the right tuple was deleted
	_, err = ops.GetIndexByID(nil, 2)
	if err == nil {
		t.Error("Expected error when searching for deleted index")
	}

	// Verify other indexes still exist
	_, err = ops.GetIndexByID(nil, 1)
	if err != nil {
		t.Errorf("Index 1 should still exist: %v", err)
	}
	_, err = ops.GetIndexByID(nil, 3)
	if err != nil {
		t.Errorf("Index 3 should still exist: %v", err)
	}
}

func TestIndexOperations_WithEmptyTable(t *testing.T) {
	mock := NewMockCatalogAccess()
	indexTableID := 100
	mock.tables[indexTableID] = []*tuple.Tuple{} // Empty table

	ops := NewIndexOperations(mock, indexTableID)

	// Test: Get indexes from empty table
	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Should not error on empty table: %v", err)
	}
	if len(indexes) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(indexes))
	}

	// Test: Find non-existent index
	_, err = ops.GetIndexByName(nil, "nonexistent")
	if err == nil {
		t.Error("Expected error for index in empty table")
	}
}

func TestIndexOperations_Isolation(t *testing.T) {
	// This test demonstrates that IndexOperations only depends on interfaces
	// and doesn't require the full SystemCatalog

	mock := NewMockCatalogAccess()
	indexTableID := 100

	// Add some data
	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "test_index", "col1", index.BTreeIndex),
	}

	// Create operations with just the interface
	ops := NewIndexOperations(mock, indexTableID)

	// Should work without any SystemCatalog dependencies
	idx, err := ops.GetIndexByID(nil, 1)
	if err != nil {
		t.Fatalf("Failed with interface-only dependency: %v", err)
	}

	if idx.IndexName != "test_index" {
		t.Errorf("Expected 'test_index', got '%s'", idx.IndexName)
	}

	t.Log("✅ IndexOperations successfully works with interface-only dependency")
}
