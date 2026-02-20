package systable

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"testing"
	"time"
)

// MockCatalogAccess implements CatalogAccess for testing
type MockCatalogAccess struct {
	tables map[primitives.FileID][]*tuple.Tuple // tableID -> tuples
}

func NewMockCatalogAccess() *MockCatalogAccess {
	return &MockCatalogAccess{
		tables: make(map[primitives.FileID][]*tuple.Tuple),
	}
}

// IterateTable implements CatalogReader
func (m *MockCatalogAccess) IterateTable(
	tableID primitives.FileID,
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
	tableID primitives.FileID,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	m.tables[tableID] = append(m.tables[tableID], tup)
	return nil
}

// DeleteRow implements CatalogWriter
func (m *MockCatalogAccess) DeleteRow(
	tableID primitives.FileID,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	tuples, ok := m.tables[tableID]
	if !ok {
		return fmt.Errorf("table %d not found", tableID)
	}

	// Parse the tuple to delete for comparison
	toDelete, err := IndexesTableDescriptor.ParseTuple(tup)
	if err != nil {
		return fmt.Errorf("failed to parse tuple to delete: %w", err)
	}

	// Find and remove the tuple by comparing IndexID
	for i, t := range tuples {
		existing, err := IndexesTableDescriptor.ParseTuple(t)
		if err != nil {
			continue
		}
		if existing.IndexID == toDelete.IndexID {
			m.tables[tableID] = append(tuples[:i], tuples[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("tuple not found")
}

// Helper to create index metadata tuple
func createIndexTuple(indexID, tableID primitives.FileID, indexName, columnName string, indexType index.IndexType) *tuple.Tuple {
	metadata := IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexName,
		TableID:    tableID,
		ColumnName: columnName,
		IndexType:  indexType,
		FilePath:   primitives.Filepath(fmt.Sprintf("/data/idx_%d.dat", indexID)),
		CreatedAt:  time.Now(),
	}
	return IndexesTableDescriptor.CreateTuple(metadata)
}

func TestGetIndexesByTable(t *testing.T) {
	// Setup mock catalog
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

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
	var indexTableID primitives.FileID = 100

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
	var indexTableID primitives.FileID = 100

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
	var indexTableID primitives.FileID = 100

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
	var indexTableID primitives.FileID = 100
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
	var indexTableID primitives.FileID = 100

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

// TestGetIndexesByTable_MultipleIndexesSameTable tests retrieving multiple indexes from the same table
func TestGetIndexesByTable_MultipleIndexesSameTable(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	// Create 5 indexes for table 10
	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_col1", "col1", index.BTreeIndex),
		createIndexTuple(2, 10, "idx_col2", "col2", index.BTreeIndex),
		createIndexTuple(3, 10, "idx_col3", "col3", index.HashIndex),
		createIndexTuple(4, 10, "idx_col4", "col4", index.BTreeIndex),
		createIndexTuple(5, 20, "idx_other", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != 4 {
		t.Errorf("Expected 4 indexes for table 10, got %d", len(indexes))
	}

	// Verify all belong to table 10
	for _, idx := range indexes {
		if idx.TableID != 10 {
			t.Errorf("Expected tableID 10, got %d", idx.TableID)
		}
	}
}

// TestGetIndexesByTable_NoIndexes tests retrieving indexes when table has none
func TestGetIndexesByTable_NoIndexes(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_table10", "col1", index.BTreeIndex),
		createIndexTuple(2, 20, "idx_table20", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Query for table 30 which has no indexes
	indexes, err := ops.GetIndexesByTable(nil, 30)
	if err != nil {
		t.Fatalf("Should not error when no indexes found: %v", err)
	}

	if len(indexes) != 0 {
		t.Errorf("Expected 0 indexes for table 30, got %d", len(indexes))
	}
}

// TestGetIndexByName_CaseInsensitive tests case-insensitive name matching
func TestGetIndexByName_CaseInsensitive(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "MyIndexName", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	testCases := []string{
		"myindexname",
		"MYINDEXNAME",
		"MyIndexName",
		"mYiNdExNaMe",
	}

	for _, testName := range testCases {
		idx, err := ops.GetIndexByName(nil, testName)
		if err != nil {
			t.Errorf("Failed to find index with name '%s': %v", testName, err)
		}
		if idx.IndexID != 1 {
			t.Errorf("Expected index ID 1, got %d for name '%s'", idx.IndexID, testName)
		}
	}
}

// TestGetIndexByName_SpecialCharacters tests indexes with special characters
func TestGetIndexByName_SpecialCharacters(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	specialNames := []string{
		"idx_user@email",
		"idx_product#price",
		"idx_order$total",
		"idx_table.column",
		"idx-with-dashes",
		"idx_with_underscores",
	}

	// Create tuples for each special name
	for i, name := range specialNames {
		tup := createIndexTuple(primitives.FileID(i+1), 10, name, "col1", index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Test each special name can be found
	for i, name := range specialNames {
		idx, err := ops.GetIndexByName(nil, name)
		if err != nil {
			t.Errorf("Failed to find index with name '%s': %v", name, err)
		}
		if int(idx.IndexID) != i+1 {
			t.Errorf("Expected index ID %d, got %d for name '%s'", i+1, idx.IndexID, name)
		}
	}
}

// TestGetIndexByID_EdgeCases tests edge cases for ID lookups
func TestGetIndexByID_EdgeCases(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_1", "col1", index.BTreeIndex),
		createIndexTuple(999999, 10, "idx_large", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Test valid IDs
	testCases := []struct {
		id          primitives.FileID
		expectFound bool
		expectName  string
	}{
		{1, true, "idx_1"},
		{999999, true, "idx_large"},
		{0, false, ""},
		{500, false, ""},
	}

	for _, tc := range testCases {
		idx, err := ops.GetIndexByID(nil, tc.id)
		if tc.expectFound {
			if err != nil {
				t.Errorf("Expected to find index with ID %d: %v", tc.id, err)
			}
			if idx.IndexName != tc.expectName {
				t.Errorf("Expected index name '%s', got '%s'", tc.expectName, idx.IndexName)
			}
		} else {
			if err == nil {
				t.Errorf("Expected error for index ID %d, but found index", tc.id)
			}
		}
	}
}

// TestDeleteIndexFromCatalog_MultipleDeletes tests deleting multiple indexes
func TestDeleteIndexFromCatalog_MultipleDeletes(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	// Create 5 indexes
	for i := 1; i <= 5; i++ {
		tup := createIndexTuple(primitives.FileID(i), 10, fmt.Sprintf("idx_%d", i), "col1", index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Delete indexes 2, 4, and 5
	deleteIDs := []primitives.FileID{2, 4, 5}
	for _, id := range deleteIDs {
		err := ops.DeleteIndexFromCatalog(nil, id)
		if err != nil {
			t.Fatalf("Failed to delete index %d: %v", id, err)
		}
	}

	// Verify only 1 and 3 remain
	remainingIDs := []primitives.FileID{1, 3}
	for _, id := range remainingIDs {
		_, err := ops.GetIndexByID(nil, id)
		if err != nil {
			t.Errorf("Index %d should still exist: %v", id, err)
		}
	}

	// Verify deleted indexes are gone
	for _, id := range deleteIDs {
		_, err := ops.GetIndexByID(nil, id)
		if err == nil {
			t.Errorf("Index %d should have been deleted", id)
		}
	}

	// Verify count
	if len(mock.tables[indexTableID]) != 2 {
		t.Errorf("Expected 2 remaining indexes, got %d", len(mock.tables[indexTableID]))
	}
}

// TestDeleteIndexFromCatalog_NonExistent tests deleting non-existent index
func TestDeleteIndexFromCatalog_NonExistent(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_1", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Try to delete non-existent index - should not error
	err := ops.DeleteIndexFromCatalog(nil, 999)
	if err != nil {
		t.Errorf("Should not error when deleting non-existent index: %v", err)
	}

	// Original index should still exist
	_, err = ops.GetIndexByID(nil, 1)
	if err != nil {
		t.Errorf("Original index should still exist: %v", err)
	}
}

// TestIndexOperations_HashIndexType tests operations with Hash indexes
func TestIndexOperations_HashIndexType(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_hash", "col1", index.HashIndex),
		createIndexTuple(2, 10, "idx_btree", "col2", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Get hash index
	idx, err := ops.GetIndexByName(nil, "idx_hash")
	if err != nil {
		t.Fatalf("Failed to get hash index: %v", err)
	}

	if idx.IndexType != index.HashIndex {
		t.Errorf("Expected HashIndex type, got %v", idx.IndexType)
	}

	// Get all indexes for table
	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	// Count by type
	btreeCount := 0
	hashCount := 0
	for _, idx := range indexes {
		switch idx.IndexType {
		case index.BTreeIndex:
			btreeCount++
		case index.HashIndex:
			hashCount++
		}
	}

	if btreeCount != 1 || hashCount != 1 {
		t.Errorf("Expected 1 BTree and 1 Hash index, got %d BTree and %d Hash", btreeCount, hashCount)
	}
}

// TestGetIndexesByTable_LargeNumberOfIndexes tests performance with many indexes
func TestGetIndexesByTable_LargeNumberOfIndexes(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	// Create 100 indexes for table 10
	for i := 1; i <= 100; i++ {
		tup := createIndexTuple(primitives.FileID(i), 10, fmt.Sprintf("idx_%d", i), fmt.Sprintf("col%d", i), index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	// Add some indexes for other tables
	for i := 101; i <= 120; i++ {
		tup := createIndexTuple(primitives.FileID(i), 20, fmt.Sprintf("idx_%d", i), "col1", index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	ops := NewIndexOperations(mock, indexTableID)

	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != 100 {
		t.Errorf("Expected 100 indexes for table 10, got %d", len(indexes))
	}
}

// TestIndexOperations_EmptyIndexName tests handling of empty index names
func TestIndexOperations_EmptyIndexName(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "valid_name", "col1", index.BTreeIndex),
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Try to find by empty name
	_, err := ops.GetIndexByName(nil, "")
	if err == nil {
		t.Error("Expected error when searching for empty index name")
	}
}

// TestDeleteIndexFromCatalog_AllIndexes tests deleting all indexes
func TestDeleteIndexFromCatalog_AllIndexes(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	// Create 3 indexes
	for i := 1; i <= 3; i++ {
		tup := createIndexTuple(primitives.FileID(i), 10, fmt.Sprintf("idx_%d", i), "col1", index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Delete all indexes
	for i := 1; i <= 3; i++ {
		err := ops.DeleteIndexFromCatalog(nil, primitives.FileID(i))
		if err != nil {
			t.Fatalf("Failed to delete index %d: %v", i, err)
		}
	}

	// Verify table is empty
	if len(mock.tables[indexTableID]) != 0 {
		t.Errorf("Expected empty table after deleting all indexes, got %d tuples", len(mock.tables[indexTableID]))
	}

	// Verify no indexes can be retrieved
	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Should not error on empty table: %v", err)
	}
	if len(indexes) != 0 {
		t.Errorf("Expected 0 indexes after deletion, got %d", len(indexes))
	}
}

// TestIndexOperations_DuplicateIndexNames tests indexes with similar names
func TestIndexOperations_DuplicateIndexNames(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	// Create indexes with very similar names (different case)
	mock.tables[indexTableID] = []*tuple.Tuple{
		createIndexTuple(1, 10, "idx_user", "col1", index.BTreeIndex),
		createIndexTuple(2, 20, "idx_user", "col1", index.BTreeIndex), // Same name, different table
	}

	ops := NewIndexOperations(mock, indexTableID)

	// GetIndexByName should return the first match
	idx, err := ops.GetIndexByName(nil, "idx_user")
	if err != nil {
		t.Fatalf("Failed to get index by name: %v", err)
	}

	// Should get one of them (the first one found)
	if idx.IndexID != 1 && idx.IndexID != 2 {
		t.Errorf("Expected index ID 1 or 2, got %d", idx.IndexID)
	}
}

// TestIndexOperations_ColumnNameValidation tests different column name formats
func TestIndexOperations_ColumnNameValidation(t *testing.T) {
	mock := NewMockCatalogAccess()
	var indexTableID primitives.FileID = 100

	columnNames := []string{
		"col1",
		"column_with_underscores",
		"ColumnWithCamelCase",
		"col123",
		"_leading_underscore",
	}

	for i, colName := range columnNames {
		tup := createIndexTuple(primitives.FileID(i+1), 10, fmt.Sprintf("idx_%d", i), colName, index.BTreeIndex)
		mock.tables[indexTableID] = append(mock.tables[indexTableID], tup)
	}

	ops := NewIndexOperations(mock, indexTableID)

	// Verify all indexes can be retrieved
	indexes, err := ops.GetIndexesByTable(nil, 10)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != len(columnNames) {
		t.Errorf("Expected %d indexes, got %d", len(columnNames), len(indexes))
	}

	// Verify column names are preserved
	foundColumns := make(map[string]bool)
	for _, idx := range indexes {
		foundColumns[idx.ColumnName] = true
	}

	for _, colName := range columnNames {
		if !foundColumns[colName] {
			t.Errorf("Column name '%s' not found in results", colName)
		}
	}
}
