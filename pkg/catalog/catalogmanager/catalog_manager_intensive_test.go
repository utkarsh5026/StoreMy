package catalogmanager

import (
	"os"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
)

// ========================================
// Edge Case Tests for Table Operations
// ========================================

// TestCatalogManager_CreateTable_NilSchema tests creating a table with nil schema
func TestCatalogManager_CreateTable_NilSchema(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, nil)
	if err == nil {
		t.Error("CreateTable should fail with nil schema")
	}
}

// TestCatalogManager_CreateTable_DuplicateName tests creating tables with duplicate names
func TestCatalogManager_CreateTable_DuplicateName(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create first table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("duplicate_test", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("First CreateTable failed: %v", err)
	}

	// Try to create table with same name
	tableSchema2 := createTestSchema("duplicate_test", "id", fields)
	tx3 := setup.beginTx()
	_, err = setup.catalogMgr.CreateTable(tx3, tableSchema2)
	if err == nil {
		t.Error("CreateTable should fail for duplicate table name")
	}
}

// TestCatalogManager_CreateTable_ComplexSchema tests creating a table with multiple columns and types
func TestCatalogManager_CreateTable_ComplexSchema(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table with many different column types
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType, IsAutoInc: true},
		{Name: "name", Type: types.StringType},
		{Name: "age", Type: types.IntType},
		{Name: "salary", Type: types.IntType},
		{Name: "email", Type: types.StringType},
		{Name: "active", Type: types.IntType}, // Boolean as int
	}

	tableSchema := createTestSchema("complex_table", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify schema
	tx3 := setup.beginTx()
	schema, err := setup.catalogMgr.GetTableSchema(tx3, tableID)
	if err != nil {
		t.Fatalf("GetTableSchema failed: %v", err)
	}

	if schema.NumFields() != len(fields) {
		t.Errorf("Expected %d fields, got %d", len(fields), schema.NumFields())
	}

	// Verify each column
	for i, expected := range fields {
		col := schema.Columns[i]
		if col.Name != expected.Name {
			t.Errorf("Column %d: expected name %s, got %s", i, expected.Name, col.Name)
		}
		if col.FieldType != expected.Type {
			t.Errorf("Column %d: expected type %v, got %v", i, expected.Type, col.FieldType)
		}
	}

	// Verify auto-increment
	autoInc, err := setup.catalogMgr.GetAutoIncrementColumn(tx3, tableID)
	if err != nil {
		t.Fatalf("GetAutoIncrementColumn failed: %v", err)
	}
	if autoInc == nil {
		t.Error("Expected auto-increment column")
	} else if autoInc.ColumnName != "id" {
		t.Errorf("Expected auto-increment on 'id', got '%s'", autoInc.ColumnName)
	}
}

// TestCatalogManager_DropTable_NonExistent tests dropping a non-existent table
func TestCatalogManager_DropTable_NonExistent(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	err := setup.catalogMgr.DropTable(tx2, "nonexistent_table")
	if err == nil {
		t.Error("DropTable should fail for non-existent table")
	}
}

// TestCatalogManager_RenameTable_ToExistingName tests renaming to an existing table name
func TestCatalogManager_RenameTable_ToExistingName(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create two tables
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}

	tableSchema1 := createTestSchema("table1", "id", fields)
	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema1)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable 1 failed: %v", err)
	}

	tableSchema2 := createTestSchema("table2", "id", fields)
	tx3 := setup.beginTx()
	_, err = setup.catalogMgr.CreateTable(tx3, tableSchema2)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateTable 2 failed: %v", err)
	}

	// Try to rename table1 to table2
	tx4 := setup.beginTx()
	err = setup.catalogMgr.RenameTable(tx4, "table1", "table2")
	if err == nil {
		t.Error("RenameTable should fail when target name already exists")
	}

	// Verify table1 still exists with original name
	if !setup.catalogMgr.tableCache.TableExists("table1") {
		t.Error("table1 should still exist after failed rename")
	}
}

// TestCatalogManager_RenameTable_NonExistent tests renaming a non-existent table
func TestCatalogManager_RenameTable_NonExistent(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	err := setup.catalogMgr.RenameTable(tx2, "nonexistent", "new_name")
	if err == nil {
		t.Error("RenameTable should fail for non-existent table")
	}
}

// TestCatalogManager_LoadTable_NonExistent tests loading a non-existent table
func TestCatalogManager_LoadTable_NonExistent(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	err := setup.catalogMgr.LoadTable(tx2, "nonexistent_table")
	if err == nil {
		t.Error("LoadTable should fail for non-existent table")
	}
}

// TestCatalogManager_LoadTable_AlreadyLoaded tests loading a table that's already in cache
func TestCatalogManager_LoadTable_AlreadyLoaded(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("loaded_table", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Load it again (should be a no-op)
	tx3 := setup.beginTx()
	err = setup.catalogMgr.LoadTable(tx3, "loaded_table")
	if err != nil {
		t.Errorf("LoadTable should succeed for already-loaded table: %v", err)
	}
}

// TestCatalogManager_ClearCache tests cache clearing functionality
func TestCatalogManager_ClearCache(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("cache_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify table exists in cache
	if !setup.catalogMgr.tableCache.TableExists("cache_test") {
		t.Fatal("Table should exist in cache before clear")
	}

	// Clear cache
	setup.catalogMgr.ClearCache()

	// Verify cache is empty
	if setup.catalogMgr.tableCache.TableExists("cache_test") {
		t.Error("Table should not exist in cache after clear")
	}

	// Verify table still exists on disk
	tx3 := setup.beginTx()
	if !setup.catalogMgr.TableExists(tx3, "cache_test") {
		t.Error("Table should still exist on disk after cache clear")
	}

	// Reload table
	tx4 := setup.beginTx()
	err = setup.catalogMgr.LoadTable(tx4, "cache_test")
	if err != nil {
		t.Fatalf("LoadTable failed after cache clear: %v", err)
	}

	// Verify table is back in cache
	if !setup.catalogMgr.tableCache.TableExists("cache_test") {
		t.Error("Table should be in cache after LoadTable")
	}

	// Verify we can still get the table file
	_, err = setup.catalogMgr.GetTableFile(tableID)
	if err != nil {
		t.Errorf("GetTableFile should work after reload: %v", err)
	}
}

// TestCatalogManager_GetTableFile_NotInCache tests GetTableFile for uncached table
func TestCatalogManager_GetTableFile_NotInCache(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Try to get file for non-existent table ID
	_, err := setup.catalogMgr.GetTableFile(999999)
	if err == nil {
		t.Error("GetTableFile should fail for non-cached table")
	}
}

// ========================================
// Index Operation Tests
// ========================================

// TestCatalogManager_CreateIndex tests creating an index
func TestCatalogManager_CreateIndex(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}
	tableSchema := createTestSchema("indexed_table", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create an index
	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("indexed_table", "idx_name")
	filePath, err := setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_name", "indexed_table", "name", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if testIndexID == 0 {
		t.Error("Expected non-zero index ID")
	}

	if filePath == "" {
		t.Error("Expected non-empty file path")
	}

	expectedPath := filepath.Join(setup.tempDir, "indexed_table_idx_name.idx")
	if string(filePath) != expectedPath {
		t.Errorf("Expected file path %s, got %s", expectedPath, filePath)
	}

	// Verify index exists
	tx4 := setup.beginTx()
	if !setup.catalogMgr.IndexExists(tx4, "idx_name") {
		t.Error("Index should exist after creation")
	}

	// Get index metadata
	metadata, err := setup.catalogMgr.GetIndexByName(tx4, "idx_name")
	if err != nil {
		t.Fatalf("GetIndexByName failed: %v", err)
	}

	if metadata.IndexName != "idx_name" {
		t.Errorf("Expected index name 'idx_name', got '%s'", metadata.IndexName)
	}
	if metadata.TableID != tableID {
		t.Errorf("Expected table ID %d, got %d", tableID, metadata.TableID)
	}
	if metadata.ColumnName != "name" {
		t.Errorf("Expected column name 'name', got '%s'", metadata.ColumnName)
	}
	if metadata.IndexType != index.BTreeIndex {
		t.Errorf("Expected index type BTreeIndex, got %v", metadata.IndexType)
	}
}

// TestCatalogManager_CreateIndex_DuplicateName tests creating indexes with duplicate names
func TestCatalogManager_CreateIndex_DuplicateName(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}
	tableSchema := createTestSchema("idx_test", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create first index
	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("idx_test", "idx_duplicate")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_duplicate", "idx_test", "name", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("First CreateIndex failed: %v", err)
	}

	// Try to create index with same name
	tx4 := setup.beginTx()
	testIndexID2 := setup.generateIndexID("idx_test", "idx_duplicate")
	_, err = setup.catalogMgr.CreateIndex(tx4, testIndexID2, "idx_duplicate", "idx_test", "id", index.BTreeIndex)
	if err == nil {
		t.Error("CreateIndex should fail for duplicate index name")
	}
}

// TestCatalogManager_CreateIndex_InvalidTable tests creating an index on non-existent table
func TestCatalogManager_CreateIndex_InvalidTable(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	testIndexID := setup.generateIndexID("nonexistent_table", "idx_invalid")
	_, err := setup.catalogMgr.CreateIndex(tx2, testIndexID, "idx_invalid", "nonexistent_table", "col", index.BTreeIndex)
	if err == nil {
		t.Error("CreateIndex should fail for non-existent table")
	}
}

// TestCatalogManager_CreateIndex_InvalidColumn tests creating an index on non-existent column
func TestCatalogManager_CreateIndex_InvalidColumn(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("idx_col_test", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Try to create index on non-existent column
	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("idx_col_test", "idx_bad_col")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_bad_col", "idx_col_test", "nonexistent_col", index.BTreeIndex)
	if err == nil {
		t.Error("CreateIndex should fail for non-existent column")
	}
}

// TestCatalogManager_DropIndex tests dropping an index
func TestCatalogManager_DropIndex(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table and index
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("drop_idx_test", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("drop_idx_test", "idx_to_drop")
	filePath, err := setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_to_drop", "drop_idx_test", "id", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Verify index exists
	tx4 := setup.beginTx()
	if !setup.catalogMgr.IndexExists(tx4, "idx_to_drop") {
		t.Fatal("Index should exist before drop")
	}
	setup.commitTx(tx4)

	// Drop the index
	tx5 := setup.beginTx()
	returnedPath, err := setup.catalogMgr.DropIndex(tx5, "idx_to_drop")
	setup.commitTx(tx5)
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if returnedPath != filePath {
		t.Errorf("Expected file path %s, got %s", filePath, returnedPath)
	}

	// Small delay to allow locks to be released
	// Note: In a real implementation, this shouldn't be necessary,
	// but concurrent tests can have timing issues
	// time.Sleep(10 * time.Millisecond)

	// Verify index no longer exists
	tx6 := setup.beginTx()
	if setup.catalogMgr.IndexExists(tx6, "idx_to_drop") {
		t.Error("Index should not exist after drop")
	}
	setup.commitTx(tx6)
}

// TestCatalogManager_DropIndex_NonExistent tests dropping a non-existent index
func TestCatalogManager_DropIndex_NonExistent(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.DropIndex(tx2, "nonexistent_index")
	if err == nil {
		t.Error("DropIndex should fail for non-existent index")
	}
}

// TestCatalogManager_GetIndexesByTable tests retrieving all indexes for a table
func TestCatalogManager_GetIndexesByTable(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table with multiple columns
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
		{Name: "email", Type: types.StringType},
	}
	tableSchema := createTestSchema("multi_idx_table", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create multiple indexes
	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("multi_idx_table", "idx_name")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_name", "multi_idx_table", "name", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex 1 failed: %v", err)
	}

	tx4 := setup.beginTx()
	testIndexID2 := setup.generateIndexID("multi_idx_table", "idx_email")
	_, err = setup.catalogMgr.CreateIndex(tx4, testIndexID2, "idx_email", "multi_idx_table", "email", index.HashIndex)
	setup.commitTx(tx4)
	if err != nil {
		t.Fatalf("CreateIndex 2 failed: %v", err)
	}

	// Get all indexes for the table
	tx5 := setup.beginTx()
	indexes, err := setup.catalogMgr.GetIndexesByTable(tx5, tableID)
	if err != nil {
		t.Fatalf("GetIndexesByTable failed: %v", err)
	}

	// Should have 2 manually created indexes (PK indexes are now created by DDL layer, not catalog manager)
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// Verify index names
	indexNames := make(map[string]bool)
	for _, idx := range indexes {
		indexNames[idx.IndexName] = true
	}

	if !indexNames["idx_name"] {
		t.Error("idx_name not found in indexes")
	}
	if !indexNames["idx_email"] {
		t.Error("idx_email not found in indexes")
	}
}

// TestCatalogManager_GetIndexesForTable tests GetIndexesForTable (IndexInfo variant)
func TestCatalogManager_GetIndexesForTable(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table and index
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("idx_info_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("idx_info_test", "idx_test")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_test", "idx_info_test", "id", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Get indexes as IndexInfo
	tx4 := setup.beginTx()
	indexInfos, err := setup.catalogMgr.GetIndexesForTable(tx4, tableID)
	if err != nil {
		t.Fatalf("GetIndexesForTable failed: %v", err)
	}

	// Should have 1 manually created index (PK indexes are now created by DDL layer, not catalog manager)
	if len(indexInfos) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indexInfos))
	}

	// Verify the manually created index
	if len(indexInfos) > 0 {
		info := indexInfos[0]
		if info.IndexName != "idx_test" {
			t.Errorf("Expected index name 'idx_test', got '%s'", info.IndexName)
		}
		if info.TableID != tableID {
			t.Errorf("Expected table ID %d, got %d", tableID, info.TableID)
		}
		if info.ColumnName != "id" {
			t.Errorf("Expected column 'id', got '%s'", info.ColumnName)
		}
	}
}

// TestCatalogManager_GetAllIndexes tests retrieving all indexes
func TestCatalogManager_GetAllIndexes(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create two tables with indexes
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}

	tableSchema1 := createTestSchema("table_a", "id", fields)
	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema1)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable 1 failed: %v", err)
	}

	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("table_a", "idx_a")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_a", "table_a", "id", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex 1 failed: %v", err)
	}

	tableSchema2 := createTestSchema("table_b", "id", fields)
	tx4 := setup.beginTx()
	_, err = setup.catalogMgr.CreateTable(tx4, tableSchema2)
	setup.commitTx(tx4)
	if err != nil {
		t.Fatalf("CreateTable 2 failed: %v", err)
	}

	tx5 := setup.beginTx()
	testIndexID2 := setup.generateIndexID("table_b", "idx_b")
	_, err = setup.catalogMgr.CreateIndex(tx5, testIndexID2, "idx_b", "table_b", "id", index.HashIndex)
	setup.commitTx(tx5)
	if err != nil {
		t.Fatalf("CreateIndex 2 failed: %v", err)
	}

	// Get all indexes
	tx6 := setup.beginTx()
	allIndexes, err := setup.catalogMgr.GetAllIndexes(tx6)
	if err != nil {
		t.Fatalf("GetAllIndexes failed: %v", err)
	}

	if len(allIndexes) < 2 {
		t.Errorf("Expected at least 2 indexes, got %d", len(allIndexes))
	}
}

// ========================================
// Statistics Operation Tests
// ========================================

// TestCatalogManager_UpdateTableStatistics tests updating table statistics
func TestCatalogManager_UpdateTableStatistics(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("stats_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Update statistics
	tx3 := setup.beginTx()
	err = setup.catalogMgr.UpdateTableStatistics(tx3, tableID)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("UpdateTableStatistics failed: %v", err)
	}

	// Get statistics
	tx4 := setup.beginTx()
	stats, err := setup.catalogMgr.GetTableStatistics(tx4, tableID)
	if err != nil {
		t.Fatalf("GetTableStatistics failed: %v", err)
	}

	if stats == nil {
		t.Fatal("Expected statistics, got nil")
	}

	if stats.TableID != tableID {
		t.Errorf("Expected table ID %d, got %d", tableID, stats.TableID)
	}
}

// TestCatalogManager_GetTableStatistics_Caching tests statistics caching
func TestCatalogManager_GetTableStatistics_Caching(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("cache_stats_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Update statistics
	tx3 := setup.beginTx()
	err = setup.catalogMgr.UpdateTableStatistics(tx3, tableID)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("UpdateTableStatistics failed: %v", err)
	}

	// Get statistics (should cache)
	tx4 := setup.beginTx()
	stats1, err := setup.catalogMgr.GetTableStatistics(tx4, tableID)
	if err != nil {
		t.Fatalf("First GetTableStatistics failed: %v", err)
	}

	// Get statistics again (should use cache)
	tx5 := setup.beginTx()
	stats2, err := setup.catalogMgr.GetTableStatistics(tx5, tableID)
	if err != nil {
		t.Fatalf("Second GetTableStatistics failed: %v", err)
	}

	// Both should be identical (same pointer from cache)
	if stats1.TableID != stats2.TableID {
		t.Error("Cached statistics should be identical")
	}
}

// TestCatalogManager_RefreshStatistics tests RefreshStatistics
func TestCatalogManager_RefreshStatistics(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("refresh_stats_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Refresh statistics (update and get in one call)
	tx3 := setup.beginTx()
	stats, err := setup.catalogMgr.RefreshStatistics(tx3, tableID)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("RefreshStatistics failed: %v", err)
	}

	if stats == nil {
		t.Fatal("Expected statistics, got nil")
	}

	if stats.TableID != tableID {
		t.Errorf("Expected table ID %d, got %d", tableID, stats.TableID)
	}
}

// TestCatalogManager_UpdateColumnStatistics tests updating column statistics
func TestCatalogManager_UpdateColumnStatistics(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table with multiple columns
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}
	tableSchema := createTestSchema("col_stats_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Update column statistics
	tx3 := setup.beginTx()
	err = setup.catalogMgr.UpdateColumnStatistics(tx3, tableID)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("UpdateColumnStatistics failed: %v", err)
	}

	// Get column statistics
	tx4 := setup.beginTx()
	colStats, err := setup.catalogMgr.GetColumnStatistics(tx4, tableID, "id")
	if err != nil {
		t.Fatalf("GetColumnStatistics failed: %v", err)
	}

	if colStats != nil {
		if colStats.TableID != tableID {
			t.Errorf("Expected table ID %d, got %d", tableID, colStats.TableID)
		}
		if colStats.ColumnName != "id" {
			t.Errorf("Expected column name 'id', got '%s'", colStats.ColumnName)
		}
	}
}

// ========================================
// Sequential Operation Tests
// ========================================

// TestCatalogManager_MultipleTableOperationsSequence tests a complex sequence of operations
func TestCatalogManager_MultipleTableOperationsSequence(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType, IsAutoInc: true},
		{Name: "name", Type: types.StringType},
	}
	tableSchema := createTestSchema("sequence_test", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create index
	tx3 := setup.beginTx()
	testIndexID := setup.generateIndexID("sequence_test", "idx_seq_name")
	_, err = setup.catalogMgr.CreateIndex(tx3, testIndexID, "idx_seq_name", "sequence_test", "name", index.BTreeIndex)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Update statistics
	tx4 := setup.beginTx()
	err = setup.catalogMgr.UpdateTableStatistics(tx4, tableID)
	setup.commitTx(tx4)
	if err != nil {
		t.Fatalf("UpdateTableStatistics failed: %v", err)
	}

	// Rename table
	tx5 := setup.beginTx()
	err = setup.catalogMgr.RenameTable(tx5, "sequence_test", "renamed_sequence")
	setup.commitTx(tx5)
	if err != nil {
		t.Fatalf("RenameTable failed: %v", err)
	}

	// Verify new name
	tx6 := setup.beginTx()
	if !setup.catalogMgr.TableExists(tx6, "renamed_sequence") {
		t.Error("Renamed table should exist")
	}
	if setup.catalogMgr.TableExists(tx6, "sequence_test") {
		t.Error("Old table name should not exist")
	}

	// Get indexes (should still exist with old table reference)
	indexes, err := setup.catalogMgr.GetIndexesByTable(tx6, tableID)
	if err != nil {
		t.Fatalf("GetIndexesByTable failed: %v", err)
	}
	// Should have 1 manually created index (PK indexes are now created by DDL layer, not catalog manager)
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index after rename, got %d", len(indexes))
	}
	setup.commitTx(tx6)

	// Drop index
	tx7 := setup.beginTx()
	_, err = setup.catalogMgr.DropIndex(tx7, "idx_seq_name")
	setup.commitTx(tx7)
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	// Small delay to allow locks to be released after index drop
	// time.Sleep(10 * time.Millisecond)

	// Drop table
	tx8 := setup.beginTx()
	err = setup.catalogMgr.DropTable(tx8, "renamed_sequence")
	setup.commitTx(tx8)
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify table is gone
	tx9 := setup.beginTx()
	if setup.catalogMgr.TableExists(tx9, "renamed_sequence") {
		t.Error("Table should not exist after drop")
	}
	setup.commitTx(tx9)
}

// TestCatalogManager_CreateDropMultipleTables tests creating and dropping multiple tables
func TestCatalogManager_CreateDropMultipleTables(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}

	// Create 10 tables
	tableIDs := make([]primitives.FileID, 10)
	for i := 0; i < 10; i++ {
		tableName := filepath.Base(setup.tempDir) + "_multi_table_" + string(rune('0'+i))
		tableSchema := createTestSchema(tableName, "id", fields)

		tx := setup.beginTx()
		tableID, err := setup.catalogMgr.CreateTable(tx, tableSchema)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateTable %d failed: %v", i, err)
		}
		tableIDs[i] = tableID
	}

	// Verify all tables exist
	tx2 := setup.beginTx()
	allTables, err := setup.catalogMgr.ListAllTables(tx2, true)
	if err != nil {
		t.Fatalf("ListAllTables failed: %v", err)
	}
	if len(allTables) < 10 {
		t.Errorf("Expected at least 10 tables, got %d", len(allTables))
	}

	// Drop all tables
	for i := 0; i < 10; i++ {
		tableName := filepath.Base(setup.tempDir) + "_multi_table_" + string(rune('0'+i))
		tx := setup.beginTx()
		err := setup.catalogMgr.DropTable(tx, tableName)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("DropTable %d failed: %v", i, err)
		}
	}

	// Verify all tables are gone
	for i := 0; i < 10; i++ {
		tableName := filepath.Base(setup.tempDir) + "_multi_table_" + string(rune('0'+i))
		if setup.catalogMgr.tableCache.TableExists(tableName) {
			t.Errorf("Table %s should not exist after drop", tableName)
		}
	}
}

// TestCatalogManager_HeapFileCreation tests that heap files are actually created on disk
func TestCatalogManager_HeapFileCreation(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("heap_file_test", "id", fields)

	tx2 := setup.beginTx()
	_, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify heap file exists on disk
	heapPath := filepath.Join(setup.tempDir, "heap_file_test.dat")
	if _, err := os.Stat(heapPath); os.IsNotExist(err) {
		t.Error("Heap file was not created on disk")
	}

	// Verify file exists (it may be empty until first insert, which is OK)
	info, err := os.Stat(heapPath)
	if err != nil {
		t.Fatalf("Failed to stat heap file: %v", err)
	}

	// Just log the file size - it may be 0 if no data has been inserted yet
	t.Logf("Heap file created with size: %d bytes", info.Size())

	// The file exists, which is what matters - the size may be 0 initially
	// This is acceptable behavior for an empty table
}
