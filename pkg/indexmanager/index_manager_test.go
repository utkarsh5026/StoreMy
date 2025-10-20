package indexmanager

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// Mock CatalogReader for testing
type mockCatalogReader struct {
	indexes []*systemtable.IndexMetadata
	schema  *schema.Schema
}

func (m *mockCatalogReader) GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]*systemtable.IndexMetadata, error) {
	return m.indexes, nil
}

func (m *mockCatalogReader) GetTableSchema(tableID int) (*schema.Schema, error) {
	return m.schema, nil
}

// Helper functions
func createTestTupleDesc() *tuple.TupleDescription {
	fieldTypes := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(fieldTypes, fields)
	if err != nil {
		panic(err)
	}
	return td
}

func createTempFile(t *testing.T, name string) string {
	t.Helper()
	tempDir := t.TempDir()
	return filepath.Join(tempDir, name)
}

func createTestTuple(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}

func setupTestEnvironment(t *testing.T) (*IndexManager, *memory.PageStore, *transaction.TransactionContext, *tuple.TupleDescription, string) {
	t.Helper()

	// Create tuple descriptor
	td := createTestTupleDesc()

	// Create test schema
	testSchema := &schema.Schema{
		TableName: "test_table",
		TupleDesc: td,
	}

	// Create mock catalog
	catalog := &mockCatalogReader{
		indexes: []*systemtable.IndexMetadata{},
		schema:  testSchema,
	}

	// Create page store (requires WAL, pass nil)
	pageStore := memory.NewPageStore(nil)

	// Create index manager (WAL can be nil for these tests)
	im := NewIndexManager(catalog, pageStore, nil)

	// Create transaction context
	txID := primitives.NewTransactionIDFromValue(1)
	ctx := &transaction.TransactionContext{
		ID: txID,
	}

	// Create temp directory for test files
	tempDir := t.TempDir()

	return im, pageStore, ctx, td, tempDir
}

func TestNewIndexManager(t *testing.T) {
	catalog := &mockCatalogReader{
		indexes: []*systemtable.IndexMetadata{},
		schema:  &schema.Schema{},
	}
	pageStore := memory.NewPageStore(nil)

	im := NewIndexManager(catalog, pageStore, nil)

	if im == nil {
		t.Fatal("Expected non-nil IndexManager")
	}

	if im.cache == nil {
		t.Error("Expected cache to be initialized")
	}

	if im.loader == nil {
		t.Error("Expected loader to be initialized")
	}

	if im.maintenance == nil {
		t.Error("Expected maintenance to be initialized")
	}

	if im.lifecycle == nil {
		t.Error("Expected lifecycle to be initialized")
	}
}

func TestCreatePhysicalIndex_BTree(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := filepath.Join(tempDir, "btree_index.dat")

	err := im.CreatePhysicalIndex(filePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create BTree index: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("BTree index file was not created")
	}
}

func TestCreatePhysicalIndex_Hash(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := filepath.Join(tempDir, "hash_index.dat")

	err := im.CreatePhysicalIndex(filePath, types.IntType, index.HashIndex)
	if err != nil {
		t.Fatalf("Failed to create Hash index: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("Hash index file was not created")
	}
}

func TestDeletePhysicalIndex(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := filepath.Join(tempDir, "delete_test.dat")

	// Create index first
	err := im.CreatePhysicalIndex(filePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Delete it
	err = im.DeletePhysicalIndex(filePath)
	if err != nil {
		t.Fatalf("Failed to delete index: %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("Index file was not deleted")
	}

	// Delete non-existent file should not error
	err = im.DeletePhysicalIndex(filePath)
	if err != nil {
		t.Errorf("Deleting non-existent file should not error: %v", err)
	}
}

func TestPopulateIndex(t *testing.T) {
	im, _, ctx, td, tempDir := setupTestEnvironment(t)

	// Create heap file with test data
	heapFilePath := filepath.Join(tempDir, "table.dat")
	heapFile, err := heap.NewHeapFile(heapFilePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Note: HeapFile doesn't have InsertTuple method exposed
	// We'll skip tuple insertion for now and just test index file creation

	// Create index file
	indexFilePath := filepath.Join(tempDir, "index.dat")
	err = im.CreatePhysicalIndex(indexFilePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Populate index (on empty table)
	err = im.PopulateIndex(ctx, indexFilePath, 1, heapFile, 0, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to populate index: %v", err)
	}

	// Verify index file still exists
	_, err = os.Stat(indexFilePath)
	if err != nil {
		t.Fatalf("Index file does not exist after population: %v", err)
	}
}

func TestOnInsert(t *testing.T) {
	im, _, ctx, td, _ := setupTestEnvironment(t)

	// Create a test tuple with RecordID
	tup := createTestTuple(td, 1, "Test")
	tup.RecordID = &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	// OnInsert with no indexes should succeed
	err := im.OnInsert(ctx, 1, tup)
	if err != nil {
		t.Errorf("OnInsert with no indexes should not error: %v", err)
	}

	// OnInsert with nil tuple should error
	err = im.OnInsert(ctx, 1, nil)
	if err == nil {
		t.Error("OnInsert with nil tuple should error")
	}

	// OnInsert with nil RecordID should error
	tupNoRID := createTestTuple(td, 2, "Test2")
	err = im.OnInsert(ctx, 1, tupNoRID)
	if err == nil {
		t.Error("OnInsert with nil RecordID should error")
	}
}

func TestOnDelete(t *testing.T) {
	im, _, ctx, td, _ := setupTestEnvironment(t)

	// Create a test tuple with RecordID
	tup := createTestTuple(td, 1, "Test")
	tup.RecordID = &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	// OnDelete with no indexes should succeed
	err := im.OnDelete(ctx, 1, tup)
	if err != nil {
		t.Errorf("OnDelete with no indexes should not error: %v", err)
	}

	// OnDelete with nil tuple should error
	err = im.OnDelete(ctx, 1, nil)
	if err == nil {
		t.Error("OnDelete with nil tuple should error")
	}
}

func TestOnUpdate(t *testing.T) {
	im, _, ctx, td, _ := setupTestEnvironment(t)

	// Create test tuples with RecordIDs
	oldTup := createTestTuple(td, 1, "OldName")
	oldTup.RecordID = &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	newTup := createTestTuple(td, 1, "NewName")
	newTup.RecordID = &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	// OnUpdate with no indexes should succeed
	err := im.OnUpdate(ctx, 1, oldTup, newTup)
	if err != nil {
		t.Errorf("OnUpdate with no indexes should not error: %v", err)
	}
}

func TestInvalidateCache(t *testing.T) {
	im, _, _, _, _ := setupTestEnvironment(t)

	// Add something to cache
	im.cache.Set(1, []*indexWithMetadata{})

	// Verify it's cached
	if _, exists := im.cache.Get(1); !exists {
		t.Error("Expected index to be cached")
	}

	// Invalidate
	im.InvalidateCache(1)

	// Verify it's removed
	if _, exists := im.cache.Get(1); exists {
		t.Error("Expected cache to be invalidated")
	}
}

func TestClose(t *testing.T) {
	im, _, _, _, _ := setupTestEnvironment(t)

	// Add some mock data to cache
	im.cache.Set(1, []*indexWithMetadata{})
	im.cache.Set(2, []*indexWithMetadata{})

	err := im.Close()
	if err != nil {
		t.Errorf("Close should not error: %v", err)
	}

	// Verify cache is cleared
	if _, exists := im.cache.Get(1); exists {
		t.Error("Cache should be cleared after Close")
	}
	if _, exists := im.cache.Get(2); exists {
		t.Error("Cache should be cleared after Close")
	}
}

func TestIndexCache_GetOrLoad(t *testing.T) {
	cache := newIndexCache()

	loadCount := 0
	loader := func() ([]*indexWithMetadata, error) {
		loadCount++
		return []*indexWithMetadata{}, nil
	}

	// First call should load
	_, err := cache.GetOrLoad(1, loader)
	if err != nil {
		t.Fatalf("GetOrLoad failed: %v", err)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader to be called once, got %d", loadCount)
	}

	// Second call should use cache
	_, err = cache.GetOrLoad(1, loader)
	if err != nil {
		t.Fatalf("GetOrLoad failed: %v", err)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader to not be called again, got %d calls", loadCount)
	}
}

func TestIndexCache_Clear(t *testing.T) {
	cache := newIndexCache()

	// Add some items
	cache.Set(1, []*indexWithMetadata{})
	cache.Set(2, []*indexWithMetadata{})

	// Clear
	old := cache.Clear()

	if len(old) != 2 {
		t.Errorf("Expected 2 items in cleared cache, got %d", len(old))
	}

	// Verify cache is empty
	if _, exists := cache.Get(1); exists {
		t.Error("Cache should be empty after Clear")
	}
}
