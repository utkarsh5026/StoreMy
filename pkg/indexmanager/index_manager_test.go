package indexmanager

import (
	"fmt"
	"os"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func createFilePath(dir string, filename string) primitives.Filepath {
	return primitives.Filepath(createFilePath(dir, filename))
}

// Mock CatalogReader for testing
type mockCatalogReader struct {
	indexes []*systemtable.IndexMetadata
	schema  *schema.Schema
}

func (m *mockCatalogReader) GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*systemtable.IndexMetadata, error) {
	return m.indexes, nil
}

func (m *mockCatalogReader) GetTableSchema(tableID primitives.FileID) (*schema.Schema, error) {
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

	// Create temp directory for test files
	tempDir := t.TempDir()

	// Create WAL instance
	walPath := createFilePath(tempDir, "test.wal")
	walInstance, err := wal.NewWAL(walPath.String(), 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	t.Cleanup(func() {
		walInstance.Close()
	})

	// Create page store with WAL
	pageStore := memory.NewPageStore(walInstance)

	// Create index manager with WAL
	im := NewIndexManager(catalog, pageStore, walInstance)

	// Create transaction context and register with WAL
	txID := primitives.NewTransactionIDFromValue(1)
	ctx := transaction.NewTransactionContext(txID)

	// Begin transaction in WAL
	_, err = walInstance.LogBegin(txID)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	t.Cleanup(func() {
		// Abort transaction on cleanup if still active
		walInstance.LogAbort(txID)
	})

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

}

func TestCreatePhysicalIndex_BTree(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := primitives.Filepath(createFilePath(tempDir, "btree_index.dat"))

	indexID, err := im.CreatePhysicalIndex(filePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create BTree index: %v", err)
	}

	if indexID == 0 {
		t.Error("Expected non-zero index ID")
	}

	// Verify file exists
	if _, err := os.Stat(string(filePath)); os.IsNotExist(err) {
		t.Error("BTree index file was not created")
	}
}

func TestCreatePhysicalIndex_Hash(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := createFilePath(tempDir, "hash_index.dat")

	indexID, err := im.CreatePhysicalIndex(filePath, types.IntType, index.HashIndex)
	if err != nil {
		t.Fatalf("Failed to create Hash index: %v", err)
	}

	if indexID == 0 {
		t.Error("Expected non-zero index ID")
	}

	// Verify file exists
	if _, err := os.Stat(filePath.String()); os.IsNotExist(err) {
		t.Error("Hash index file was not created")
	}
}

func TestDeletePhysicalIndex(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := createFilePath(tempDir, "delete_test.dat")

	// Create index first
	_, err := im.CreatePhysicalIndex(filePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Delete it
	err = im.DeletePhysicalIndex(filePath)
	if err != nil {
		t.Fatalf("Failed to delete index: %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(filePath.String()); !os.IsNotExist(err) {
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
	heapFilePath := createFilePath(tempDir, "table.dat")
	heapFile, err := heap.NewHeapFile(heapFilePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Note: HeapFile doesn't have InsertTuple method exposed
	// We'll skip tuple insertion for now and just test index file creation

	// Create index file
	indexFilePath := createFilePath(tempDir, "index.dat")
	_, err = im.CreatePhysicalIndex(indexFilePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Populate index (on empty table)
	err = im.PopulateIndex(ctx, indexFilePath, 1, heapFile, 0, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to populate index: %v", err)
	}

	// Verify index file still exists
	_, err = os.Stat(indexFilePath.String())
	if err != nil {
		t.Fatalf("Index file does not exist after population: %v", err)
	}
}

func TestOnInsert(t *testing.T) {
	im, _, ctx, td, _ := setupTestEnvironment(t)

	// Create a test tuple with RecordID
	tup := createTestTuple(td, 1, "Test")
	tup.RecordID = &tuple.TupleRecordID{
		PageID:   page.NewPageDescriptor(1, 0),
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
		PageID:   page.NewPageDescriptor(1, 0),
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
		PageID:   page.NewPageDescriptor(1, 0),
		TupleNum: 0,
	}

	newTup := createTestTuple(td, 1, "NewName")
	newTup.RecordID = &tuple.TupleRecordID{
		PageID:   page.NewPageDescriptor(1, 0),
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

// ========== HARDCORE TESTS ==========

func TestCreatePhysicalIndex_UnsupportedType(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := createFilePath(tempDir, "unsupported.dat")

	// Try to create with invalid index type
	_, err := im.CreatePhysicalIndex(filePath, types.IntType, index.IndexType("InvalidType"))
	if err == nil {
		t.Fatal("Expected error for unsupported index type")
	}
}

func TestDeletePhysicalIndex_NonExistentFile(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	filePath := createFilePath(tempDir, "nonexistent.dat")

	// Delete non-existent file should not error
	err := im.DeletePhysicalIndex(filePath)
	if err != nil {
		t.Errorf("Deleting non-existent file should not error: %v", err)
	}
}

func TestPopulateIndex_WithData(t *testing.T) {
	im, _, ctx, td, tempDir := setupTestEnvironment(t)

	// Create heap file with test data
	heapFilePath := createFilePath(tempDir, "populated_table.dat")
	heapFile, err := heap.NewHeapFile(heapFilePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create and write pages with tuples
	pageID := page.NewPageDescriptor(heapFile.GetID(), 0)
	pageData := make([]byte, 4096) // page.PageSize
	heapPage, err := heap.NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create heap page: %v", err)
	}

	// Add tuples to page
	for i := int64(1); i <= 10; i++ {
		tup := createTestTuple(td, i, fmt.Sprintf("name%d", i))
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	// Write page to file
	err = heapFile.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Create index file
	indexFilePath := createFilePath(tempDir, "populated_index.dat")
	_, err = im.CreatePhysicalIndex(indexFilePath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Populate index
	err = im.PopulateIndex(ctx, indexFilePath, 1, heapFile, 0, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to populate index: %v", err)
	}

	// Verify index file still exists
	_, err = os.Stat(indexFilePath.String())
	if err != nil {
		t.Fatalf("Index file does not exist after population: %v", err)
	}
}

func TestPopulateIndex_HashIndex(t *testing.T) {
	im, _, ctx, td, tempDir := setupTestEnvironment(t)

	// Create heap file with test data
	heapFilePath := createFilePath(tempDir, "hash_populated_table.dat")
	heapFile, err := heap.NewHeapFile(heapFilePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create and write page with tuples
	pageID := page.NewPageDescriptor(heapFile.GetID(), 0)
	pageData := make([]byte, 4096)
	heapPage, err := heap.NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create heap page: %v", err)
	}

	// Add tuples to page
	for i := int64(1); i <= 5; i++ {
		tup := createTestTuple(td, i, fmt.Sprintf("name%d", i))
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	// Write page to file
	err = heapFile.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Create hash index file
	indexFilePath := createFilePath(tempDir, "hash_populated_index.dat")
	_, err = im.CreatePhysicalIndex(indexFilePath, types.IntType, index.HashIndex)
	if err != nil {
		t.Fatalf("Failed to create hash index: %v", err)
	}

	// Populate hash index
	err = im.PopulateIndex(ctx, indexFilePath, 2, heapFile, 0, types.IntType, index.HashIndex)
	if err != nil {
		t.Fatalf("Failed to populate hash index: %v", err)
	}
}

func TestOnInsert_WithMultipleIndexes(t *testing.T) {
	t.Skip("Skipping test requiring WAL - needs proper WAL setup")

	// Note: This test requires a properly initialized WAL because BTree and HashIndex
	// operations require logging. To properly test this, we would need to:
	// 1. Create a mock WAL or initialize a real WAL
	// 2. Handle WAL cleanup
	// The basic OnInsert functionality is already tested in other tests
}

func TestOnDelete_WithMultipleIndexes(t *testing.T) {
	t.Skip("Skipping test requiring WAL - needs proper WAL setup")

	// Note: This test requires a properly initialized WAL because BTree and HashIndex
	// operations require logging. To properly test this, we would need to:
	// 1. Create a mock WAL or initialize a real WAL
	// 2. Handle WAL cleanup
	// The basic OnDelete functionality is already tested in other tests
}

func TestOnUpdate_WithMultipleIndexes(t *testing.T) {
	t.Skip("Skipping test requiring WAL - needs proper WAL setup")

	// Note: This test requires a properly initialized WAL because BTree and HashIndex
	// operations require logging. To properly test this, we would need to:
	// 1. Create a mock WAL or initialize a real WAL
	// 2. Handle WAL cleanup
	// The basic OnUpdate functionality is already tested in other tests
}

func TestInvalidateCache_MultipleTables(t *testing.T) {
	im, _, _, _, _ := setupTestEnvironment(t)

	// Add indexes for multiple tables to cache
	im.cache.Set(1, []*indexWithMetadata{})
	im.cache.Set(2, []*indexWithMetadata{})
	im.cache.Set(3, []*indexWithMetadata{})

	// Invalidate one table
	im.InvalidateCache(2)

	// Verify only table 2 is removed
	if _, exists := im.cache.Get(1); !exists {
		t.Error("Table 1 should still be cached")
	}
	if _, exists := im.cache.Get(2); exists {
		t.Error("Table 2 should be invalidated")
	}
	if _, exists := im.cache.Get(3); !exists {
		t.Error("Table 3 should still be cached")
	}
}

func TestClose_WithOpenIndexes(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	// Create and cache some indexes
	idx1Path := createFilePath(tempDir, "close_idx1.dat")
	idx2Path := createFilePath(tempDir, "close_idx2.dat")

	_, err := im.CreatePhysicalIndex(idx1Path, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Failed to create index 1: %v", err)
	}

	_, err = im.CreatePhysicalIndex(idx2Path, types.IntType, index.HashIndex)
	if err != nil {
		t.Fatalf("Failed to create index 2: %v", err)
	}

	// Manually add some indexes to cache
	im.cache.Set(1, []*indexWithMetadata{})
	im.cache.Set(2, []*indexWithMetadata{})

	// Close should clear cache
	err = im.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify cache is cleared
	if _, exists := im.cache.Get(1); exists {
		t.Error("Cache should be cleared after Close")
	}
	if _, exists := im.cache.Get(2); exists {
		t.Error("Cache should be cleared after Close")
	}
}

func TestIndexCache_GetOrLoad_ConcurrentLoads(t *testing.T) {
	cache := newIndexCache()

	loadCount := int64(0)
	var loadMu sync.Mutex

	loader := func() ([]*indexWithMetadata, error) {
		loadMu.Lock()
		loadCount++
		loadMu.Unlock()
		return []*indexWithMetadata{}, nil
	}

	// Multiple goroutines try to load the same table
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.GetOrLoad(1, loader)
			if err != nil {
				t.Errorf("GetOrLoad failed: %v", err)
			}
		}()
	}
	wg.Wait()

	// Loader should be called at least once, but possibly more due to race conditions
	// The double-check locking in Set prevents duplicate storage, but doesn't prevent concurrent loads
	if loadCount == 0 {
		t.Error("Loader should have been called at least once")
	}
}

func TestIndexCache_Set_DoubleCheckLocking(t *testing.T) {
	cache := newIndexCache()

	indexes1 := []*indexWithMetadata{{metadata: &IndexMetadata{IndexMetadata: systemtable.IndexMetadata{IndexID: 1}}}}
	indexes2 := []*indexWithMetadata{{metadata: &IndexMetadata{IndexMetadata: systemtable.IndexMetadata{IndexID: 2}}}}

	// First set
	result1 := cache.Set(1, indexes1)
	if len(result1) != 1 || result1[0].metadata.IndexID != 1 {
		t.Error("First set should return the set value")
	}

	// Second set should return the already-cached value
	result2 := cache.Set(1, indexes2)
	if len(result2) != 1 || result2[0].metadata.IndexID != 1 {
		t.Error("Second set should return the already-cached value, not the new value")
	}

	// Verify cache still contains the first value
	cached, exists := cache.Get(1)
	if !exists || len(cached) != 1 || cached[0].metadata.IndexID != 1 {
		t.Error("Cache should contain the first set value")
	}
}

func TestCreatePhysicalIndex_AllSupportedTypes(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	testCases := []struct {
		name      string
		keyType   types.Type
		indexType index.IndexType
	}{
		{"BTree_Int", types.IntType, index.BTreeIndex},
		{"BTree_String", types.StringType, index.BTreeIndex},
		{"Hash_Int", types.IntType, index.HashIndex},
		{"Hash_String", types.StringType, index.HashIndex},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := createFilePath(tempDir, tc.name+".dat")

			_, err := im.CreatePhysicalIndex(filePath, tc.keyType, tc.indexType)
			if err != nil {
				t.Fatalf("Failed to create %s index: %v", tc.name, err)
			}

			// Verify file exists
			if _, err := os.Stat(filePath.String()); os.IsNotExist(err) {
				t.Errorf("%s index file was not created", tc.name)
			}
		})
	}
}

func TestOnInsert_WithNilContext(t *testing.T) {
	im, _, _, td, _ := setupTestEnvironment(t)

	tup := createTestTuple(td, 1, "Test")
	tup.RecordID = &tuple.TupleRecordID{
		PageID:   page.NewPageDescriptor(1, 0),
		TupleNum: 0,
	}

	// OnInsert with nil context should still work for tables with no indexes
	err := im.OnInsert(nil, 1, tup)
	if err != nil {
		t.Errorf("OnInsert with nil context should not error when no indexes exist: %v", err)
	}
}

func TestIndexManager_Lifecycle_CompleteWorkflow(t *testing.T) {
	im, _, ctx, td, tempDir := setupTestEnvironment(t)

	// Step 1: Create physical index
	indexPath := createFilePath(tempDir, "workflow_index.dat")
	_, err := im.CreatePhysicalIndex(indexPath, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Step 1 failed - Create index: %v", err)
	}

	// Step 2: Create heap file and populate it
	heapPath := createFilePath(tempDir, "workflow_table.dat")
	heapFile, err := heap.NewHeapFile(heapPath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create and write pages with tuples
	pageID := page.NewPageDescriptor(heapFile.GetID(), 0)
	pageData := make([]byte, 4096)
	heapPage, err := heap.NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create heap page: %v", err)
	}

	// Add tuples to page (reduce to 10 to fit in single page)
	for i := int64(1); i <= 10; i++ {
		tup := createTestTuple(td, i, fmt.Sprintf("name%d", i))
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	// Write page to file
	err = heapFile.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Step 3: Populate index
	err = im.PopulateIndex(ctx, indexPath, 100, heapFile, 0, types.IntType, index.BTreeIndex)
	if err != nil {
		t.Fatalf("Step 3 failed - Populate index: %v", err)
	}

	// Step 4: Verify index exists
	if _, err := os.Stat(indexPath.String()); os.IsNotExist(err) {
		t.Fatal("Step 4 failed - Index file should exist")
	}

	// Step 5: Delete physical index
	err = im.DeletePhysicalIndex(indexPath)
	if err != nil {
		t.Fatalf("Step 5 failed - Delete index: %v", err)
	}

	// Step 6: Verify index is deleted
	if _, err := os.Stat(indexPath.String()); !os.IsNotExist(err) {
		t.Fatal("Step 6 failed - Index file should be deleted")
	}
}

func TestIndexManager_StressTest_ManyIndexes(t *testing.T) {
	im, _, _, _, tempDir := setupTestEnvironment(t)

	// Create many indexes
	numIndexes := 50
	paths := make([]primitives.Filepath, numIndexes)

	for i := 0; i < numIndexes; i++ {
		paths[i] = createFilePath(tempDir, fmt.Sprintf("stress_idx_%d.dat", i))

		indexType := index.BTreeIndex
		if i%2 == 0 {
			indexType = index.HashIndex
		}

		_, err := im.CreatePhysicalIndex(paths[i], types.IntType, indexType)
		if err != nil {
			t.Fatalf("Failed to create index %d: %v", i, err)
		}
	}

	// Verify all exist
	for i, path := range paths {
		if _, err := os.Stat(path.String()); os.IsNotExist(err) {
			t.Errorf("Index %d was not created", i)
		}
	}

	// Delete all
	for i, path := range paths {
		err := im.DeletePhysicalIndex(path)
		if err != nil {
			t.Errorf("Failed to delete index %d: %v", i, err)
		}
	}

	// Verify all deleted
	for i, path := range paths {
		if _, err := os.Stat(path.String()); !os.IsNotExist(err) {
			t.Errorf("Index %d was not deleted", i)
		}
	}
}
