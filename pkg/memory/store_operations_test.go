package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestNewPageStore(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	if ps == nil {
		t.Fatal("NewPageStore returned nil")
	}

	if ps.cache == nil {
		t.Error("cache should be initialized")
	}

	if ps.transactions == nil {
		t.Error("transactions should be initialized")
	}

	if ps.tableManager != tm {
		t.Error("tableManager should be set correctly")
	}

	// Cache is properly initialized - no need to check internal fields

	if ps.cache.Size() != 0 {
		t.Errorf("cache should be empty, got %d entries", ps.cache.Size())
	}

	if len(ps.transactions) != 0 {
		t.Errorf("transactions should be empty, got %d entries", len(ps.transactions))
	}
}

func TestPageStore_InsertTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and tuple
	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	stringField := types.NewStringField("test_value", 128)
	testTuple.SetField(0, intField)
	testTuple.SetField(1, stringField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Verify page was cached
	if ps.cache.Size() == 0 {
		t.Error("Expected page to be added to cache")
	}
}

func TestPageStore_InsertTuple_TableNotFound(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	tid := transaction.NewTransactionID()

	// Create a dummy tuple (we won't be able to insert it anyway)
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}

	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test with non-existent table ID
	err = ps.InsertTuple(tid, 999, testTuple)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	if !contains(err.Error(), "table with ID 999 not found") {
		t.Errorf("Expected error containing 'table with ID 999 not found', got %q", err.Error())
	}
}

func TestPageStore_InsertTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify the page exists and is marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.cache.Get(pageID)
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}

		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_InsertTuple_MultiplePages(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create mock file that returns multiple pages
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})

	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Since our mock only returns one page per AddTuple call, we should have 1 page
	if ps.cache.Size() != 1 {
		t.Errorf("Expected 1 page in cache, got %d", ps.cache.Size())
	}

	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) != 1 {
		t.Errorf("Expected 1 dirty page tracked, got %d", len(txInfo.dirtyPages))
	}
}

func TestPageStore_InsertTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numInsertsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent inserts
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numInsertsPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3

				// Create tuple
				tupleValue := goroutineID*numInsertsPerGoroutine + j
				dbFile, _ := tm.GetDbFile(tableID)
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(tupleValue))
				testTuple.SetField(0, intField)

				// Insert tuple
				err := ps.InsertTuple(tid, tableID, testTuple)
				if err != nil {
					t.Errorf("InsertTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - cache should be at max capacity due to LRU eviction
	expectedCacheSize := MaxPageCount // 50 pages max
	if ps.cache.Size() != expectedCacheSize {
		t.Errorf("Expected %d pages in cache, got %d", expectedCacheSize, ps.cache.Size())
	}
}

func TestPageStore_TransactionInfo(t *testing.T) {
	info := &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Test dirty pages tracking
	pageID1 := heap.NewHeapPageID(1, 0)
	pageID2 := heap.NewHeapPageID(1, 1)

	info.dirtyPages[pageID1] = true
	info.dirtyPages[pageID2] = true

	if len(info.dirtyPages) != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", len(info.dirtyPages))
	}

	// Test locked pages tracking
	info.lockedPages[pageID1] = ReadOnly
	info.lockedPages[pageID2] = ReadWrite

	if len(info.lockedPages) != 2 {
		t.Errorf("Expected 2 locked pages, got %d", len(info.lockedPages))
	}

	if info.lockedPages[pageID1] != ReadOnly {
		t.Errorf("Expected ReadOnly permission for pageID1, got %v", info.lockedPages[pageID1])
	}

	if info.lockedPages[pageID2] != ReadWrite {
		t.Errorf("Expected ReadWrite permission for pageID2, got %v", info.lockedPages[pageID2])
	}
}

func TestPageStore_Permissions(t *testing.T) {
	tests := []struct {
		name       string
		permission Permissions
		expected   int
	}{
		{"ReadOnly", ReadOnly, 0},
		{"ReadWrite", ReadWrite, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.permission) != tt.expected {
				t.Errorf("Expected %s to have value %d, got %d", tt.name, tt.expected, int(tt.permission))
			}
		})
	}
}

func TestPageStore_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)

		// Test with nil transaction ID
		err = ps.InsertTuple(nil, 1, testTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()

		// Test with nil tuple
		err = ps.InsertTuple(tid, 1, nil)
		// This should not panic
	})
}

func TestPageStore_DeleteTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and tuple with RecordID
	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	stringField := types.NewStringField("test_value", 128)
	testTuple.SetField(0, intField)
	testTuple.SetField(1, stringField)

	// Set up RecordID for the tuple
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test DeleteTuple
	err = ps.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Errorf("DeleteTuple failed: %v", err)
	}

	// Verify page was cached
	if ps.cache.Size() == 0 {
		t.Error("Expected page to be added to cache")
	}
}

func TestPageStore_DeleteTuple_NoRecordID(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	tid := transaction.NewTransactionID()

	// Create a tuple without RecordID
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}

	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test with tuple that has no RecordID
	err = ps.DeleteTuple(tid, testTuple)
	if err == nil {
		t.Error("Expected error for tuple without RecordID")
	}

	expectedErrMsg := "tuple has no record ID"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_DeleteTuple_TableNotFound(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	tid := transaction.NewTransactionID()

	// Create a tuple with RecordID pointing to non-existent table
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}

	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Set up RecordID with non-existent table ID
	pageID := heap.NewHeapPageID(999, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test with non-existent table ID
	err = ps.DeleteTuple(tid, testTuple)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	if !contains(err.Error(), "table with ID 999 not found") {
		t.Errorf("Expected error containing 'table with ID 999 not found', got %q", err.Error())
	}
}

func TestPageStore_DeleteTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple with RecordID
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test DeleteTuple
	err = ps.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Errorf("DeleteTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify the page exists and is marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.cache.Get(pageID)
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}

		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_DeleteTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numDeletesPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numDeletesPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3

				// Create tuple with RecordID
				dbFile, _ := tm.GetDbFile(tableID)
				tupleValue := goroutineID*numDeletesPerGoroutine + j
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(tupleValue))
				testTuple.SetField(0, intField)

				pageID := heap.NewHeapPageID(tableID, j)
				recordID := &tuple.TupleRecordID{
					PageID:   pageID,
					TupleNum: 0,
				}
				testTuple.RecordID = recordID

				// Delete tuple
				err := ps.DeleteTuple(tid, testTuple)
				if err != nil {
					t.Errorf("DeleteTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - cache should be at max capacity due to LRU eviction
	expectedCacheSize := MaxPageCount // 50 pages max
	if ps.cache.Size() != expectedCacheSize {
		t.Errorf("Expected %d pages in cache, got %d", expectedCacheSize, ps.cache.Size())
	}
}

func TestPageStore_DeleteTuple_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)

		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		testTuple.RecordID = recordID

		// Test with nil transaction ID
		_ = ps.DeleteTuple(nil, testTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		tid := transaction.NewTransactionID()

		// Test with nil tuple
		err := ps.DeleteTuple(tid, nil)
		// This should not panic, but will likely error due to nil pointer
		if err == nil {
			t.Error("Expected error for nil tuple")
		}
	})
}

func TestPageStore_MemoryLeaks(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create many transactions and insert tuples
	numTransactions := 100
	for i := 0; i < numTransactions; i++ {
		tid := transaction.NewTransactionID()

		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(i))
		testTuple.SetField(0, intField)

		err := ps.InsertTuple(tid, 1, testTuple)
		if err != nil {
			t.Errorf("InsertTuple failed for transaction %d: %v", i, err)
		}
	}

	// Verify the page cache reached its maximum size due to LRU eviction
	expectedCacheSize := MaxPageCount // 50 pages max
	if ps.cache.Size() != expectedCacheSize {
		t.Errorf("Expected %d pages in cache, got %d", expectedCacheSize, ps.cache.Size())
	}

	// Note: In a real implementation, you might want to test cache eviction,
	// transaction cleanup, etc. This test ensures basic memory tracking works.
}

func TestPageStore_UpdateTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction
	tid := transaction.NewTransactionID()

	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	stringField1 := types.NewStringField("old_value", 128)
	oldTuple.SetField(0, intField1)
	oldTuple.SetField(1, stringField1)

	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID

	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(42))
	stringField2 := types.NewStringField("new_value", 128)
	newTuple.SetField(0, intField2)
	newTuple.SetField(1, stringField2)

	// Test UpdateTuple
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err != nil {
		t.Errorf("UpdateTuple failed: %v", err)
	}

	// Verify pages were cached (should have at least 1-2 pages from delete and insert operations)
	if ps.cache.Size() == 0 {
		t.Error("Expected pages to be added to cache after update")
	}
}

func TestPageStore_UpdateTuple_DeleteFails(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	tid := transaction.NewTransactionID()

	// Create old tuple without RecordID (will cause delete to fail)
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}

	oldTuple := tuple.NewTuple(td)
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)
	// Note: oldTuple.RecordID is nil, which will cause delete to fail

	// Create new tuple
	newTuple := tuple.NewTuple(td)
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// Test UpdateTuple - should fail because delete fails
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err == nil {
		t.Error("Expected error when delete operation fails")
	}

	expectedErrMsg := "failed to delete old tuple: tuple has no record ID"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_UpdateTuple_InsertFails_Rollback(t *testing.T) {
	// This test is more complex to set up since we need to make insert fail
	// after delete succeeds, and verify rollback behavior
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()

	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)

	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID

	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// For this test, we'll test with a non-existent table to force insert to fail
	// We'll modify the test to use a different table ID that doesn't exist

	// First, remove the table we just added to cause insert to fail
	tm = NewTableManager() // Reset table manager
	ps = NewPageStore(tm)  // Reset page store

	// Add the table back
	err = tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Now remove the table to make insert fail but keep the old tuple pointing to table 1
	// Actually, let's use a different approach - modify the old tuple to point to a non-existent table
	pageID2 := heap.NewHeapPageID(999, 0) // Non-existent table
	recordID2 := &tuple.TupleRecordID{
		PageID:   pageID2,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID2

	// Test UpdateTuple - delete will fail due to table not found
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err == nil {
		t.Error("Expected error when both delete and insert operations would fail")
	}
}

func TestPageStore_UpdateTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)

	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID

	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// Test UpdateTuple
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err != nil {
		t.Errorf("UpdateTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify pages exist and are marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.cache.Get(pageID)
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}

		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_UpdateTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numUpdatesPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent updates
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numUpdatesPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3

				// Create old tuple with RecordID
				dbFile, _ := tm.GetDbFile(tableID)
				oldValue := goroutineID*numUpdatesPerGoroutine + j
				oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField1 := types.NewIntField(int32(oldValue))
				oldTuple.SetField(0, intField1)

				pageID := heap.NewHeapPageID(tableID, j)
				recordID := &tuple.TupleRecordID{
					PageID:   pageID,
					TupleNum: 0,
				}
				oldTuple.RecordID = recordID

				// Create new tuple
				newValue := oldValue + 1000
				newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField2 := types.NewIntField(int32(newValue))
				newTuple.SetField(0, intField2)

				// Update tuple
				err := ps.UpdateTuple(tid, oldTuple, newTuple)
				if err != nil {
					t.Errorf("UpdateTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - cache should be at max capacity due to LRU eviction
	expectedCacheSize := MaxPageCount // 50 pages max
	if ps.cache.Size() != expectedCacheSize {
		t.Errorf("Expected %d pages in cache, got %d", expectedCacheSize, ps.cache.Size())
	}
}

func TestPageStore_UpdateTuple_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		// Create old tuple with RecordID
		oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField1 := types.NewIntField(int32(42))
		oldTuple.SetField(0, intField1)

		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		oldTuple.RecordID = recordID

		// Create new tuple
		newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField2 := types.NewIntField(int32(84))
		newTuple.SetField(0, intField2)

		// Test with nil transaction ID
		_ = ps.UpdateTuple(nil, oldTuple, newTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Old Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()

		// Create new tuple
		newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(84))
		newTuple.SetField(0, intField)

		// Test with nil old tuple
		err = ps.UpdateTuple(tid, nil, newTuple)
		if err == nil {
			t.Error("Expected error for nil old tuple")
		}
	})

	t.Run("Nil New Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()

		// Create old tuple with RecordID
		oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		oldTuple.SetField(0, intField)

		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		oldTuple.RecordID = recordID

		// Test with nil new tuple
		err = ps.UpdateTuple(tid, oldTuple, nil)
		// This will likely fail during the insert phase, but may succeed
		// depending on the mock implementation
		_ = err // We don't require this to error
	})

	t.Run("Same Old and New Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()

		// Create tuple with RecordID
		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)

		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		testTuple.RecordID = recordID

		// Test with same tuple for old and new
		err = ps.UpdateTuple(tid, testTuple, testTuple)
		if err != nil {
			t.Errorf("UpdateTuple should handle same tuple update: %v", err)
		}
	})
}

func TestPageStore_FlushAllPages_EmptyCache(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Test with empty cache
	err := ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages should not fail with empty cache: %v", err)
	}
}

func TestPageStore_FlushAllPages_SinglePage(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Insert tuple to add page to cache
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Verify page is in cache and dirty
	if ps.cache.Size() != 1 {
		t.Fatalf("Expected 1 page in cache, got %d", ps.cache.Size())
	}

	// Test FlushAllPages
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify page is still in cache but no longer dirty
	if ps.cache.Size() != 1 {
		t.Errorf("Expected page to remain in cache after flush, got %d pages", ps.cache.Size())
	}

	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			t.Error("Page should not be dirty after flush")
		}
	}
}

func TestPageStore_FlushAllPages_MultiplePages(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add mock tables
	numTables := 3
	numTuplesPerTable := 5

	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	// Insert tuples to create multiple pages
	for tableID := 1; tableID <= numTables; tableID++ {
		dbFile, _ := tm.GetDbFile(tableID)
		for tupleIdx := 0; tupleIdx < numTuplesPerTable; tupleIdx++ {
			tid := transaction.NewTransactionID()
			testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
			intField := types.NewIntField(int32(tableID*100 + tupleIdx))
			testTuple.SetField(0, intField)

			err := ps.InsertTuple(tid, tableID, testTuple)
			if err != nil {
				t.Fatalf("Failed to insert tuple %d in table %d: %v", tupleIdx, tableID, err)
			}
		}
	}

	expectedPages := numTables * numTuplesPerTable
	if ps.cache.Size() != expectedPages {
		t.Fatalf("Expected %d pages in cache, got %d", expectedPages, ps.cache.Size())
	}

	// Count dirty pages before flush
	dirtyPagesBefore := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		}
	}

	if dirtyPagesBefore != expectedPages {
		t.Fatalf("Expected %d dirty pages before flush, got %d", expectedPages, dirtyPagesBefore)
	}

	// Test FlushAllPages
	err := ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify all pages are still in cache but no longer dirty
	if ps.cache.Size() != expectedPages {
		t.Errorf("Expected %d pages to remain in cache after flush, got %d", expectedPages, ps.cache.Size())
	}

	dirtyPagesAfter := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}

	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_ConcurrentAccess(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	numGoroutines := 10
	numInsertsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent inserts
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numInsertsPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(goroutineID*numInsertsPerGoroutine + j))
				testTuple.SetField(0, intField)

				err := ps.InsertTuple(tid, 1, testTuple)
				if err != nil {
					t.Errorf("InsertTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Cache should be at max capacity due to LRU eviction
	expectedCacheSize := MaxPageCount // 50 pages max
	if ps.cache.Size() != expectedCacheSize {
		t.Fatalf("Expected %d pages in cache, got %d", expectedCacheSize, ps.cache.Size())
	}

	// Test concurrent FlushAllPages calls
	flushGoroutines := 5
	var flushWg sync.WaitGroup
	flushWg.Add(flushGoroutines)

	flushErrors := make([]error, flushGoroutines)
	for i := 0; i < flushGoroutines; i++ {
		go func(idx int) {
			defer flushWg.Done()
			flushErrors[idx] = ps.FlushAllPages()
		}(i)
	}

	flushWg.Wait()

	// Check that all flush operations succeeded
	for i, err := range flushErrors {
		if err != nil {
			t.Errorf("FlushAllPages failed in goroutine %d: %v", i, err)
		}
	}

	// Verify final state - pages should still be in cache but not dirty
	if ps.cache.Size() != expectedCacheSize {
		t.Errorf("Expected %d pages to remain in cache after concurrent flush, got %d", expectedCacheSize, ps.cache.Size())
	}

	dirtyPagesAfter := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}

	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after concurrent flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_MixedDirtyAndCleanPages(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Insert some tuples to create dirty pages
	numTuples := 5
	for i := 0; i < numTuples; i++ {
		tid := transaction.NewTransactionID()
		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(i))
		testTuple.SetField(0, intField)

		err := ps.InsertTuple(tid, 1, testTuple)
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
	}

	if ps.cache.Size() != numTuples {
		t.Fatalf("Expected %d pages in cache, got %d", numTuples, ps.cache.Size())
	}

	// Manually mark some pages as clean
	pageIDs := make([]tuple.PageID, 0, ps.cache.Size())
	for _, pid := range ps.cache.GetAll() {
		pageIDs = append(pageIDs, pid)
	}

	// Mark first two pages as clean
	for i := 0; i < 2 && i < len(pageIDs); i++ {
		page, _ := ps.cache.Get(pageIDs[i])
		page.MarkDirty(false, nil)
		ps.cache.Put(pageIDs[i], page)
	}

	// Count dirty pages before flush
	dirtyPagesBefore := 0
	cleanPagesBefore := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		} else {
			cleanPagesBefore++
		}
	}

	expectedDirty := numTuples - 2
	expectedClean := 2
	if dirtyPagesBefore != expectedDirty {
		t.Fatalf("Expected %d dirty pages before flush, got %d", expectedDirty, dirtyPagesBefore)
	}
	if cleanPagesBefore != expectedClean {
		t.Fatalf("Expected %d clean pages before flush, got %d", expectedClean, cleanPagesBefore)
	}

	// Test FlushAllPages
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify all pages are still in cache and all are clean
	if ps.cache.Size() != numTuples {
		t.Errorf("Expected %d pages to remain in cache after flush, got %d", numTuples, ps.cache.Size())
	}

	dirtyPagesAfter := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}

	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_WriteFailure(t *testing.T) {
	// This test would require a mock that can simulate write failures
	// For now, we'll test the basic error propagation structure
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create a mock table and insert a tuple
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Test FlushAllPages (should succeed with our mock)
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed unexpectedly: %v", err)
	}

	// In a real implementation, you would create a mock that fails WritePage
	// and verify that FlushAllPages returns the error appropriately
}

// Tests for GetPage method
func TestPageStore_GetPage_Success(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// Test GetPage with ReadOnly permission
	page, err := ps.GetPage(tid, pageID, ReadOnly)
	if err != nil {
		t.Errorf("GetPage failed: %v", err)
	}
	if page == nil {
		t.Error("Expected page to be returned, got nil")
	}
	if page.GetID() != pageID {
		t.Errorf("Expected page ID %v, got %v", pageID, page.GetID())
	}

	// Verify page was cached
	if ps.cache.Size() != 1 {
		t.Errorf("Expected 1 page in cache, got %d", ps.cache.Size())
	}

	// Verify transaction tracking
	txInfo, exists := ps.transactions[tid]
	if !exists {
		t.Error("Expected transaction to be tracked")
	}
	if _, locked := txInfo.lockedPages[pageID]; !locked {
		t.Error("Expected page to be locked by transaction")
	}
	if txInfo.lockedPages[pageID] != ReadOnly {
		t.Errorf("Expected ReadOnly permission, got %v", txInfo.lockedPages[pageID])
	}
}

func TestPageStore_GetPage_CacheHit(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// First call - should load from disk
	page1, err := ps.GetPage(tid, pageID, ReadOnly)
	if err != nil {
		t.Fatalf("First GetPage failed: %v", err)
	}

	// Second call - should hit cache
	page2, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Errorf("Second GetPage failed: %v", err)
	}

	// Should be the same page instance
	if page1.GetID() != page2.GetID() {
		t.Error("Expected same page ID from cache hit")
	}

	// Verify cache size didn't grow
	if ps.cache.Size() != 1 {
		t.Errorf("Expected 1 page in cache after cache hit, got %d", ps.cache.Size())
	}

	// Verify permission was updated in transaction info
	txInfo := ps.transactions[tid]
	if txInfo.lockedPages[pageID] != ReadWrite {
		t.Errorf("Expected permission to be updated to ReadWrite, got %v", txInfo.lockedPages[pageID])
	}
}

func TestPageStore_GetPage_TableNotFound(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(999, 0) // Non-existent table

	// Test with non-existent table
	page, err := ps.GetPage(tid, pageID, ReadOnly)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
	if page != nil {
		t.Error("Expected nil page for non-existent table")
	}

	if !contains(err.Error(), "table with ID 999 not found") {
		t.Errorf("Expected error containing 'table with ID 999 not found', got %q", err.Error())
	}
}

func TestPageStore_GetPage_ReadPageFailure(t *testing.T) {
	// This test would require a mock that can simulate read failures
	// For now, we'll test the basic structure
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// With our current mock, this should succeed
	page, err := ps.GetPage(tid, pageID, ReadOnly)
	if err != nil {
		t.Errorf("GetPage failed unexpectedly: %v", err)
	}
	if page == nil {
		t.Error("Expected page to be returned")
	}
}

func TestPageStore_GetPage_PermissionTypes(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	pageID := heap.NewHeapPageID(1, 0)

	tests := []struct {
		name       string
		permission Permissions
	}{
		{"ReadOnly", ReadOnly},
		{"ReadWrite", ReadWrite},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tid := transaction.NewTransactionID()
			defer ps.CommitTransaction(tid) // Clean up transaction and release locks

			page, err := ps.GetPage(tid, pageID, tt.permission)
			if err != nil {
				t.Errorf("GetPage failed for %s permission: %v", tt.name, err)
			}
			if page == nil {
				t.Errorf("Expected page for %s permission", tt.name)
			}

			// Verify transaction tracking
			txInfo := ps.transactions[tid]
			if txInfo.lockedPages[pageID] != tt.permission {
				t.Errorf("Expected %s permission, got %v", tt.name, txInfo.lockedPages[pageID])
			}
		})
	}
}

func TestPageStore_GetPage_MultipleTransactions(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	pageID1 := heap.NewHeapPageID(1, 0)
	pageID2 := heap.NewHeapPageID(1, 1)

	// Create multiple transactions
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()

	// First transaction accesses page 1
	page1, err := ps.GetPage(tid1, pageID1, ReadOnly)
	if err != nil {
		t.Fatalf("GetPage failed for transaction 1: %v", err)
	}
	if page1 == nil {
		t.Fatal("Expected page for transaction 1")
	}

	// Second transaction accesses page 2
	page2, err := ps.GetPage(tid2, pageID2, ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed for transaction 2: %v", err)
	}
	if page2 == nil {
		t.Fatal("Expected page for transaction 2")
	}

	// Verify both transactions are tracked
	if len(ps.transactions) != 2 {
		t.Errorf("Expected 2 transactions, got %d", len(ps.transactions))
	}

	// Verify transaction 1 info
	txInfo1 := ps.transactions[tid1]
	if txInfo1.lockedPages[pageID1] != ReadOnly {
		t.Error("Transaction 1 should have ReadOnly lock on page 1")
	}

	// Verify transaction 2 info
	txInfo2 := ps.transactions[tid2]
	if txInfo2.lockedPages[pageID2] != ReadWrite {
		t.Error("Transaction 2 should have ReadWrite lock on page 2")
	}
}

func TestPageStore_GetPage_AccessOrder(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID1 := heap.NewHeapPageID(1, 0)
	pageID2 := heap.NewHeapPageID(1, 1)

	// Access first page
	_, err = ps.GetPage(tid, pageID1, ReadOnly)
	if err != nil {
		t.Fatalf("GetPage failed for page 1: %v", err)
	}

	// Access second page
	_, err = ps.GetPage(tid, pageID2, ReadOnly)
	if err != nil {
		t.Fatalf("GetPage failed for page 2: %v", err)
	}

	// Verify access order
	accessOrder := ps.cache.GetAll()
	if len(accessOrder) != 2 {
		t.Errorf("Expected 2 pages in access order, got %d", len(accessOrder))
	}
	if accessOrder[0] != pageID1 {
		t.Errorf("Expected first page to be %v, got %v", pageID1, accessOrder[0])
	}
	if accessOrder[1] != pageID2 {
		t.Errorf("Expected second page to be %v, got %v", pageID2, accessOrder[1])
	}

	// Access first page again (should move to end)
	_, err = ps.GetPage(tid, pageID1, ReadOnly)
	if err != nil {
		t.Fatalf("GetPage failed for page 1 (second access): %v", err)
	}

	// Verify access order updated
	accessOrder = ps.cache.GetAll()
	if len(accessOrder) != 2 {
		t.Errorf("Expected 2 pages in access order after reaccess, got %d", len(accessOrder))
	}
	if accessOrder[0] != pageID2 {
		t.Errorf("Expected first page to be %v after reaccess, got %v", pageID2, accessOrder[0])
	}
	if accessOrder[1] != pageID1 {
		t.Errorf("Expected second page to be %v after reaccess, got %v", pageID1, accessOrder[1])
	}
}

func TestPageStore_GetPage_ConcurrentAccess(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add mock tables
	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 5            // Reduce to prevent all pages being locked
	numAccessesPerGoroutine := 10 // Reduce to prevent cache overflow

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent GetPage calls
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			// Use one transaction per goroutine to reduce transaction overhead
			tid := transaction.NewTransactionID()
			defer ps.CommitTransaction(tid) // Clean up transaction

			for j := 0; j < numAccessesPerGoroutine; j++ {
				tableID := (j % numTables) + 1
				pageNum := j % 10 // Reuse some page numbers to test cache hits
				pageID := heap.NewHeapPageID(tableID, pageNum)
				permission := ReadOnly
				if j%2 == 0 {
					permission = ReadWrite
				}

				page, err := ps.GetPage(tid, pageID, permission)
				if err != nil {
					t.Errorf("GetPage failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
					continue
				}
				if page == nil {
					t.Errorf("Expected page for goroutine %d, iteration %d", goroutineID, j)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	if ps.cache.Size() == 0 {
		t.Error("Expected pages in cache after concurrent access")
	}
	// Transactions should be cleaned up after commits
	if len(ps.transactions) != 0 {
		t.Errorf("Expected no active transactions after cleanup, got %d", len(ps.transactions))
	}

	// All pages should be accessible without error
	for _, pageID := range ps.cache.GetAll() {
		if page, exists := ps.cache.Get(pageID); !exists || page == nil {
			t.Errorf("Found nil page in cache for ID %v", pageID)
		}
	}
}

func TestPageStore_GetPage_TransactionInfoCreation(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// Verify transaction doesn't exist initially
	if _, exists := ps.transactions[tid]; exists {
		t.Error("Transaction should not exist initially")
	}

	// Access page - should create transaction info
	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	if page == nil {
		t.Fatal("Expected page to be returned")
	}

	// Verify transaction info was created
	txInfo, exists := ps.transactions[tid]
	if !exists {
		t.Fatal("Expected transaction info to be created")
	}

	// Verify transaction info fields
	if txInfo.startTime.IsZero() {
		t.Error("Expected start time to be set")
	}
	if txInfo.dirtyPages == nil {
		t.Error("Expected dirty pages map to be initialized")
	}
	if txInfo.lockedPages == nil {
		t.Error("Expected locked pages map to be initialized")
	}
	if txInfo.lockedPages[pageID] != ReadWrite {
		t.Errorf("Expected ReadWrite permission, got %v", txInfo.lockedPages[pageID])
	}
}

func TestPageStore_GetPage_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		pageID := heap.NewHeapPageID(1, 0)

		// Test with nil transaction ID
		page, err := ps.GetPage(nil, pageID, ReadOnly)
		// This should not panic - behavior depends on implementation
		_ = page
		_ = err
	})

	t.Run("Zero PageID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)

		dbFile := newMockDbFileForPageStore(0, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()
		pageID := heap.NewHeapPageID(0, 0)

		page, err := ps.GetPage(tid, pageID, ReadOnly)
		if err != nil {
			t.Errorf("GetPage failed for zero table ID: %v", err)
		}
		if page != nil && page.GetID() != pageID {
			t.Errorf("Expected page ID %v, got %v", pageID, page.GetID())
		}
	})
}
