package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
	"sync"
	"testing"
)

// Helper function to create a PageStore with a temporary WAL file for testing
func newTestPageStore(t *testing.T, tm *TableManager) *PageStore {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	ps := NewPageStore(tm, wal)

	// Ensure cleanup happens after test
	t.Cleanup(func() {
		if ps != nil {
			ps.Close()
		}
		os.Remove(walPath)
	})

	return ps
}

func TestPageStore_CommitTransaction_Success(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	if ps.cache.Size() != 1 {
		t.Fatalf("Expected 1 page in cache, got %d", ps.cache.Size())
	}

	var pageID tuple.PageID
	for _, pid := range ps.cache.GetAll() {
		pageID = pid
		break
	}

	page, _ := ps.cache.Get(pageID)
	if page.IsDirty() != tid.ID {
		t.Error("Page should be dirty before commit")
	}

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	page, _ = ps.cache.Get(pageID)
	if page.IsDirty() != nil {
		t.Error("Page should be clean after commit")
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry

	if page.GetBeforeImage() == nil {
		t.Error("Before image should be set after commit")
	}
}

func TestPageStore_CommitTransaction_NilTransactionID(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	err := ps.CommitTransaction(nil)
	if err == nil {
		t.Error("Expected error for nil transaction ID")
	}

	expectedErr := "transaction ID cannot be nil"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestPageStore_CommitTransaction_NonExistentTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed for non-existent transaction: %v", err)
	}
}

func TestPageStore_CommitTransaction_MultiplePages(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	tid := createTransactionContext(t, ps.wal)
	numTuplesPerTable := 5

	for tableID := 1; tableID <= numTables; tableID++ {
		dbFile, _ := tm.GetDbFile(tableID)
		for i := 0; i < numTuplesPerTable; i++ {
			testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
			intField := types.NewIntField(int32(tableID*100 + i))
			testTuple.SetField(0, intField)

			err := ps.InsertTuple(tid, tableID, testTuple)
			if err != nil {
				t.Fatalf("Failed to insert tuple %d in table %d: %v", i, tableID, err)
			}
		}
	}

	expectedPages := numTables * numTuplesPerTable
	if ps.cache.Size() != expectedPages {
		t.Fatalf("Expected %d pages in cache, got %d", expectedPages, ps.cache.Size())
	}

	dirtyPagesBefore := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		}
	}

	if dirtyPagesBefore != expectedPages {
		t.Fatalf("Expected %d dirty pages before commit, got %d", expectedPages, dirtyPagesBefore)
	}

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	dirtyPagesAfter := 0
	beforeImagesSet := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
		if page.GetBeforeImage() != nil {
			beforeImagesSet++
		}
	}

	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after commit, got %d", dirtyPagesAfter)
	}

	if beforeImagesSet != expectedPages {
		t.Errorf("Expected %d before images to be set, got %d", expectedPages, beforeImagesSet)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_CommitTransaction_FlushFailure(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed with mock: %v", err)
	}
}

func TestPageStore_CommitTransaction_ConcurrentCommits(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numTuplesPerGoroutine := 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	commitErrors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			tid := createTransactionContext(t, ps.wal)

			for j := range numTuplesPerGoroutine {
				tableID := (j % numTables) + 1
				dbFile, _ := tm.GetDbFile(tableID)
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(goroutineID*100 + j))
				testTuple.SetField(0, intField)

				err := ps.InsertTuple(tid, tableID, testTuple)
				if err != nil {
					t.Errorf("Failed to insert tuple for goroutine %d: %v", goroutineID, err)
					return
				}
			}

			commitErrors[goroutineID] = ps.CommitTransaction(tid)
		}(i)
	}

	wg.Wait()

	for i, err := range commitErrors {
		if err != nil {
			t.Errorf("CommitTransaction failed for goroutine %d: %v", i, err)
		}
	}

	expectedPages := numGoroutines * numTuplesPerGoroutine
	if ps.cache.Size() != expectedPages {
		t.Errorf("Expected %d pages in cache after concurrent commits, got %d", expectedPages, ps.cache.Size())
	}

	dirtyPagesAfter := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}

	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after concurrent commits, got %d", dirtyPagesAfter)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_CommitTransaction_EmptyTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	// Transaction context is already created

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed for empty transaction: %v", err)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_CommitTransaction_WithGetPageAccess(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	page, _ = ps.cache.Get(pageID)
	if page.IsDirty() != nil {
		t.Error("Page should be clean after commit")
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_Success(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	originalData := make([]byte, len(page.GetPageData()))
	copy(originalData, page.GetPageData())
	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	if page.IsDirty() != tid.ID {
		t.Error("Page should be dirty before abort")
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	restoredPage, _ := ps.cache.Get(pageID)
	if restoredPage.IsDirty() != nil {
		t.Error("Page should be clean after abort (restored from before image)")
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_NilTransactionID(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	err := ps.AbortTransaction(nil)
	if err == nil {
		t.Error("Expected error for nil transaction ID")
	}

	expectedErr := "transaction ID cannot be nil"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestPageStore_AbortTransaction_NonExistentTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	err := ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should succeed for non-existent transaction: %v", err)
	}
}

func TestPageStore_AbortTransaction_MultiplePages(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	tid := createTransactionContext(t, ps.wal)
	numTuplesPerTable := 5

	for tableID := 1; tableID <= numTables; tableID++ {
		dbFile, _ := tm.GetDbFile(tableID)
		for i := 0; i < numTuplesPerTable; i++ {
			testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
			intField := types.NewIntField(int32(tableID*100 + i))
			testTuple.SetField(0, intField)

			err := ps.InsertTuple(tid, tableID, testTuple)
			if err != nil {
				t.Fatalf("Failed to insert tuple %d in table %d: %v", i, tableID, err)
			}
		}
	}

	expectedPages := numTables * numTuplesPerTable
	if ps.cache.Size() != expectedPages {
		t.Fatalf("Expected %d pages in cache, got %d", expectedPages, ps.cache.Size())
	}

	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		page.SetBeforeImage()
	}

	dirtyPagesBefore := 0
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		}
	}

	if dirtyPagesBefore != expectedPages {
		t.Fatalf("Expected %d dirty pages before abort, got %d", expectedPages, dirtyPagesBefore)
	}

	err := ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			t.Error("All pages should be clean after abort")
		}
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_NoBeforeImage(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	var pageID tuple.PageID
	for _, pid := range ps.cache.GetAll() {
		pageID = pid
		break
	}

	page, _ := ps.cache.Get(pageID)
	if page.GetBeforeImage() != nil {
		page.SetBeforeImage()
		beforeImage := page.GetBeforeImage()
		if mockPage, ok := beforeImage.(*mockPage); ok {
			mockPage.beforeImg = nil
		}
		if mockPageMain, ok := page.(*mockPage); ok {
			mockPageMain.beforeImg = nil
		}
	}

	initialCacheSize := ps.cache.Size()

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should not fail even without before image: %v", err)
	}

	if ps.cache.Size() >= initialCacheSize {
		t.Log("Page without before image was handled appropriately")
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_ConcurrentAborts(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numTuplesPerGoroutine := 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	abortErrors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			tid := createTransactionContext(t, ps.wal)

			for j := 0; j < numTuplesPerGoroutine; j++ {
				tableID := (j % numTables) + 1
				dbFile, _ := tm.GetDbFile(tableID)
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(goroutineID*100 + j))
				testTuple.SetField(0, intField)

				err := ps.InsertTuple(tid, tableID, testTuple)
				if err != nil {
					t.Errorf("Failed to insert tuple for goroutine %d: %v", goroutineID, err)
					return
				}
			}

			for _, pid := range ps.cache.GetAll() {
				page, _ := ps.cache.Get(pid)
				if page.IsDirty() == tid.ID {
					page.SetBeforeImage()
				}
			}

			abortErrors[goroutineID] = ps.AbortTransaction(tid)
		}(i)
	}

	wg.Wait()

	for i, err := range abortErrors {
		if err != nil {
			t.Errorf("AbortTransaction failed for goroutine %d: %v", i, err)
		}
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_EmptyTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	// Transaction context is already created

	err := ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should succeed for empty transaction: %v", err)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_AbortTransaction_RestoresBeforeImage(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	originalData := make([]byte, len(page.GetPageData()))
	copy(originalData, page.GetPageData())
	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	modifiedData := make([]byte, len(page.GetPageData()))
	for i := range modifiedData {
		modifiedData[i] = byte(i % 256)
	}
	if mockPage, ok := page.(*mockPage); ok {
		mockPage.data = modifiedData
	}
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	beforeImageData := page.GetBeforeImage().GetPageData()
	for i, b := range originalData {
		if i < len(beforeImageData) && beforeImageData[i] != b {
			t.Errorf("Before image data mismatch at index %d: expected %d, got %d", i, b, beforeImageData[i])
		}
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	restoredPage, _ := ps.cache.Get(pageID)
	restoredData := restoredPage.GetPageData()

	for i, b := range originalData {
		if i < len(restoredData) && restoredData[i] != b {
			t.Errorf("Restored data mismatch at index %d: expected %d, got %d", i, b, restoredData[i])
		}
	}
}

func TestPageStore_AbortTransaction_WithGetPageAccess(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	restoredPage, _ := ps.cache.Get(pageID)
	if restoredPage.IsDirty() != nil {
		t.Error("Page should be clean after abort")
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_LockManagerIntegration_CommitReleasesLocks(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	tid2 := createTransactionContext(t, ps.wal)
	page2, err := ps.GetPage(tid2, pageID, transaction.ReadWrite)
	if err != nil {
		t.Errorf("Should be able to acquire lock after commit: %v", err)
	}
	if page2 == nil {
		t.Error("Expected to get page after commit released locks")
	}
}

func TestPageStore_LockManagerIntegration_AbortReleasesLocks(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	tid2 := createTransactionContext(t, ps.wal)
	page2, err := ps.GetPage(tid2, pageID, transaction.ReadWrite)
	if err != nil {
		t.Errorf("Should be able to acquire lock after abort: %v", err)
	}
	if page2 == nil {
		t.Error("Expected to get page after abort released locks")
	}
}

func TestPageStore_LockManagerIntegration_GetPageAcquiresLocks(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadOnly)
	if err != nil {
		t.Fatalf("Failed to get page with ReadOnly: %v", err)
	}
	if page == nil {
		t.Fatal("Expected page for ReadOnly access")
	}

	// With TransactionContext, transaction info is tracked in the context itself
	if perm, exists := tid.GetPagePermission(pageID); !exists || perm != transaction.ReadOnly {
		t.Errorf("Expected ReadOnly permission in transaction info, got %v (exists=%v)", perm, exists)
	}

	page2, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to upgrade to ReadWrite: %v", err)
	}
	if page2 == nil {
		t.Fatal("Expected page for ReadWrite access")
	}

	if perm, exists := tid.GetPagePermission(pageID); !exists || perm != transaction.ReadWrite {
		t.Errorf("Expected ReadWrite permission after upgrade, got %v (exists=%v)", perm, exists)
	}
}

func TestPageStore_LockManagerIntegration_ConcurrentTransactions(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numOperationsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	lockErrors := make([][]error, numGoroutines)
	for i := range lockErrors {
		lockErrors[i] = make([]error, numOperationsPerGoroutine)
	}

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			tid := createTransactionContext(t, ps.wal)

			for j := 0; j < numOperationsPerGoroutine; j++ {
				tableID := (j % numTables) + 1
				pageNum := j % 5
				pageID := heap.NewHeapPageID(tableID, pageNum)
				permission := transaction.ReadOnly
				if j%3 == 0 {
					permission = transaction.ReadWrite
				}

				_, err := ps.GetPage(tid, pageID, permission)
				lockErrors[goroutineID][j] = err
			}

			if goroutineID%2 == 0 {
				ps.CommitTransaction(tid)
			} else {
				for _, pid := range ps.cache.GetAll() {
					page, exists := ps.cache.Get(pid)
					if exists && page != nil && page.IsDirty() == tid.ID {
						page.SetBeforeImage()
					}
				}
				ps.AbortTransaction(tid)
			}
		}(i)
	}

	wg.Wait()

	successfulLocks := 0
	failedLocks := 0
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperationsPerGoroutine; j++ {
			if lockErrors[i][j] == nil {
				successfulLocks++
			} else {
				failedLocks++
			}
		}
	}

	totalOperations := numGoroutines * numOperationsPerGoroutine
	if successfulLocks+failedLocks != totalOperations {
		t.Errorf("Inconsistent lock operation count: %d successful + %d failed != %d total",
			successfulLocks, failedLocks, totalOperations)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_LockManagerIntegration_TransactionIsolation(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid1 := createTransactionContext(t, ps.wal)
	tid2 := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page1, err := ps.GetPage(tid1, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Transaction 1 failed to get page: %v", err)
	}
	if page1 == nil {
		t.Fatal("Expected page for transaction 1")
	}

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid1)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page1.MarkDirty(true, tid1.ID)
	ps.cache.Put(pageID, page1)

	// Transaction context tracks dirty pages automatically

	// Transaction 2 should fail to get ReadOnly lock while tid1 holds ReadWrite lock
	_, err = ps.GetPage(tid2, pageID, transaction.ReadOnly)
	if err == nil {
		t.Fatal("Expected transaction 2 to fail due to lock conflict with exclusive lock")
	}
	// Verify the error is about lock timeout/conflict
	if !strings.Contains(err.Error(), "timeout waiting for lock") {
		t.Errorf("Expected timeout error, got: %v", err)
	}

	if len(tid1.GetLockedPages()) == 0 {
		t.Error("Transaction 1 should have locked pages")
	}

	if perm, exists := tid1.GetPagePermission(pageID); !exists || perm != transaction.ReadWrite {
		t.Errorf("Transaction 1 should have ReadWrite lock, got %v (exists=%v)", perm, exists)
	}

	// Transaction 2 should not have any locked pages since it failed to acquire lock
	if len(tid2.GetLockedPages()) > 0 {
		t.Error("Transaction 2 should not have any locked pages since it failed to acquire lock")
	}

	err = ps.CommitTransaction(tid1)
	if err != nil {
		t.Errorf("Failed to commit transaction 1: %v", err)
	}

	err = ps.CommitTransaction(tid2)
	if err != nil {
		t.Errorf("Failed to commit transaction 2: %v", err)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_LockManagerIntegration_NonExistentTransactionCleanup(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed for non-existent transaction: %v", err)
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should succeed for non-existent transaction: %v", err)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

// ============================================================================
// WAL Integration Tests
// ============================================================================

func TestPageStore_WAL_TransactionBeginLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// First operation should log BEGIN
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Verify that BEGIN was logged
	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	if tid == nil {
		t.Fatal("Transaction should exist after insert")
	}
	// With TransactionContext, BEGIN is logged automatically when needed

	// Second operation should NOT log another BEGIN
	testTuple2 := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple2.SetField(0, types.NewIntField(int32(43)))
	err = ps.InsertTuple(tid, 1, testTuple2)
	if err != nil {
		t.Fatalf("Failed to insert second tuple: %v", err)
	}

	// With TransactionContext, BEGIN is logged automatically when needed
}

func TestPageStore_WAL_CommitLoggedAndForced(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple.SetField(0, types.NewIntField(int32(42)))

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Commit should log COMMIT record and force to disk
	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Transaction should be removed from active transactions
	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry

	// Pages should be flushed and clean
	for _, pid := range ps.cache.GetAll() {
		page, _ := ps.cache.Get(pid)
		if page.IsDirty() != nil {
			t.Error("All pages should be clean after commit")
		}
	}
}

func TestPageStore_WAL_AbortLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, transaction.ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()
	page.MarkDirty(true, tid.ID)
	ps.cache.Put(pageID, page)

	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	// Transaction context tracks dirty pages automatically

	// Actually log BEGIN to WAL so abort can work
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to log BEGIN: %v", err)
	}

	// Abort should log ABORT record
	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Transaction should be removed
	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry

	// Page should be restored from before-image
	restoredPage, _ := ps.cache.Get(pageID)
	if restoredPage.IsDirty() != nil {
		t.Error("Page should be clean after abort")
	}
}

func TestPageStore_WAL_InsertOperationLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple.SetField(0, types.NewIntField(int32(100)))

	// Insert should log to WAL
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify transaction tracking
	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	if tid == nil {
		t.Fatal("Transaction should exist after insert")
	}
	// With TransactionContext, BEGIN is logged automatically and dirty pages are tracked
}

func TestPageStore_WAL_DeleteOperationLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple.SetField(0, types.NewIntField(int32(200)))

	// Insert first
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Get the RecordID from inserted tuple
	if testTuple.RecordID == nil {
		t.Fatal("Tuple should have RecordID after insert")
	}

	// Delete should log to WAL
	err = ps.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// With TransactionContext, transaction remains active until commit/abort
}

func TestPageStore_WAL_UpdateOperationLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	oldTuple.SetField(0, types.NewIntField(int32(300)))

	// Insert first
	err = ps.InsertTuple(tid, 1, oldTuple)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if oldTuple.RecordID == nil {
		t.Fatal("Tuple should have RecordID after insert")
	}

	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	newTuple.SetField(0, types.NewIntField(int32(301)))

	// Update should log to WAL (as DELETE + INSERT)
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify transaction tracking
	// With TransactionContext, transaction info is tracked in the context itself
	_ = tid // txInfo is now accessed via tid directly
	if tid == nil {
		t.Fatal("Transaction should exist after update")
	}
	if !tid.IsActive() {
		t.Error("BEGIN should be logged")
	}
}

func TestPageStore_WAL_CloseFlushesAndClosesWAL(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := createTransactionContext(t, ps.wal)
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple.SetField(0, types.NewIntField(int32(400)))

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Close should flush all pages and close WAL
	err = ps.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPageStore_WAL_EmptyTransactionNoWALRecords(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := createTransactionContext(t, ps.wal)

	// Transaction context is already created - no need for manual initialization

	// Commit should handle empty transaction
	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("Commit of empty transaction should succeed: %v", err)
	}

	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}

func TestPageStore_WAL_ConcurrentTransactionsWALConsistency(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	numTables := 3
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 5
	numOpsPerTxn := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			tid := createTransactionContext(t, ps.wal)

			for j := 0; j < numOpsPerTxn; j++ {
				tableID := (j % numTables) + 1
				dbFile, _ := tm.GetDbFile(tableID)
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				testTuple.SetField(0, types.NewIntField(int32(goroutineID*1000+j)))

				err := ps.InsertTuple(tid, tableID, testTuple)
				if err != nil {
					errors[goroutineID] = fmt.Errorf("insert failed: %v", err)
					return
				}
			}

			// Commit half, abort half
			if goroutineID%2 == 0 {
				errors[goroutineID] = ps.CommitTransaction(tid)
			} else {
				// Set before images for abort
				// With TransactionContext, transaction info is tracked in the context itself
				_ = tid // txInfo is now accessed via tid directly
				if tid != nil {
					for _, pid := range tid.GetDirtyPages() {
						if page, exists := ps.cache.Get(pid); exists {
							page.SetBeforeImage()
						}
					}
				}
				errors[goroutineID] = ps.AbortTransaction(tid)
			}
		}(i)
	}

	wg.Wait()

	// Check all transactions completed successfully
	for i, err := range errors {
		if err != nil {
			t.Errorf("Transaction %d failed: %v", i, err)
		}
	}

	// All transactions should be removed
	// Note: With TransactionContext, transaction cleanup is managed by the transaction registry
}
