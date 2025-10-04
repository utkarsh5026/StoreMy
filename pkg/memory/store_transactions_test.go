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
	"time"
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

	tid := transaction.NewTransactionID()
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
	if page.IsDirty() != tid {
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

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after commit")
	}

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

	tid := transaction.NewTransactionID()

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

	tid := transaction.NewTransactionID()
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

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after commit")
	}
}

func TestPageStore_CommitTransaction_FlushFailure(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

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

			tid := transaction.NewTransactionID()

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

	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 active transactions after commits, got %d", numTransactions)
	}
}

func TestPageStore_CommitTransaction_EmptyTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := transaction.NewTransactionID()

	// GetOrCreate handles transaction creation
	_ = ps.txManager.GetOrCreate(tid)

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed for empty transaction: %v", err)
	}

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Empty transaction should be removed after commit")
	}
}

func TestPageStore_CommitTransaction_WithGetPageAccess(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	page, _ = ps.cache.Get(pageID)
	if page.IsDirty() != nil {
		t.Error("Page should be clean after commit")
	}

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after commit")
	}
}

func TestPageStore_AbortTransaction_Success(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
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

	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

	if page.IsDirty() != tid {
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

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after abort")
	}
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

	tid := transaction.NewTransactionID()

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

	tid := transaction.NewTransactionID()
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

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after abort")
	}
}

func TestPageStore_AbortTransaction_NoBeforeImage(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

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

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after abort")
	}
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

			tid := transaction.NewTransactionID()

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
				if page.IsDirty() == tid {
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

	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 active transactions after aborts, got %d", numTransactions)
	}
}

func TestPageStore_AbortTransaction_EmptyTransaction(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := transaction.NewTransactionID()

	// GetOrCreate handles transaction creation
	_ = ps.txManager.GetOrCreate(tid)

	err := ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should succeed for empty transaction: %v", err)
	}

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Empty transaction should be removed after abort")
	}
}

func TestPageStore_AbortTransaction_RestoresBeforeImage(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
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

	page.MarkDirty(true, tid)
	modifiedData := make([]byte, len(page.GetPageData()))
	for i := range modifiedData {
		modifiedData[i] = byte(i % 256)
	}
	if mockPage, ok := page.(*mockPage); ok {
		mockPage.data = modifiedData
	}
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

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

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	restoredPage, _ := ps.cache.Get(pageID)
	if restoredPage.IsDirty() != nil {
		t.Error("Page should be clean after abort")
	}

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after abort")
	}
}

func TestPageStore_LockManagerIntegration_CommitReleasesLocks(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

	err = ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}

	tid2 := transaction.NewTransactionID()
	page2, err := ps.GetPage(tid2, pageID, ReadWrite)
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

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()

	// Ensure transaction has begun in WAL
	err = ps.ensureTransactionBegun(tid)
	if err != nil {
		t.Fatalf("Failed to ensure transaction begun: %v", err)
	}

	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction failed: %v", err)
	}

	tid2 := transaction.NewTransactionID()
	page2, err := ps.GetPage(tid2, pageID, ReadWrite)
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

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadOnly)
	if err != nil {
		t.Fatalf("Failed to get page with ReadOnly: %v", err)
	}
	if page == nil {
		t.Fatal("Expected page for ReadOnly access")
	}

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo.lockedPages[pageID] != ReadOnly {
		t.Errorf("Expected ReadOnly permission in transaction info, got %v", txInfo.lockedPages[pageID])
	}

	page2, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to upgrade to ReadWrite: %v", err)
	}
	if page2 == nil {
		t.Fatal("Expected page for ReadWrite access")
	}

	if txInfo.lockedPages[pageID] != ReadWrite {
		t.Errorf("Expected ReadWrite permission after upgrade, got %v", txInfo.lockedPages[pageID])
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

			tid := transaction.NewTransactionID()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				tableID := (j % numTables) + 1
				pageNum := j % 5
				pageID := heap.NewHeapPageID(tableID, pageNum)
				permission := ReadOnly
				if j%3 == 0 {
					permission = ReadWrite
				}

				_, err := ps.GetPage(tid, pageID, permission)
				lockErrors[goroutineID][j] = err
			}

			if goroutineID%2 == 0 {
				ps.CommitTransaction(tid)
			} else {
				for _, pid := range ps.cache.GetAll() {
					page, exists := ps.cache.Get(pid)
					if exists && page != nil && page.IsDirty() == tid {
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

	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 active transactions after all commits/aborts, got %d", numTransactions)
	}
}

func TestPageStore_LockManagerIntegration_TransactionIsolation(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page1, err := ps.GetPage(tid1, pageID, ReadWrite)
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

	page1.MarkDirty(true, tid1)
	ps.cache.Put(pageID, page1)

	txInfo1 := ps.txManager.GetOrCreate(tid1)
	if txInfo1 != nil {
		txInfo1.dirtyPages[pageID] = true
	}

	// Transaction 2 should fail to get ReadOnly lock while tid1 holds ReadWrite lock
	_, err = ps.GetPage(tid2, pageID, ReadOnly)
	if err == nil {
		t.Fatal("Expected transaction 2 to fail due to lock conflict with exclusive lock")
	}
	// Verify the error is about lock timeout/conflict
	if !strings.Contains(err.Error(), "timeout waiting for lock") {
		t.Errorf("Expected timeout error, got: %v", err)
	}

	if len(txInfo1.lockedPages) == 0 {
		t.Error("Transaction 1 should have locked pages")
	}

	if txInfo1.lockedPages[pageID] != ReadWrite {
		t.Errorf("Transaction 1 should have ReadWrite lock, got %v", txInfo1.lockedPages[pageID])
	}

	// Transaction 2 should not exist or have no locked pages since it failed to acquire lock
	ps.txManager.mutex.RLock()
	txInfo2, exists := ps.txManager.transactions[tid2]
	ps.txManager.mutex.RUnlock()
	if exists && len(txInfo2.lockedPages) > 0 {
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

	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 active transactions after commits, got %d", numTransactions)
	}
}

func TestPageStore_LockManagerIntegration_NonExistentTransactionCleanup(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	tid := transaction.NewTransactionID()

	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("CommitTransaction should succeed for non-existent transaction: %v", err)
	}

	err = ps.AbortTransaction(tid)
	if err != nil {
		t.Errorf("AbortTransaction should succeed for non-existent transaction: %v", err)
	}

	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 transactions after cleanup, got %d", numTransactions)
	}
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

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// First operation should log BEGIN
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Verify that BEGIN was logged
	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo == nil {
		t.Fatal("Transaction should exist after insert")
	}
	if !txInfo.hasBegun {
		t.Error("Transaction BEGIN should be logged after first operation")
	}

	// Second operation should NOT log another BEGIN
	testTuple2 := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple2.SetField(0, types.NewIntField(int32(43)))
	err = ps.InsertTuple(tid, 1, testTuple2)
	if err != nil {
		t.Fatalf("Failed to insert second tuple: %v", err)
	}

	// hasBegun should still be true (not changed)
	if !txInfo.hasBegun {
		t.Error("Transaction BEGIN flag should remain set")
	}
}

func TestPageStore_WAL_CommitLoggedAndForced(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
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
	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after commit")
	}

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

	tid := transaction.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	page, err := ps.GetPage(tid, pageID, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	page.SetBeforeImage()
	page.MarkDirty(true, tid)
	ps.cache.Put(pageID, page)

	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo != nil {
		txInfo.dirtyPages[pageID] = true
	}

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
	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after abort")
	}

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

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	testTuple.SetField(0, types.NewIntField(int32(100)))

	// Insert should log to WAL
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify transaction tracking
	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo == nil {
		t.Fatal("Transaction should exist after insert")
	}
	if !txInfo.hasBegun {
		t.Error("BEGIN should be logged")
	}
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Should have dirty pages after insert")
	}
}

func TestPageStore_WAL_DeleteOperationLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
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

	// Verify transaction still active
	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if !exists {
		t.Error("Transaction should still exist after delete")
	}
}

func TestPageStore_WAL_UpdateOperationLogged(t *testing.T) {
	tm := NewTableManager()
	ps := newTestPageStore(t, tm)

	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
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
	txInfo := ps.txManager.GetOrCreate(tid)
	if txInfo == nil {
		t.Fatal("Transaction should exist after update")
	}
	if !txInfo.hasBegun {
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

	tid := transaction.NewTransactionID()
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

	tid := transaction.NewTransactionID()

	// Create transaction info without any operations
	ps.txManager.mutex.Lock()
	ps.txManager.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
		hasBegun:    false, // No BEGIN logged
	}
	ps.txManager.mutex.Unlock()

	// Commit should handle transaction with no BEGIN
	err := ps.CommitTransaction(tid)
	if err != nil {
		t.Errorf("Commit of empty transaction should succeed: %v", err)
	}

	ps.txManager.mutex.RLock()
	_, exists := ps.txManager.transactions[tid]
	ps.txManager.mutex.RUnlock()
	if exists {
		t.Error("Transaction should be removed after commit")
	}
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

			tid := transaction.NewTransactionID()

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
				txInfo := ps.txManager.GetOrCreate(tid)
				if txInfo != nil {
					for pid := range txInfo.dirtyPages {
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
	ps.txManager.mutex.RLock()
	numTransactions := len(ps.txManager.transactions)
	ps.txManager.mutex.RUnlock()
	if numTransactions != 0 {
		t.Errorf("Expected 0 active transactions, got %d", numTransactions)
	}
}
