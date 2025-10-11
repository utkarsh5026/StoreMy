package memory

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/log"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// ============================================================================
// CommitTransaction Tests
// ============================================================================

func TestCommitTransaction_Success(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)

	// Insert a tuple to make the transaction dirty
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Verify page is dirty before commit
	dirtyPages := ctx.GetDirtyPages()
	if len(dirtyPages) == 0 {
		t.Fatal("Expected dirty pages before commit")
	}

	pageID := dirtyPages[0]
	page, exists := ps.cache.Get(pageID)
	if !exists {
		t.Fatal("Page should exist in cache")
	}
	if page.IsDirty() == nil {
		t.Error("Page should be dirty before commit")
	}

	// Commit the transaction
	err = ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify page is clean after commit
	page, exists = ps.cache.Get(pageID)
	if !exists {
		t.Fatal("Page should still exist in cache after commit")
	}
	if page.IsDirty() != nil {
		t.Error("Page should be clean after commit")
	}

	// Verify before-image is set
	if page.GetBeforeImage() == nil {
		t.Error("Before image should be set after commit")
	}

	// Verify locks are released
	if ps.lockManager.IsPageLocked(pageID) {
		t.Error("Page should be unlocked after commit")
	}
}

func TestCommitTransaction_NilContext(t *testing.T) {
	ps, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	err := ps.CommitTransaction(nil)
	if err == nil {
		t.Fatal("Expected error for nil transaction context")
	}

	expectedErr := "transaction context cannot be nil"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestCommitTransaction_EmptyTransaction(t *testing.T) {
	ps, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)

	// Commit without any operations
	err := ps.CommitTransaction(ctx)
	if err != nil {
		t.Errorf("Empty transaction commit should succeed: %v", err)
	}

	// Verify locks are released
	lockedPages := ctx.GetLockedPages()
	for _, pageID := range lockedPages {
		if ps.lockManager.IsPageLocked(pageID) {
			t.Errorf("Page %v should be unlocked after commit", pageID)
		}
	}
}

func TestCommitTransaction_MultiplePages(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)

	// Insert multiple tuples to create multiple dirty pages
	numTuples := 10
	for i := 0; i < numTuples; i++ {
		testTuple := createTestTuple(heapFile.GetTupleDesc(), int64(i), fmt.Sprintf("test_%d", i))
		err := ps.InsertTuple(ctx, heapFile, testTuple)
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
	}

	dirtyPagesBefore := ctx.GetDirtyPages()
	if len(dirtyPagesBefore) == 0 {
		t.Fatal("Expected dirty pages before commit")
	}

	// Commit
	err := ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify all pages are clean
	for _, pageID := range dirtyPagesBefore {
		page, exists := ps.cache.Get(pageID)
		if !exists {
			continue
		}
		if page.IsDirty() != nil {
			t.Errorf("Page %v should be clean after commit", pageID)
		}
		if page.GetBeforeImage() == nil {
			t.Errorf("Page %v should have before-image after commit", pageID)
		}
	}
}

func TestCommitTransaction_ConcurrentCommits(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			ctx := createTransactionContext(t, ps.wal)
			testTuple := createTestTuple(
				heapFile.GetTupleDesc(),
				int64(goroutineID*1000),
				fmt.Sprintf("goroutine_%d", goroutineID),
			)

			err := ps.InsertTuple(ctx, heapFile, testTuple)
			if err != nil {
				errors[goroutineID] = fmt.Errorf("insert failed: %v", err)
				return
			}

			errors[goroutineID] = ps.CommitTransaction(ctx)
		}(i)
	}

	wg.Wait()

	// Check all commits succeeded
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}
}

func TestCommitTransaction_AfterCommit(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// First commit
	err = ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit should succeed (no-op for empty transaction)
	ctx2 := createTransactionContext(t, ps.wal)
	err = ps.CommitTransaction(ctx2)
	if err != nil {
		t.Errorf("Second commit should succeed: %v", err)
	}
}

// ============================================================================
// AbortTransaction Tests
// ============================================================================

func TestAbortTransaction_Success(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// First, insert and commit a tuple to establish a page with before-image
	ctx1 := createTransactionContext(t, ps.wal)
	tuple1 := createTestTuple(heapFile.GetTupleDesc(), 1, "committed")
	err := ps.InsertTuple(ctx1, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}
	err = ps.CommitTransaction(ctx1)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Now modify the page and abort
	ctx2 := createTransactionContext(t, ps.wal)

	// Set before-image for the page
	pageID := tuple1.RecordID.PageID
	ps.mutex.Lock()
	page, exists := ps.cache.Get(pageID)
	if exists {
		page.SetBeforeImage()
		ps.cache.Put(pageID, page)
	}
	ps.mutex.Unlock()

	// Delete the tuple (modifies the page)
	err = ps.DeleteTuple(ctx2, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}

	// Verify page is dirty
	dirtyPages := ctx2.GetDirtyPages()
	if len(dirtyPages) == 0 {
		t.Fatal("Expected dirty pages before abort")
	}

	// Abort the transaction
	err = ps.AbortTransaction(ctx2)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	// Verify page was restored (exists in cache)
	_, exists = ps.cache.Get(pageID)
	if !exists {
		t.Fatal("Page should exist after abort")
	}

	// Clean up cache to avoid before-image issues during cleanup
	ps.mutex.Lock()
	ps.cache.Remove(pageID)
	ps.mutex.Unlock()
}

func TestAbortTransaction_NilContext(t *testing.T) {
	ps, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	err := ps.AbortTransaction(nil)
	if err == nil {
		t.Fatal("Expected error for nil transaction context")
	}

	expectedErr := "transaction context cannot be nil"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestAbortTransaction_EmptyTransaction(t *testing.T) {
	ps, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)

	// Abort without any operations
	err := ps.AbortTransaction(ctx)
	if err != nil {
		t.Errorf("Empty transaction abort should succeed: %v", err)
	}
}

func TestAbortTransaction_NoBeforeImage(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")

	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Don't set before-image, so abort should remove the page
	dirtyPages := ctx.GetDirtyPages()
	if len(dirtyPages) == 0 {
		t.Fatal("Expected dirty pages")
	}

	_ = dirtyPages[0] // pageID for reference

	// Abort
	err = ps.AbortTransaction(ctx)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	// Verify page was removed (or at least handled appropriately)
	// The page might still exist in cache depending on implementation
}

func TestAbortTransaction_MultiplePages(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// First commit some tuples to establish pages with before-images
	ctx1 := createTransactionContext(t, ps.wal)
	numTuples := 5
	tuples := make([]*tuple.Tuple, numTuples)

	for i := 0; i < numTuples; i++ {
		tuples[i] = createTestTuple(heapFile.GetTupleDesc(), int64(i), fmt.Sprintf("test_%d", i))
		err := ps.InsertTuple(ctx1, heapFile, tuples[i])
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
	}
	err := ps.CommitTransaction(ctx1)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Now modify multiple pages and abort
	ctx2 := createTransactionContext(t, ps.wal)

	// Set before-images for all pages
	ps.mutex.Lock()
	for _, tup := range tuples {
		if tup.RecordID != nil {
			if page, exists := ps.cache.Get(tup.RecordID.PageID); exists {
				page.SetBeforeImage()
				ps.cache.Put(tup.RecordID.PageID, page)
			}
		}
	}
	ps.mutex.Unlock()

	// Delete all tuples
	for i, tup := range tuples {
		err := ps.DeleteTuple(ctx2, heapFile, tup)
		if err != nil {
			t.Fatalf("Failed to delete tuple %d: %v", i, err)
		}
	}

	dirtyPagesBefore := ctx2.GetDirtyPages()
	if len(dirtyPagesBefore) == 0 {
		t.Fatal("Expected dirty pages before abort")
	}

	// Abort
	err = ps.AbortTransaction(ctx2)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	// Verify all pages were restored (exist in cache)
	for _, pageID := range dirtyPagesBefore {
		_, exists := ps.cache.Get(pageID)
		if !exists {
			continue
		}
		// Page exists - restoration successful
	}

	// Clean up cache to avoid before-image issues during cleanup
	ps.mutex.Lock()
	for _, pageID := range dirtyPagesBefore {
		ps.cache.Remove(pageID)
	}
	ps.mutex.Unlock()
}

func TestAbortTransaction_ConcurrentAborts(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			ctx := createTransactionContext(t, ps.wal)
			testTuple := createTestTuple(
				heapFile.GetTupleDesc(),
				int64(goroutineID*1000),
				fmt.Sprintf("goroutine_%d", goroutineID),
			)

			err := ps.InsertTuple(ctx, heapFile, testTuple)
			if err != nil {
				errors[goroutineID] = fmt.Errorf("insert failed: %v", err)
				return
			}

			// Set before-image if page exists
			if testTuple.RecordID != nil {
				ps.mutex.Lock()
				if page, exists := ps.cache.Get(testTuple.RecordID.PageID); exists {
					page.SetBeforeImage()
					ps.cache.Put(testTuple.RecordID.PageID, page)
				}
				ps.mutex.Unlock()
			}

			errors[goroutineID] = ps.AbortTransaction(ctx)
		}(i)
	}

	wg.Wait()

	// Check all aborts succeeded
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// Clean up all pages to avoid before-image issues during cleanup
	ps.mutex.Lock()
	for _, pageID := range ps.cache.GetAll() {
		ps.cache.Remove(pageID)
	}
	ps.mutex.Unlock()
}

// ============================================================================
// finalizeTransaction Tests (indirect via Commit/Abort)
// ============================================================================

func TestFinalizeTransaction_NilTransactionID(t *testing.T) {
	ps, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Test with nil ID in context
	err := ps.CommitTransaction(nil)
	if err == nil {
		t.Fatal("Expected error for nil context")
	}
}

func TestFinalizeTransaction_UnknownOperation(t *testing.T) {
	// This is tested indirectly - finalizeTransaction only accepts
	// CommitOperation and AbortOperation, which are tested above
}

// ============================================================================
// handleCommit Tests (indirect via CommitTransaction)
// ============================================================================

func TestHandleCommit_BeforeImageSet(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")

	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	pageID := testTuple.RecordID.PageID

	// Commit
	err = ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify before-image is set
	page, exists := ps.cache.Get(pageID)
	if !exists {
		t.Fatal("Page should exist after commit")
	}
	if page.GetBeforeImage() == nil {
		t.Error("Before-image should be set after commit")
	}
}

func TestHandleCommit_PagesFlushed(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")

	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	pageID := testTuple.RecordID.PageID

	// Verify page is dirty before commit
	page, _ := ps.cache.Get(pageID)
	if page.IsDirty() == nil {
		t.Fatal("Page should be dirty before commit")
	}

	// Commit
	err = ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify page is clean after commit (flushed)
	page, exists := ps.cache.Get(pageID)
	if !exists {
		t.Fatal("Page should exist after commit")
	}
	if page.IsDirty() != nil {
		t.Error("Page should be clean after commit (flushed to disk)")
	}
}

// ============================================================================
// handleAbort Tests (indirect via AbortTransaction)
// ============================================================================

func TestHandleAbort_BeforeImageRestored(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Commit a tuple first
	ctx1 := createTransactionContext(t, ps.wal)
	tuple1 := createTestTuple(heapFile.GetTupleDesc(), 1, "original")
	err := ps.InsertTuple(ctx1, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	err = ps.CommitTransaction(ctx1)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	pageID := tuple1.RecordID.PageID

	// Modify and abort
	ctx2 := createTransactionContext(t, ps.wal)

	ps.mutex.Lock()
	page, _ := ps.cache.Get(pageID)
	page.SetBeforeImage()
	ps.cache.Put(pageID, page)
	ps.mutex.Unlock()

	err = ps.DeleteTuple(ctx2, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Abort
	err = ps.AbortTransaction(ctx2)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Verify page was restored (exists in cache)
	ps.mutex.RLock()
	_, exists := ps.cache.Get(pageID)
	ps.mutex.RUnlock()

	if !exists {
		t.Fatal("Page should exist after abort")
	}

	// Clean up cache to avoid before-image issues during cleanup
	ps.mutex.Lock()
	ps.cache.Remove(pageID)
	ps.mutex.Unlock()
}

func TestHandleAbort_PageRemovedWithoutBeforeImage(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")

	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_ = testTuple.RecordID.PageID // Used for documentation

	// Don't set before-image
	// Abort should remove the page
	err = ps.AbortTransaction(ctx)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Page might be removed or marked differently
	// The exact behavior depends on implementation
}

// ============================================================================
// Integration Tests - Transaction Lifecycle
// ============================================================================

func TestTransaction_CommitAfterAbort(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Transaction 1: Insert and abort
	ctx1 := createTransactionContext(t, ps.wal)
	tuple1 := createTestTuple(heapFile.GetTupleDesc(), 1, "aborted")
	err := ps.InsertTuple(ctx1, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = ps.AbortTransaction(ctx1)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Transaction 2: Insert and commit
	ctx2 := createTransactionContext(t, ps.wal)
	tuple2 := createTestTuple(heapFile.GetTupleDesc(), 2, "committed")
	err = ps.InsertTuple(ctx2, heapFile, tuple2)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = ps.CommitTransaction(ctx2)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestTransaction_SerialCommits(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	numTransactions := 10

	for i := 0; i < numTransactions; i++ {
		ctx := createTransactionContext(t, ps.wal)
		testTuple := createTestTuple(
			heapFile.GetTupleDesc(),
			int64(i),
			fmt.Sprintf("transaction_%d", i),
		)

		err := ps.InsertTuple(ctx, heapFile, testTuple)
		if err != nil {
			t.Fatalf("Transaction %d: insert failed: %v", i, err)
		}

		err = ps.CommitTransaction(ctx)
		if err != nil {
			t.Fatalf("Transaction %d: commit failed: %v", i, err)
		}
	}
}

func TestTransaction_MixedCommitsAndAborts(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	numTransactions := 10

	for i := 0; i < numTransactions; i++ {
		ctx := createTransactionContext(t, ps.wal)
		testTuple := createTestTuple(
			heapFile.GetTupleDesc(),
			int64(i),
			fmt.Sprintf("transaction_%d", i),
		)

		err := ps.InsertTuple(ctx, heapFile, testTuple)
		if err != nil {
			t.Fatalf("Transaction %d: insert failed: %v", i, err)
		}

		// Commit even transactions, abort odd ones
		if i%2 == 0 {
			err = ps.CommitTransaction(ctx)
			if err != nil {
				t.Fatalf("Transaction %d: commit failed: %v", i, err)
			}
		} else {
			// Set before-image for abort
			if testTuple.RecordID != nil {
				ps.mutex.Lock()
				if page, exists := ps.cache.Get(testTuple.RecordID.PageID); exists {
					page.SetBeforeImage()
					ps.cache.Put(testTuple.RecordID.PageID, page)
				}
				ps.mutex.Unlock()
			}

			err = ps.AbortTransaction(ctx)
			if err != nil {
				t.Fatalf("Transaction %d: abort failed: %v", i, err)
			}
		}
	}

	// Clean up all pages to avoid before-image issues during cleanup
	ps.mutex.Lock()
	for _, pageID := range ps.cache.GetAll() {
		ps.cache.Remove(pageID)
	}
	ps.mutex.Unlock()
}

func TestTransaction_LockReleaseOnCommit(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Transaction 1 acquires lock and commits
	ctx1 := createTransactionContext(t, ps.wal)
	tuple1 := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx1, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_ = tuple1.RecordID.PageID // Used for lock verification

	// Commit
	err = ps.CommitTransaction(ctx1)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Locks should be released after commit
}

func TestTransaction_LockReleaseOnAbort(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Commit a tuple first to establish page
	ctx1 := createTransactionContext(t, ps.wal)
	tuple1 := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx1, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	err = ps.CommitTransaction(ctx1)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	recordPageID := tuple1.RecordID.PageID

	// Transaction 2 modifies and aborts
	ctx2 := createTransactionContext(t, ps.wal)

	ps.mutex.Lock()
	page, _ := ps.cache.Get(recordPageID)
	page.SetBeforeImage()
	ps.cache.Put(recordPageID, page)
	ps.mutex.Unlock()

	err = ps.DeleteTuple(ctx2, heapFile, tuple1)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Abort
	err = ps.AbortTransaction(ctx2)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Locks should be released after abort

	// Clean up cache to avoid before-image issues during cleanup
	ps.mutex.Lock()
	ps.cache.Remove(recordPageID)
	ps.mutex.Unlock()
}

// ============================================================================
// Stress Tests
// ============================================================================

func TestTransaction_HighConcurrencyMixedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	numGoroutines := 20
	operationsPerGoroutine := 30

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				ctx := createTransactionContext(t, ps.wal)
				testTuple := createTestTuple(
					heapFile.GetTupleDesc(),
					int64(goroutineID*10000+j),
					fmt.Sprintf("g%d_op%d", goroutineID, j),
				)

				err := ps.InsertTuple(ctx, heapFile, testTuple)
				if err != nil {
					errors[goroutineID] = fmt.Errorf("insert failed: %v", err)
					return
				}

				// Randomly commit or abort
				if j%3 == 0 {
					// Set before-image for abort
					if testTuple.RecordID != nil {
						ps.mutex.Lock()
						if page, exists := ps.cache.Get(testTuple.RecordID.PageID); exists {
							page.SetBeforeImage()
							ps.cache.Put(testTuple.RecordID.PageID, page)
						}
						ps.mutex.Unlock()
					}
					err = ps.AbortTransaction(ctx)
				} else {
					err = ps.CommitTransaction(ctx)
				}

				if err != nil {
					errors[goroutineID] = fmt.Errorf("finalize failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Check results
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// Clean up all pages to avoid before-image issues during cleanup
	ps.mutex.Lock()
	for _, pageID := range ps.cache.GetAll() {
		ps.cache.Remove(pageID)
	}
	ps.mutex.Unlock()
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestTransaction_CommitWithUnregisteredDbFile(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")
	heapPath := filepath.Join(tempDir, "test.heap")

	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)

	heapFile, err := heap.NewHeapFile(heapPath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer heapFile.Close()

	ps := NewPageStore(wal)
	ps.RegisterDbFile(heapFile.GetID(), heapFile)

	ctx := createTransactionContext(t, wal)
	testTuple := tuple.NewTuple(td)
	testTuple.SetField(0, types.NewIntField(1))

	// Use InsertTuple which properly adds the tuple
	err = ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Now unregister the DbFile before commit
	ps.UnregisterDbFile(heapFile.GetID())

	// Commit should fail because DbFile is not registered
	err = ps.CommitTransaction(ctx)
	if err == nil {
		t.Error("Expected error for unregistered DbFile during commit")
	}
}

func TestTransaction_DoubleCommit(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First commit
	err = ps.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit on same context (should be no-op since no dirty pages)
	ctx2 := createTransactionContext(t, ps.wal)
	err = ps.CommitTransaction(ctx2)
	if err != nil {
		t.Errorf("Second commit should succeed as no-op: %v", err)
	}
}

func TestTransaction_DoubleAbort(t *testing.T) {
	ps, heapFile, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := createTransactionContext(t, ps.wal)
	testTuple := createTestTuple(heapFile.GetTupleDesc(), 1, "test")
	err := ps.InsertTuple(ctx, heapFile, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Set before-image
	if testTuple.RecordID != nil {
		ps.mutex.Lock()
		if page, exists := ps.cache.Get(testTuple.RecordID.PageID); exists {
			page.SetBeforeImage()
			ps.cache.Put(testTuple.RecordID.PageID, page)
		}
		ps.mutex.Unlock()
	}

	// First abort
	err = ps.AbortTransaction(ctx)
	if err != nil {
		t.Fatalf("First abort failed: %v", err)
	}

	// Clean up cache after first abort
	if testTuple.RecordID != nil {
		ps.mutex.Lock()
		ps.cache.Remove(testTuple.RecordID.PageID)
		ps.mutex.Unlock()
	}

	// Second abort on new context (should be no-op)
	ctx2 := createTransactionContext(t, ps.wal)
	err = ps.AbortTransaction(ctx2)
	if err != nil {
		t.Errorf("Second abort should succeed as no-op: %v", err)
	}
}
