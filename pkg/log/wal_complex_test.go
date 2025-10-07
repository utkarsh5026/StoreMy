package log

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"sync"
	"testing"
	"time"
)

// TestCompleteTransactionLifecycle tests the full lifecycle of a transaction
func TestCompleteTransactionLifecycle(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()

	// Begin
	beginLSN, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Insert
	pageID1 := &mockPageID{tableID: 1, pageNo: 1}
	insertLSN, err := wal.LogInsert(tid, pageID1, []byte("new tuple"))
	if err != nil {
		t.Fatalf("LogInsert failed: %v", err)
	}

	// Update
	pageID2 := &mockPageID{tableID: 1, pageNo: 2}
	updateLSN, err := wal.LogUpdate(tid, pageID2, []byte("old"), []byte("new"))
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	// Delete
	pageID3 := &mockPageID{tableID: 1, pageNo: 3}
	deleteLSN, err := wal.LogDelete(tid, pageID3, []byte("deleted"))
	if err != nil {
		t.Fatalf("LogDelete failed: %v", err)
	}

	// Commit
	commitLSN, err := wal.LogCommit(tid)
	if err != nil {
		t.Fatalf("LogCommit failed: %v", err)
	}

	// Verify LSN ordering
	if !(beginLSN < insertLSN && insertLSN < updateLSN && updateLSN < deleteLSN && deleteLSN < commitLSN) {
		t.Error("LSNs not in expected order")
	}

	// Verify transaction is removed
	if _, exists := wal.activeTxns[tid]; exists {
		t.Error("transaction should be removed after commit")
	}

	// Verify all pages tracked
	if len(wal.dirtyPages) != 3 {
		t.Errorf("expected 3 dirty pages, got %d", len(wal.dirtyPages))
	}
}

// TestInterleavedTransactions tests multiple transactions executing concurrently
func TestInterleavedTransactions(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	// Start 3 transactions
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	tid3 := transaction.NewTransactionID()

	_, err := wal.LogBegin(tid1)
	if err != nil {
		t.Fatalf("LogBegin tid1 failed: %v", err)
	}

	_, err = wal.LogBegin(tid2)
	if err != nil {
		t.Fatalf("LogBegin tid2 failed: %v", err)
	}

	_, err = wal.LogBegin(tid3)
	if err != nil {
		t.Fatalf("LogBegin tid3 failed: %v", err)
	}

	// Interleaved operations
	pageID1 := &mockPageID{tableID: 1, pageNo: 1}
	_, err = wal.LogUpdate(tid1, pageID1, []byte("t1_before"), []byte("t1_after"))
	if err != nil {
		t.Fatalf("LogUpdate tid1 failed: %v", err)
	}

	pageID2 := &mockPageID{tableID: 1, pageNo: 2}
	_, err = wal.LogInsert(tid2, pageID2, []byte("t2_insert"))
	if err != nil {
		t.Fatalf("LogInsert tid2 failed: %v", err)
	}

	pageID3 := &mockPageID{tableID: 1, pageNo: 3}
	_, err = wal.LogDelete(tid3, pageID3, []byte("t3_delete"))
	if err != nil {
		t.Fatalf("LogDelete tid3 failed: %v", err)
	}

	pageID4 := &mockPageID{tableID: 1, pageNo: 4}
	_, err = wal.LogUpdate(tid2, pageID4, []byte("t2_before"), []byte("t2_after"))
	if err != nil {
		t.Fatalf("LogUpdate tid2 failed: %v", err)
	}

	// Commit tid1
	_, err = wal.LogCommit(tid1)
	if err != nil {
		t.Fatalf("LogCommit tid1 failed: %v", err)
	}

	// Abort tid3
	_, err = wal.LogAbort(tid3)
	if err != nil {
		t.Fatalf("LogAbort tid3 failed: %v", err)
	}

	// Commit tid2
	_, err = wal.LogCommit(tid2)
	if err != nil {
		t.Fatalf("LogCommit tid2 failed: %v", err)
	}

	// Verify tid1 and tid2 are removed
	if _, exists := wal.activeTxns[tid1]; exists {
		t.Error("tid1 should be removed after commit")
	}
	if _, exists := wal.activeTxns[tid2]; exists {
		t.Error("tid2 should be removed after commit")
	}

	// Verify tid3 still exists (pending undo)
	if _, exists := wal.activeTxns[tid3]; !exists {
		t.Error("tid3 should still exist after abort")
	}
}

// TestConcurrentWrites tests concurrent writes to WAL
func TestConcurrentWrites(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	numGoroutines := 10
	opsPerGoroutine := 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			tid := transaction.NewTransactionID()
			_, err := wal.LogBegin(tid)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: LogBegin failed: %v", routineID, err)
				return
			}

			for j := 0; j < opsPerGoroutine; j++ {
				pageID := &mockPageID{tableID: routineID, pageNo: j}
				_, err := wal.LogUpdate(tid, pageID, []byte(fmt.Sprintf("before_%d_%d", routineID, j)), []byte(fmt.Sprintf("after_%d_%d", routineID, j)))
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: LogUpdate failed: %v", routineID, err)
					return
				}
			}

			_, err = wal.LogCommit(tid)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: LogCommit failed: %v", routineID, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all transactions are committed
	if len(wal.activeTxns) != 0 {
		t.Errorf("expected no active transactions, got %d", len(wal.activeTxns))
	}

	// Verify dirty pages tracked
	expectedPages := numGoroutines * opsPerGoroutine
	if len(wal.dirtyPages) != expectedPages {
		t.Errorf("expected %d dirty pages, got %d", expectedPages, len(wal.dirtyPages))
	}
}

// TestLargeTransaction tests a transaction with many operations
func TestLargeTransaction(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	_, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	numOps := 1000
	for i := 0; i < numOps; i++ {
		pageID := &mockPageID{tableID: 1, pageNo: i}

		switch i % 3 {
		case 0:
			_, err = wal.LogInsert(tid, pageID, []byte(fmt.Sprintf("insert_%d", i)))
		case 1:
			_, err = wal.LogUpdate(tid, pageID, []byte(fmt.Sprintf("before_%d", i)), []byte(fmt.Sprintf("after_%d", i)))
		case 2:
			_, err = wal.LogDelete(tid, pageID, []byte(fmt.Sprintf("delete_%d", i)))
		}

		if err != nil {
			t.Fatalf("operation %d failed: %v", i, err)
		}
	}

	_, err = wal.LogCommit(tid)
	if err != nil {
		t.Fatalf("LogCommit failed: %v", err)
	}

	// Verify transaction is removed
	if _, exists := wal.activeTxns[tid]; exists {
		t.Error("transaction should be removed after commit")
	}

	// Verify all pages tracked
	if len(wal.dirtyPages) != numOps {
		t.Errorf("expected %d dirty pages, got %d", numOps, len(wal.dirtyPages))
	}
}

// TestMultiplePagesPerTransaction tests updating the same page multiple times
func TestMultiplePagesPerTransaction(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	_, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	pageID := &mockPageID{tableID: 1, pageNo: 1}

	// Update same page multiple times
	lsns := make([]primitives.LSN, 5)
	for i := 0; i < 5; i++ {
		lsn, err := wal.LogUpdate(tid, pageID, []byte(fmt.Sprintf("v%d", i)), []byte(fmt.Sprintf("v%d", i+1)))
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}
		lsns[i] = lsn
	}

	// Verify LSNs are increasing
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSN[%d] (%d) should be > LSN[%d] (%d)", i, lsns[i], i-1, lsns[i-1])
		}
	}

	// Verify only one entry in dirty pages (recLSN should be first update)
	recLSN, exists := wal.dirtyPages[pageID]
	if !exists {
		t.Fatal("page should be in dirty pages")
	}

	if recLSN != lsns[0] {
		t.Errorf("expected recLSN to be first update LSN %d, got %d", lsns[0], recLSN)
	}

	_, err = wal.LogCommit(tid)
	if err != nil {
		t.Fatalf("LogCommit failed: %v", err)
	}
}

// TestBufferOverflow tests that buffer overflow triggers flush
func TestBufferOverflow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "test.wal")
	// Create WAL with small buffer (but large enough for individual records)
	wal, err := NewWAL(logPath, 512)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.file.Close()

	tid := transaction.NewTransactionID()
	_, err = wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	initialLSN := wal.writer.CurrentLSN()

	// Write many small records to eventually overflow buffer
	smallData := make([]byte, 20)
	for i := 0; i < 20; i++ {
		pageID := &mockPageID{tableID: 1, pageNo: i}
		_, err := wal.LogUpdate(tid, pageID, smallData, smallData)
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}
	}

	// Verify buffer was used (currentLSN advanced beyond initial)
	if wal.writer.CurrentLSN() == initialLSN {
		t.Error("expected currentLSN to advance after multiple writes")
	}
}

// TestGetDirtyPages tests the GetDirtyPages method
func TestGetDirtyPages(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	_, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Create some dirty pages and track them
	numPages := 5
	pageIDs := make([]*mockPageID, numPages)
	for i := 0; i < numPages; i++ {
		pageIDs[i] = &mockPageID{tableID: 1, pageNo: i}
		_, err := wal.LogUpdate(tid, pageIDs[i], []byte("before"), []byte("after"))
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}
	}

	// Get dirty pages
	dirtyPages := wal.GetDirtyPages()

	// Verify count
	if len(dirtyPages) != numPages {
		t.Errorf("expected %d dirty pages, got %d", numPages, len(dirtyPages))
	}

	// Verify all pages are present (using same pageID references)
	for i, pageID := range pageIDs {
		if _, exists := dirtyPages[pageID]; !exists {
			t.Errorf("page %d not found in dirty pages", i)
		}
	}

	// Verify it's a copy (modifying returned map shouldn't affect internal state)
	for pageID := range dirtyPages {
		delete(dirtyPages, pageID)
	}

	if len(wal.dirtyPages) != numPages {
		t.Error("GetDirtyPages should return a copy, not the original map")
	}
}

// TestGetActiveTransactions tests the GetActiveTransactions method
func TestGetActiveTransactions(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	// Start multiple transactions
	numTxns := 5
	tids := make([]*transaction.TransactionID, numTxns)

	for i := 0; i < numTxns; i++ {
		tid := transaction.NewTransactionID()
		_, err := wal.LogBegin(tid)
		if err != nil {
			t.Fatalf("LogBegin %d failed: %v", i, err)
		}
		tids[i] = tid
	}

	// Get active transactions
	activeTxns := wal.GetActiveTransactions()

	// Verify count
	if len(activeTxns) != numTxns {
		t.Errorf("expected %d active transactions, got %d", numTxns, len(activeTxns))
	}

	// Verify all transactions are present
	txnMap := make(map[*transaction.TransactionID]bool)
	for _, tid := range activeTxns {
		txnMap[tid] = true
	}

	for _, tid := range tids {
		if !txnMap[tid] {
			t.Errorf("transaction %v not found in active transactions", tid)
		}
	}

	// Commit one transaction and verify it's removed
	_, err := wal.LogCommit(tids[0])
	if err != nil {
		t.Fatalf("LogCommit failed: %v", err)
	}

	activeTxns = wal.GetActiveTransactions()
	if len(activeTxns) != numTxns-1 {
		t.Errorf("expected %d active transactions after commit, got %d", numTxns-1, len(activeTxns))
	}
}

// TestTransactionChaining tests PrevLSN links
func TestTransactionChaining(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	beginLSN, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	txnInfo := wal.activeTxns[tid]

	// Verify begin record
	if txnInfo.FirstLSN != beginLSN {
		t.Errorf("expected FirstLSN to be %d, got %d", beginLSN, txnInfo.FirstLSN)
	}
	if txnInfo.LastLSN != beginLSN {
		t.Errorf("expected LastLSN to be %d, got %d", beginLSN, txnInfo.LastLSN)
	}

	// Perform operations and verify LastLSN is updated
	pageID := &mockPageID{tableID: 1, pageNo: 1}
	updateLSN, err := wal.LogUpdate(tid, pageID, []byte("before"), []byte("after"))
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	if txnInfo.LastLSN != updateLSN {
		t.Errorf("expected LastLSN to be updated to %d, got %d", updateLSN, txnInfo.LastLSN)
	}

	// FirstLSN should remain unchanged
	if txnInfo.FirstLSN != beginLSN {
		t.Errorf("FirstLSN should remain %d, got %d", beginLSN, txnInfo.FirstLSN)
	}
}

// TestForceIdempotency tests that multiple Force calls are safe
func TestForceIdempotency(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	lsn, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Force multiple times
	for i := 0; i < 5; i++ {
		err := wal.Force(lsn)
		if err != nil {
			t.Fatalf("Force %d failed: %v", i, err)
		}
	}

	// Force is idempotent - multiple calls should not cause errors
}

// TestAbortPreservesTransactionInfo tests that abort doesn't remove transaction
func TestAbortPreservesTransactionInfo(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	beginLSN, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	pageID := &mockPageID{tableID: 1, pageNo: 1}
	_, err = wal.LogUpdate(tid, pageID, []byte("before"), []byte("after"))
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	abortLSN, err := wal.LogAbort(tid)
	if err != nil {
		t.Fatalf("LogAbort failed: %v", err)
	}

	// Verify transaction still exists
	txnInfo, exists := wal.activeTxns[tid]
	if !exists {
		t.Fatal("transaction should still exist after abort")
	}

	// Verify UndoNextLSN is set
	if txnInfo.UndoNextLSN != abortLSN {
		t.Errorf("expected UndoNextLSN to be %d, got %d", abortLSN, txnInfo.UndoNextLSN)
	}

	// Verify FirstLSN unchanged
	if txnInfo.FirstLSN != beginLSN {
		t.Errorf("FirstLSN should remain %d, got %d", beginLSN, txnInfo.FirstLSN)
	}

	// Verify LastLSN updated
	if txnInfo.LastLSN != abortLSN {
		t.Errorf("expected LastLSN to be %d, got %d", abortLSN, txnInfo.LastLSN)
	}
}

// TestStressTest runs a stress test with many concurrent transactions
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	numGoroutines := 50
	opsPerTxn := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*opsPerTxn)

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				tid := transaction.NewTransactionID()
				_, err := wal.LogBegin(tid)
				if err != nil {
					errors <- err
					return
				}

				for k := 0; k < opsPerTxn; k++ {
					pageID := &mockPageID{tableID: routineID, pageNo: k}

					switch k % 3 {
					case 0:
						_, err = wal.LogInsert(tid, pageID, []byte(fmt.Sprintf("insert_%d_%d_%d", routineID, j, k)))
					case 1:
						_, err = wal.LogUpdate(tid, pageID, []byte("before"), []byte("after"))
					case 2:
						_, err = wal.LogDelete(tid, pageID, []byte("deleted"))
					}

					if err != nil {
						errors <- err
						return
					}
				}

				// Randomly commit or abort
				if j%2 == 0 {
					_, err = wal.LogCommit(tid)
				} else {
					_, err = wal.LogAbort(tid)
				}

				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	duration := time.Since(start)
	t.Logf("Stress test completed in %v", duration)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
		if errorCount >= 10 {
			t.Log("Too many errors, stopping error output")
			break
		}
	}
}

// TestPersistenceAfterCommit tests that committed data persists after WAL reopen
func TestPersistenceAfterCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL and write some transactions
	wal, err := NewWAL(logPath, 4096)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	numTxns := 10
	for i := 0; i < numTxns; i++ {
		tid := transaction.NewTransactionID()
		_, err := wal.LogBegin(tid)
		if err != nil {
			t.Fatalf("LogBegin %d failed: %v", i, err)
		}

		pageID := &mockPageID{tableID: 1, pageNo: i}
		_, err = wal.LogUpdate(tid, pageID, []byte(fmt.Sprintf("before_%d", i)), []byte(fmt.Sprintf("after_%d", i)))
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}

		_, err = wal.LogCommit(tid)
		if err != nil {
			t.Fatalf("LogCommit %d failed: %v", i, err)
		}
	}

	expectedLSN := wal.writer.CurrentLSN()
	wal.Close()

	// Reopen WAL
	wal2, err := NewWAL(logPath, 4096)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.file.Close()

	// Verify LSN was preserved
	if wal2.writer.CurrentLSN() != expectedLSN {
		t.Errorf("expected currentLSN to be %d after reopen, got %d", expectedLSN, wal2.writer.CurrentLSN())
	}

	// Verify no active transactions (all were committed)
	activeTxns := wal2.GetActiveTransactions()
	if len(activeTxns) != 0 {
		t.Errorf("expected no active transactions after reopen, got %d", len(activeTxns))
	}
}
