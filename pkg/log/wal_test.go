package log

import (
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"testing"
)

// mockPageID is a simple implementation of PageID for testing
type mockPageID struct {
	tableID int
	pageNo  int
}

func (m *mockPageID) GetTableID() int {
	return m.tableID
}

func (m *mockPageID) PageNo() int {
	return m.pageNo
}

func (m *mockPageID) Serialize() []int {
	return []int{m.tableID, m.pageNo}
}

func (m *mockPageID) Equals(other tuple.PageID) bool {
	if otherMock, ok := other.(*mockPageID); ok {
		return m.tableID == otherMock.tableID && m.pageNo == otherMock.pageNo
	}
	return false
}

func (m *mockPageID) String() string {
	return ""
}

func (m *mockPageID) HashCode() int {
	return m.tableID*1000 + m.pageNo
}

// Helper function to create a temporary WAL for testing
func createTestWAL(t *testing.T) (*WAL, string, func()) {
	t.Helper()

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tmpDir, "test.wal")
	wal, err := NewWAL(logPath, 4096)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	cleanup := func() {
		wal.file.Close()
		os.RemoveAll(tmpDir)
	}

	return wal, logPath, cleanup
}

func TestNewWAL(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	if wal == nil {
		t.Fatal("expected non-nil WAL")
	}

	if wal.currentLSN != 0 {
		t.Errorf("expected currentLSN to be 0, got %d", wal.currentLSN)
	}

	if wal.flushedLSN != 0 {
		t.Errorf("expected flushedLSN to be 0, got %d", wal.flushedLSN)
	}

	if wal.bufferSize != 4096 {
		t.Errorf("expected bufferSize to be 4096, got %d", wal.bufferSize)
	}

	if len(wal.activeTxns) != 0 {
		t.Errorf("expected empty activeTxns, got %d", len(wal.activeTxns))
	}

	if len(wal.dirtyPages) != 0 {
		t.Errorf("expected empty dirtyPages, got %d", len(wal.dirtyPages))
	}
}

func TestLogBegin(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	lsn, err := wal.LogBegin(tid)

	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	if lsn != FirstLSN {
		t.Errorf("expected first LSN to be %d, got %d", FirstLSN, lsn)
	}

	// Check that transaction is tracked
	txnInfo, exists := wal.activeTxns[tid]
	if !exists {
		t.Fatal("transaction not found in activeTxns")
	}

	if txnInfo.FirstLSN != lsn {
		t.Errorf("expected FirstLSN to be %d, got %d", lsn, txnInfo.FirstLSN)
	}

	if txnInfo.LastLSN != lsn {
		t.Errorf("expected LastLSN to be %d, got %d", lsn, txnInfo.LastLSN)
	}
}

func TestLogUpdate(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	_, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	pageID := &mockPageID{tableID: 1, pageNo: 100}
	beforeImage := []byte("before data")
	afterImage := []byte("after data")

	lsn, err := wal.LogUpdate(tid, pageID, beforeImage, afterImage)
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	if lsn == FirstLSN {
		t.Error("expected LSN to be different from FirstLSN")
	}

	// Check transaction info was updated
	txnInfo := wal.activeTxns[tid]
	if txnInfo.LastLSN != lsn {
		t.Errorf("expected LastLSN to be %d, got %d", lsn, txnInfo.LastLSN)
	}

	// Check dirty page tracking
	recLSN, exists := wal.dirtyPages[pageID]
	if !exists {
		t.Fatal("page not found in dirtyPages")
	}

	if recLSN != lsn {
		t.Errorf("expected recLSN to be %d, got %d", lsn, recLSN)
	}
}

func TestLogUpdateWithoutBegin(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	pageID := &mockPageID{tableID: 1, pageNo: 100}

	_, err := wal.LogUpdate(tid, pageID, []byte("before"), []byte("after"))
	if err == nil {
		t.Fatal("expected error when logging update without begin")
	}
}

func TestMultipleUpdates(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	firstLSN, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Log multiple updates
	lsns := make([]LSN, 3)
	for i := 0; i < 3; i++ {
		pageID := &mockPageID{tableID: 1, pageNo: 100 + i}
		lsn, err := wal.LogUpdate(tid, pageID, []byte("before"), []byte("after"))
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}
		lsns[i] = lsn
	}

	// Verify LSNs are increasing
	if lsns[0] <= firstLSN {
		t.Error("expected LSN to increase")
	}

	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("expected LSN[%d] (%d) > LSN[%d] (%d)", i, lsns[i], i-1, lsns[i-1])
		}
	}

	// Verify last LSN in transaction info
	txnInfo := wal.activeTxns[tid]
	if txnInfo.LastLSN != lsns[len(lsns)-1] {
		t.Errorf("expected LastLSN to be %d, got %d", lsns[len(lsns)-1], txnInfo.LastLSN)
	}
}

func TestForce(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	lsn, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Force flush
	err = wal.Force(lsn)
	if err != nil {
		t.Fatalf("Force failed: %v", err)
	}

	// Check flushed LSN was updated
	if wal.flushedLSN != wal.currentLSN {
		t.Errorf("expected flushedLSN to be %d, got %d", wal.currentLSN, wal.flushedLSN)
	}

	// After flushing, flushed LSN should be at least the requested LSN
	if wal.flushedLSN < lsn {
		t.Errorf("expected flushedLSN to be at least %d, got %d", lsn, wal.flushedLSN)
	}
}

func TestForceIdempotent(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	lsn, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Force flush
	err = wal.Force(lsn)
	if err != nil {
		t.Fatalf("Force failed: %v", err)
	}

	flushedLSN := wal.flushedLSN

	// Force again - should be idempotent
	err = wal.Force(lsn)
	if err != nil {
		t.Fatalf("Force failed: %v", err)
	}

	if wal.flushedLSN != flushedLSN {
		t.Error("Force should be idempotent")
	}
}

func TestBufferFlushing(t *testing.T) {
	// Create WAL with small buffer to force flushing
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "test.wal")
	wal, err := NewWAL(logPath, 256) // Very small buffer
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.file.Close()

	tid := transaction.NewTransactionID()
	_, err = wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	// Log many updates to force buffer flush
	for i := 0; i < 10; i++ {
		pageID := &mockPageID{tableID: 1, pageNo: i}
		beforeImage := make([]byte, 10)
		afterImage := make([]byte, 10)

		_, err := wal.LogUpdate(tid, pageID, beforeImage, afterImage)
		if err != nil {
			t.Fatalf("LogUpdate %d failed: %v", i, err)
		}
	}

	// Verify buffer was flushed (flushedLSN should have advanced)
	// With many records, the buffer should have been flushed at least once
	if wal.flushedLSN == 0 {
		t.Errorf("expected buffer to be flushed due to size, currentLSN=%d, bufferOffset=%d",
			wal.currentLSN, wal.bufferOffset)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()

	lsn1, err := wal.LogBegin(tid1)
	if err != nil {
		t.Fatalf("LogBegin for tid1 failed: %v", err)
	}

	lsn2, err := wal.LogBegin(tid2)
	if err != nil {
		t.Fatalf("LogBegin for tid2 failed: %v", err)
	}

	// Verify both transactions are tracked
	if len(wal.activeTxns) != 2 {
		t.Errorf("expected 2 active transactions, got %d", len(wal.activeTxns))
	}

	// Log updates for both transactions
	pageID1 := &mockPageID{tableID: 1, pageNo: 1}
	_, err = wal.LogUpdate(tid1, pageID1, []byte("before1"), []byte("after1"))
	if err != nil {
		t.Fatalf("LogUpdate for tid1 failed: %v", err)
	}

	pageID2 := &mockPageID{tableID: 2, pageNo: 2}
	_, err = wal.LogUpdate(tid2, pageID2, []byte("before2"), []byte("after2"))
	if err != nil {
		t.Fatalf("LogUpdate for tid2 failed: %v", err)
	}

	// Verify transaction info is independent
	txnInfo1 := wal.activeTxns[tid1]
	txnInfo2 := wal.activeTxns[tid2]

	if txnInfo1.FirstLSN != lsn1 {
		t.Error("tid1 FirstLSN mismatch")
	}

	if txnInfo2.FirstLSN != lsn2 {
		t.Error("tid2 FirstLSN mismatch")
	}
}

func TestDirtyPageTracking(t *testing.T) {
	wal, _, cleanup := createTestWAL(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	_, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	pageID := &mockPageID{tableID: 1, pageNo: 100}

	// First update - should add to dirty pages
	lsn1, err := wal.LogUpdate(tid, pageID, []byte("v1"), []byte("v2"))
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	recLSN := wal.dirtyPages[pageID]
	if recLSN != lsn1 {
		t.Errorf("expected recLSN to be %d, got %d", lsn1, recLSN)
	}

	// Second update to same page - recLSN should not change
	_, err = wal.LogUpdate(tid, pageID, []byte("v2"), []byte("v3"))
	if err != nil {
		t.Fatalf("second LogUpdate failed: %v", err)
	}

	recLSN2 := wal.dirtyPages[pageID]
	if recLSN2 != lsn1 {
		t.Errorf("expected recLSN to remain %d, got %d", lsn1, recLSN2)
	}
}

func TestWALPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL and write some records
	wal, err := NewWAL(logPath, 4096)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	tid := transaction.NewTransactionID()
	lsn, err := wal.LogBegin(tid)
	if err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}

	err = wal.Force(lsn)
	if err != nil {
		t.Fatalf("Force failed: %v", err)
	}

	expectedLSN := wal.currentLSN
	wal.file.Close()

	// Reopen WAL
	wal2, err := NewWAL(logPath, 4096)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.file.Close()

	// Check that LSN was restored
	if wal2.currentLSN != expectedLSN {
		t.Errorf("expected currentLSN to be %d after reopen, got %d", expectedLSN, wal2.currentLSN)
	}
}
