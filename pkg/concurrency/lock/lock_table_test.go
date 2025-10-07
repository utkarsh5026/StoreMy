package lock

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"testing"
)

func TestNewLockTable(t *testing.T) {
	lt := NewLockTable()

	if lt == nil {
		t.Fatal("NewLockTable() returned nil")
	}

	if lt.pageLocks == nil {
		t.Error("pageLocks map not initialized")
	}

	if lt.transactionLocks == nil {
		t.Error("transactionLocks map not initialized")
	}
}

func TestAddLock(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add shared lock
	lt.AddLock(tid, pid, SharedLock)

	// Verify page locks
	locks := lt.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock, got %d", len(locks))
	}
	if locks[0].TID != tid {
		t.Error("Lock has wrong transaction ID")
	}
	if locks[0].LockType != SharedLock {
		t.Error("Lock has wrong type")
	}

	// Verify transaction locks
	txLocks := lt.GetTransactionLockTable()[tid]
	if txLocks == nil {
		t.Fatal("Transaction locks not initialized")
	}
	if txLocks[pid] != SharedLock {
		t.Error("Transaction lock type mismatch")
	}
}

func TestAddMultipleLocks(t *testing.T) {
	lt := NewLockTable()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add two shared locks on same page
	lt.AddLock(tid1, pid, SharedLock)
	lt.AddLock(tid2, pid, SharedLock)

	// Verify page has both locks
	locks := lt.GetPageLocks(pid)
	if len(locks) != 2 {
		t.Fatalf("Expected 2 locks, got %d", len(locks))
	}

	// Verify both transactions have entries
	txTable := lt.GetTransactionLockTable()
	if txTable[tid1][pid] != SharedLock {
		t.Error("Transaction 1 lock type mismatch")
	}
	if txTable[tid2][pid] != SharedLock {
		t.Error("Transaction 2 lock type mismatch")
	}
}

func TestHasSufficientLock(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// No lock exists
	if lt.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Should not have sufficient lock when no lock exists")
	}

	// Add shared lock
	lt.AddLock(tid, pid, SharedLock)

	// Shared lock sufficient for shared request
	if !lt.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Shared lock should be sufficient for shared request")
	}

	// Shared lock not sufficient for exclusive request
	if lt.HasSufficientLock(tid, pid, ExclusiveLock) {
		t.Error("Shared lock should not be sufficient for exclusive request")
	}

	// Upgrade to exclusive lock
	lt.UpgradeLock(tid, pid)

	// Exclusive lock sufficient for both
	if !lt.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Exclusive lock should be sufficient for shared request")
	}
	if !lt.HasSufficientLock(tid, pid, ExclusiveLock) {
		t.Error("Exclusive lock should be sufficient for exclusive request")
	}
}

func TestHasLockType(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// No lock exists
	if lt.HasLockType(tid, pid, SharedLock) {
		t.Error("Should not have lock type when no lock exists")
	}

	// Add shared lock
	lt.AddLock(tid, pid, SharedLock)

	// Check correct type
	if !lt.HasLockType(tid, pid, SharedLock) {
		t.Error("Should have shared lock type")
	}

	// Check incorrect type
	if lt.HasLockType(tid, pid, ExclusiveLock) {
		t.Error("Should not have exclusive lock type")
	}

	// Upgrade to exclusive
	lt.UpgradeLock(tid, pid)

	// Check types after upgrade
	if lt.HasLockType(tid, pid, SharedLock) {
		t.Error("Should not have shared lock type after upgrade")
	}
	if !lt.HasLockType(tid, pid, ExclusiveLock) {
		t.Error("Should have exclusive lock type after upgrade")
	}
}

func TestIsPageLocked(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Page not locked initially
	if lt.IsPageLocked(pid) {
		t.Error("Page should not be locked initially")
	}

	// Add lock
	lt.AddLock(tid, pid, SharedLock)

	// Page should be locked now
	if !lt.IsPageLocked(pid) {
		t.Error("Page should be locked after adding lock")
	}

	// Release lock
	lt.ReleaseLock(tid, pid)

	// Page should not be locked after release
	if lt.IsPageLocked(pid) {
		t.Error("Page should not be locked after releasing lock")
	}
}

func TestUpgradeLock(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add shared lock
	lt.AddLock(tid, pid, SharedLock)

	// Verify initial state
	locks := lt.GetPageLocks(pid)
	if locks[0].LockType != SharedLock {
		t.Error("Initial lock should be shared")
	}

	// Upgrade lock
	lt.UpgradeLock(tid, pid)

	// Verify upgrade
	locks = lt.GetPageLocks(pid)
	if locks[0].LockType != ExclusiveLock {
		t.Error("Lock should be upgraded to exclusive")
	}
	if !lt.HasLockType(tid, pid, ExclusiveLock) {
		t.Error("Transaction should have exclusive lock after upgrade")
	}
}

func TestReleaseLock(t *testing.T) {
	lt := NewLockTable()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add two locks
	lt.AddLock(tid1, pid, SharedLock)
	lt.AddLock(tid2, pid, SharedLock)

	// Verify both locks exist
	locks := lt.GetPageLocks(pid)
	if len(locks) != 2 {
		t.Fatalf("Expected 2 locks, got %d", len(locks))
	}

	// Release first transaction's lock
	lt.ReleaseLock(tid1, pid)

	// Verify only one lock remains
	locks = lt.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock after release, got %d", len(locks))
	}
	if locks[0].TID != tid2 {
		t.Error("Wrong lock remained after release")
	}

	// Verify transaction lock table updated
	txTable := lt.GetTransactionLockTable()
	if _, exists := txTable[tid1]; exists {
		t.Error("Transaction 1 should be removed from lock table")
	}
	if txTable[tid2][pid] != SharedLock {
		t.Error("Transaction 2 lock should still exist")
	}

	// Release second transaction's lock
	lt.ReleaseLock(tid2, pid)

	// Verify page has no locks
	if lt.IsPageLocked(pid) {
		t.Error("Page should not be locked after releasing all locks")
	}

	// Verify transaction removed from table
	if _, exists := txTable[tid2]; exists {
		t.Error("Transaction 2 should be removed from lock table")
	}
}

func TestReleaseAllLocks(t *testing.T) {
	lt := NewLockTable()
	tid := primitives.NewTransactionID()
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)
	pid3 := heap.NewHeapPageID(2, 1)

	// Add locks on multiple pages
	lt.AddLock(tid, pid1, SharedLock)
	lt.AddLock(tid, pid2, ExclusiveLock)
	lt.AddLock(tid, pid3, SharedLock)

	// Verify locks exist
	if !lt.IsPageLocked(pid1) || !lt.IsPageLocked(pid2) || !lt.IsPageLocked(pid3) {
		t.Error("All pages should be locked")
	}

	// Release all locks
	affectedPages := lt.ReleaseAllLocks(tid)

	// Verify affected pages returned
	if len(affectedPages) != 3 {
		t.Fatalf("Expected 3 affected pages, got %d", len(affectedPages))
	}

	// Verify all pages unlocked
	if lt.IsPageLocked(pid1) || lt.IsPageLocked(pid2) || lt.IsPageLocked(pid3) {
		t.Error("No pages should be locked after releasing all")
	}

	// Verify transaction removed from table
	txTable := lt.GetTransactionLockTable()
	if _, exists := txTable[tid]; exists {
		t.Error("Transaction should be removed from lock table")
	}

	// Test releasing locks for non-existent transaction
	nonExistentTid := primitives.NewTransactionID()
	affectedPages = lt.ReleaseAllLocks(nonExistentTid)
	if affectedPages != nil {
		t.Error("Should return nil for non-existent transaction")
	}
}

func TestReleaseAllLocksWithMultipleTransactions(t *testing.T) {
	lt := NewLockTable()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add locks from both transactions
	lt.AddLock(tid1, pid, SharedLock)
	lt.AddLock(tid2, pid, SharedLock)

	// Verify both locks exist
	locks := lt.GetPageLocks(pid)
	if len(locks) != 2 {
		t.Fatalf("Expected 2 locks, got %d", len(locks))
	}

	// Release all locks for first transaction
	lt.ReleaseAllLocks(tid1)

	// Verify second transaction's lock still exists
	locks = lt.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock after release, got %d", len(locks))
	}
	if locks[0].TID != tid2 {
		t.Error("Wrong lock remained after release")
	}

	// Verify page still locked
	if !lt.IsPageLocked(pid) {
		t.Error("Page should still be locked")
	}

	// Verify first transaction removed, second still exists
	txTable := lt.GetTransactionLockTable()
	if _, exists := txTable[tid1]; exists {
		t.Error("Transaction 1 should be removed")
	}
	if txTable[tid2][pid] != SharedLock {
		t.Error("Transaction 2 lock should still exist")
	}
}

func TestGetPageLocks(t *testing.T) {
	lt := NewLockTable()
	pid := heap.NewHeapPageID(1, 1)

	// Get locks for non-existent page
	locks := lt.GetPageLocks(pid)
	if locks != nil {
		t.Error("Should return nil for non-existent page")
	}

	// Add lock and verify
	tid := primitives.NewTransactionID()
	lt.AddLock(tid, pid, SharedLock)

	locks = lt.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock, got %d", len(locks))
	}
	if locks[0].TID != tid {
		t.Error("Lock has wrong transaction ID")
	}
}

func TestGetters(t *testing.T) {
	lt := NewLockTable()

	// Test empty tables
	pageTable := lt.GetPageLockTable()
	txTable := lt.GetTransactionLockTable()

	if pageTable == nil {
		t.Error("Page lock table should not be nil")
	}
	if txTable == nil {
		t.Error("Transaction lock table should not be nil")
	}
	if len(pageTable) != 0 {
		t.Error("Page lock table should be empty initially")
	}
	if len(txTable) != 0 {
		t.Error("Transaction lock table should be empty initially")
	}

	// Add some locks and verify tables are populated
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)
	lt.AddLock(tid, pid, SharedLock)

	pageTable = lt.GetPageLockTable()
	txTable = lt.GetTransactionLockTable()

	if len(pageTable) != 1 {
		t.Error("Page lock table should have 1 entry")
	}
	if len(txTable) != 1 {
		t.Error("Transaction lock table should have 1 entry")
	}
}
