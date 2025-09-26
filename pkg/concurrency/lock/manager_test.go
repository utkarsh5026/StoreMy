package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"sync"
	"testing"
	"time"
)

func TestNewLockManager(t *testing.T) {
	lm := NewLockManager()

	if lm == nil {
		t.Fatal("NewLockManager() returned nil")
	}

	if lm.pageLocks == nil {
		t.Error("pageLocks map not initialized")
	}

	if lm.txLocks == nil {
		t.Error("txLocks map not initialized")
	}

	if lm.waitingFor == nil {
		t.Error("waitingFor map not initialized")
	}

	if lm.depGraph == nil {
		t.Error("depGraph not initialized")
	}

	if lm.waitQueue == nil {
		t.Error("waitQueue map not initialized")
	}
}

func TestLockPageSharedLock(t *testing.T) {
	lm := NewLockManager()
	tid := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	err := lm.LockPage(tid, pid, false) // shared lock
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Verify lock was granted
	locks := lm.pageLocks[pid]
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock, got %d", len(locks))
	}

	if locks[0].TID != tid {
		t.Error("Lock not associated with correct transaction")
	}

	if locks[0].LockType != SharedLock {
		t.Error("Expected shared lock type")
	}

	// Verify transaction mapping
	txLocks := lm.txLocks[tid]
	if txLocks == nil {
		t.Fatal("Transaction locks not recorded")
	}

	if txLocks[pid] != SharedLock {
		t.Error("Transaction lock type not recorded correctly")
	}
}

func TestLockPageExclusiveLock(t *testing.T) {
	lm := NewLockManager()
	tid := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	err := lm.LockPage(tid, pid, true) // exclusive lock
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Verify lock was granted
	locks := lm.pageLocks[pid]
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock, got %d", len(locks))
	}

	if locks[0].TID != tid {
		t.Error("Lock not associated with correct transaction")
	}

	if locks[0].LockType != ExclusiveLock {
		t.Error("Expected exclusive lock type")
	}
}

func TestMultipleSharedLocks(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First transaction gets shared lock
	err := lm.LockPage(tid1, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire first shared lock: %v", err)
	}

	// Second transaction should also get shared lock
	err = lm.LockPage(tid2, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire second shared lock: %v", err)
	}

	// Verify both locks exist
	locks := lm.pageLocks[pid]
	if len(locks) != 2 {
		t.Fatalf("Expected 2 locks, got %d", len(locks))
	}

	// Verify both are shared locks
	for _, lock := range locks {
		if lock.LockType != SharedLock {
			t.Error("Expected shared lock type")
		}
	}
}

func TestExclusiveLockBlocksSharedLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First transaction gets exclusive lock
	err := lm.LockPage(tid1, pid, true)
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Second transaction should timeout trying to get shared lock
	done := make(chan error, 1)
	go func() {
		done <- lm.LockPage(tid2, pid, false)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Expected timeout error when acquiring conflicting lock")
		}
	case <-time.After(200 * time.Millisecond):
		// This is expected - the lock should be blocked
	}
}

func TestSharedLockBlocksExclusiveLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First transaction gets shared lock
	err := lm.LockPage(tid1, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Second transaction should timeout trying to get exclusive lock
	done := make(chan error, 1)
	go func() {
		done <- lm.LockPage(tid2, pid, true)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Expected timeout error when acquiring conflicting lock")
		}
	case <-time.After(200 * time.Millisecond):
		// This is expected - the lock should be blocked
	}
}

func TestLockUpgrade(t *testing.T) {
	lm := NewLockManager()
	tid := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First acquire shared lock
	err := lm.LockPage(tid, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Verify shared lock
	if lm.txLocks[tid][pid] != SharedLock {
		t.Error("Expected shared lock initially")
	}

	// Upgrade to exclusive lock
	err = lm.LockPage(tid, pid, true)
	if err != nil {
		t.Fatalf("Failed to upgrade to exclusive lock: %v", err)
	}

	// Verify upgrade
	if lm.txLocks[tid][pid] != ExclusiveLock {
		t.Error("Expected exclusive lock after upgrade")
	}

	// Verify only one lock exists
	locks := lm.pageLocks[pid]
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock after upgrade, got %d", len(locks))
	}

	if locks[0].LockType != ExclusiveLock {
		t.Error("Expected exclusive lock type after upgrade")
	}
}

func TestLockUpgradeBlocked(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Both transactions get shared locks
	err := lm.LockPage(tid1, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire first shared lock: %v", err)
	}

	err = lm.LockPage(tid2, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire second shared lock: %v", err)
	}

	// First transaction should timeout trying to upgrade
	done := make(chan error, 1)
	go func() {
		done <- lm.LockPage(tid1, pid, true)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Expected timeout error when upgrading with other shared locks present")
		}
	case <-time.After(200 * time.Millisecond):
		// This is expected - the upgrade should be blocked
	}
}

func TestAlreadyHasLock(t *testing.T) {
	lm := NewLockManager()
	tid := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Acquire shared lock
	err := lm.LockPage(tid, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Try to acquire same shared lock again - should succeed immediately
	err = lm.LockPage(tid, pid, false)
	if err != nil {
		t.Fatalf("Failed to reacquire same shared lock: %v", err)
	}

	// Acquire exclusive lock
	err = lm.LockPage(tid, pid, true)
	if err != nil {
		t.Fatalf("Failed to upgrade to exclusive lock: %v", err)
	}

	// Try to acquire shared lock when we have exclusive - should succeed
	err = lm.LockPage(tid, pid, false)
	if err != nil {
		t.Fatalf("Failed to get shared lock when holding exclusive: %v", err)
	}

	// Try to acquire exclusive lock again - should succeed
	err = lm.LockPage(tid, pid, true)
	if err != nil {
		t.Fatalf("Failed to reacquire exclusive lock: %v", err)
	}
}

func TestDeadlockDetection(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)

	// tid1 gets exclusive lock on pid1
	err := lm.LockPage(tid1, pid1, true)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}

	// tid2 gets exclusive lock on pid2
	err = lm.LockPage(tid2, pid2, true)
	if err != nil {
		t.Fatalf("Failed to acquire second lock: %v", err)
	}

	// Now create deadlock: tid1 tries to get pid2, tid2 tries to get pid1
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)

	go func() {
		done1 <- lm.LockPage(tid1, pid2, true)
	}()

	// Small delay to ensure first goroutine starts
	time.Sleep(10 * time.Millisecond)

	go func() {
		done2 <- lm.LockPage(tid2, pid1, true)
	}()

	// At least one should detect deadlock
	deadlockDetected := false
	timeout := time.After(1 * time.Second)

	for i := 0; i < 2; i++ {
		select {
		case err1 := <-done1:
			if err1 != nil {
				t.Logf("Transaction 1 error: %v", err1)
				if containsDeadlockMessage(err1.Error(), tid1.ID()) {
					deadlockDetected = true
				}
			}
		case err2 := <-done2:
			if err2 != nil {
				t.Logf("Transaction 2 error: %v", err2)
				if containsDeadlockMessage(err2.Error(), tid2.ID()) {
					deadlockDetected = true
				}
			}
		case <-timeout:
			t.Log("Test timed out")
			break
		}
		if deadlockDetected {
			break
		}
	}

	if !deadlockDetected {
		t.Error("Expected deadlock to be detected")
	}
}

func containsDeadlockMessage(errorMsg string, tidID int64) bool {
	expectedMsg := fmt.Sprintf("deadlock detected for transaction %d", tidID)
	return errorMsg == expectedMsg
}

func TestConcurrentLockAcquisition(t *testing.T) {
	lm := NewLockManager()
	pid := heap.NewHeapPageID(1, 1)
	numGoroutines := 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Multiple goroutines trying to get shared locks concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tid := transaction.NewTransactionID()
			err := lm.LockPage(tid, pid, false)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Unexpected error in concurrent shared lock acquisition: %v", err)
	}

	// Verify all locks were granted
	locks := lm.pageLocks[pid]
	if len(locks) != numGoroutines {
		t.Errorf("Expected %d locks, got %d", numGoroutines, len(locks))
	}
}

func TestCanGrantLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Empty page - should grant any lock
	if !lm.canGrantLock(tid1, pid, SharedLock) {
		t.Error("Should grant shared lock on empty page")
	}

	if !lm.canGrantLock(tid1, pid, ExclusiveLock) {
		t.Error("Should grant exclusive lock on empty page")
	}

	// Add shared lock
	lm.grantLock(tid1, pid, SharedLock)

	// Should grant another shared lock
	if !lm.canGrantLock(tid2, pid, SharedLock) {
		t.Error("Should grant shared lock when shared lock exists")
	}

	// Should not grant exclusive lock
	if lm.canGrantLock(tid2, pid, ExclusiveLock) {
		t.Error("Should not grant exclusive lock when shared lock exists")
	}

	// Same transaction should be able to get exclusive lock
	if !lm.canGrantLock(tid1, pid, ExclusiveLock) {
		t.Error("Should allow same transaction to upgrade to exclusive")
	}
}

func TestCanUpgradeLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Single shared lock - should be able to upgrade
	lm.grantLock(tid1, pid, SharedLock)
	if !lm.canUpgradeLock(tid1, pid) {
		t.Error("Should be able to upgrade when holding only lock")
	}

	// Add another shared lock
	lm.grantLock(tid2, pid, SharedLock)

	// Should not be able to upgrade now
	if lm.canUpgradeLock(tid1, pid) {
		t.Error("Should not be able to upgrade when other locks exist")
	}
}

func TestHasLock(t *testing.T) {
	lm := NewLockManager()
	tid := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// No lock initially
	if lm.hasLock(tid, pid, SharedLock) {
		t.Error("Should not have shared lock initially")
	}

	if lm.hasLock(tid, pid, ExclusiveLock) {
		t.Error("Should not have exclusive lock initially")
	}

	// Grant shared lock
	lm.grantLock(tid, pid, SharedLock)

	if !lm.hasLock(tid, pid, SharedLock) {
		t.Error("Should have shared lock after granting")
	}

	if lm.hasLock(tid, pid, ExclusiveLock) {
		t.Error("Should not have exclusive lock when only shared is granted")
	}

	// Upgrade to exclusive
	lm.upgradeLock(tid, pid)

	if !lm.hasLock(tid, pid, ExclusiveLock) {
		t.Error("Should have exclusive lock after upgrade")
	}
}

func TestWaitQueueOperations(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add to wait queue
	lm.addToWaitQueue(tid1, pid, SharedLock)
	lm.addToWaitQueue(tid2, pid, ExclusiveLock)

	// Verify wait queue
	queue := lm.waitQueue[pid]
	if len(queue) != 2 {
		t.Fatalf("Expected 2 items in wait queue, got %d", len(queue))
	}

	// Verify waiting for tracking
	if len(lm.waitingFor[tid1]) != 1 || lm.waitingFor[tid1][0] != pid {
		t.Error("Transaction 1 waiting tracking incorrect")
	}

	if len(lm.waitingFor[tid2]) != 1 || lm.waitingFor[tid2][0] != pid {
		t.Error("Transaction 2 waiting tracking incorrect")
	}

	// Remove from wait queue
	lm.removeFromWaitQueue(tid1, pid)

	// Verify removal
	queue = lm.waitQueue[pid]
	if len(queue) != 1 {
		t.Fatalf("Expected 1 item in wait queue after removal, got %d", len(queue))
	}

	if queue[0].TID != tid2 {
		t.Error("Wrong transaction remaining in queue")
	}

	// Verify waiting tracking removed
	if _, exists := lm.waitingFor[tid1]; exists {
		t.Error("Transaction 1 should not be in waiting tracking after removal")
	}

	// Remove last item
	lm.removeFromWaitQueue(tid2, pid)

	// Verify queue is deleted
	if _, exists := lm.waitQueue[pid]; exists {
		t.Error("Wait queue should be deleted when empty")
	}
}

func TestUpdateDependencies(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	tid3 := transaction.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant some locks
	lm.grantLock(tid1, pid, SharedLock)
	lm.grantLock(tid2, pid, SharedLock)

	// tid3 wants exclusive lock - should create dependencies
	lm.updateDependencies(tid3, pid, ExclusiveLock)

	// Verify dependencies exist
	waiters := lm.depGraph.GetWaitingTransactions()
	if len(waiters) != 1 || waiters[0] != tid3 {
		t.Error("Expected tid3 to be waiting in dependency graph")
	}

	// Grant exclusive lock to tid1, then tid2 wants exclusive
	lm = NewLockManager() // Reset
	lm.grantLock(tid1, pid, ExclusiveLock)
	lm.updateDependencies(tid2, pid, ExclusiveLock)

	// Verify dependency
	waiters = lm.depGraph.GetWaitingTransactions()
	if len(waiters) != 1 || waiters[0] != tid2 {
		t.Error("Expected tid2 to be waiting for tid1")
	}
}

func TestMultiplePageWaiting(t *testing.T) {
	lm := NewLockManager()
	tid1 := transaction.NewTransactionID()
	tid2 := transaction.NewTransactionID()
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)

	// tid1 gets exclusive locks on both pages
	err := lm.LockPage(tid1, pid1, true)
	if err != nil {
		t.Fatalf("Failed to acquire lock on pid1: %v", err)
	}

	err = lm.LockPage(tid1, pid2, true)
	if err != nil {
		t.Fatalf("Failed to acquire lock on pid2: %v", err)
	}

	// tid2 tries to get both pages - should be added to wait queue for both
	go func() {
		lm.LockPage(tid2, pid1, true)
	}()

	go func() {
		lm.LockPage(tid2, pid2, true)
	}()

	// Give some time for the waiting to be registered
	time.Sleep(50 * time.Millisecond)

	lm.mutex.RLock()
	waitingPages := lm.waitingFor[tid2]
	lm.mutex.RUnlock()

	if len(waitingPages) != 2 {
		t.Errorf("Expected transaction to be waiting for 2 pages, got %d", len(waitingPages))
	}

	// Remove from wait queue for one page specifically
	lm.mutex.Lock()
	lm.removeFromWaitQueue(tid2, pid1)
	lm.mutex.Unlock()

	lm.mutex.RLock()
	waitingPages = lm.waitingFor[tid2]
	lm.mutex.RUnlock()

	if len(waitingPages) != 1 {
		t.Errorf("Expected transaction to be waiting for 1 page after removal, got %d", len(waitingPages))
	}

	if !waitingPages[0].Equals(pid2) {
		t.Error("Expected transaction to still be waiting for pid2")
	}
}
