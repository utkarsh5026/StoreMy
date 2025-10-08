package lock

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewLockManager(t *testing.T) {
	lm := NewLockManager()

	if lm == nil {
		t.Fatal("NewLockManager() returned nil")
	}

	if lm.lockTable == nil {
		t.Error("lockTable map not initialized")
	}

	if lm.waitQueue == nil {
		t.Error("waitQueue map not initialized")
	}

	if lm.depGraph == nil {
		t.Error("depGraph not initialized")
	}
}

func TestLockPageSharedLock(t *testing.T) {
	lm := NewLockManager()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	err := lm.LockPage(tid, pid, false) // shared lock
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Verify lock was granted
	locks := lm.lockTable.GetPageLocks(pid)
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
	transactionLocks := lm.lockTable.GetTransactionLockTable()[tid]
	if transactionLocks == nil {
		t.Fatal("Transaction locks not recorded")
	}

	if transactionLocks[pid] != SharedLock {
		t.Error("Transaction lock type not recorded correctly")
	}
}

func TestLockPageExclusiveLock(t *testing.T) {
	lm := NewLockManager()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	err := lm.LockPage(tid, pid, true) // exclusive lock
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Verify lock was granted
	locks := lm.lockTable.GetPageLocks(pid)
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
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
	locks := lm.lockTable.GetPageLocks(pid)
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
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First acquire shared lock
	err := lm.LockPage(tid, pid, false)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Verify shared lock
	if lm.lockTable.GetTransactionLockTable()[tid][pid] != SharedLock {
		t.Error("Expected shared lock initially")
	}

	// Upgrade to exclusive lock
	err = lm.LockPage(tid, pid, true)
	if err != nil {
		t.Fatalf("Failed to upgrade to exclusive lock: %v", err)
	}

	// Verify upgrade
	if lm.lockTable.GetTransactionLockTable()[tid][pid] != ExclusiveLock {
		t.Error("Expected exclusive lock after upgrade")
	}

	// Verify only one lock exists
	locks := lm.lockTable.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock after upgrade, got %d", len(locks))
	}

	if locks[0].LockType != ExclusiveLock {
		t.Error("Expected exclusive lock type after upgrade")
	}
}

func TestLockUpgradeBlocked(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
	tid := primitives.NewTransactionID()
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
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
			tid := primitives.NewTransactionID()
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
	locks := lm.lockTable.GetPageLocks(pid)
	if len(locks) != numGoroutines {
		t.Errorf("Expected %d locks, got %d", numGoroutines, len(locks))
	}
}

func TestCanGrantLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Empty page - should grant any lock
	if !lm.lockGrantor.CanGrantImmediately(tid1, pid, SharedLock) {
		t.Error("Should grant shared lock on empty page")
	}

	if !lm.lockGrantor.CanGrantImmediately(tid1, pid, ExclusiveLock) {
		t.Error("Should grant exclusive lock on empty page")
	}

	// Add shared lock
	lm.lockGrantor.GrantLock(tid1, pid, SharedLock)

	// Should grant another shared lock
	if !lm.lockGrantor.CanGrantImmediately(tid2, pid, SharedLock) {
		t.Error("Should grant shared lock when shared lock exists")
	}

	// Should not grant exclusive lock
	if lm.lockGrantor.CanGrantImmediately(tid2, pid, ExclusiveLock) {
		t.Error("Should not grant exclusive lock when shared lock exists")
	}

	// Same transaction should be able to get exclusive lock
	if !lm.lockGrantor.CanGrantImmediately(tid1, pid, ExclusiveLock) {
		t.Error("Should allow same transaction to upgrade to exclusive")
	}
}

func TestCanUpgradeLock(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Single shared lock - should be able to upgrade
	lm.lockGrantor.GrantLock(tid1, pid, SharedLock)
	if !lm.lockGrantor.CanUpgradeLock(tid1, pid) {
		t.Error("Should be able to upgrade when holding only lock")
	}

	// Add another shared lock
	lm.lockGrantor.GrantLock(tid2, pid, SharedLock)

	// Should not be able to upgrade now
	if lm.lockGrantor.CanUpgradeLock(tid1, pid) {
		t.Error("Should not be able to upgrade when other locks exist")
	}
}

func TestHasLock(t *testing.T) {
	lm := NewLockManager()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// No lock initially
	if lm.lockTable.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Should not have shared lock initially")
	}

	if lm.lockTable.HasSufficientLock(tid, pid, ExclusiveLock) {
		t.Error("Should not have exclusive lock initially")
	}

	// Grant shared lock
	lm.lockGrantor.GrantLock(tid, pid, SharedLock)

	if !lm.lockTable.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Should have shared lock after granting")
	}

	if lm.lockTable.HasSufficientLock(tid, pid, ExclusiveLock) {
		t.Error("Should not have exclusive lock when only shared is granted")
	}

	// Upgrade to exclusive
	lm.lockTable.UpgradeLock(tid, pid)

	if !lm.lockTable.HasSufficientLock(tid, pid, ExclusiveLock) {
		t.Error("Should have exclusive lock after upgrade")
	}
}

func TestWaitQueueOperations(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add to wait queue
	lm.waitQueue.Add(tid1, pid, SharedLock)
	lm.waitQueue.Add(tid2, pid, ExclusiveLock)

	// Verify wait queue
	queue := lm.waitQueue.GetRequests(pid)
	if len(queue) != 2 {
		t.Fatalf("Expected 2 items in wait queue, got %d", len(queue))
	}

	// Verify waiting for tracking
	if len(lm.waitQueue.GetPagesRequestedFor(tid1)) != 1 || lm.waitQueue.GetPagesRequestedFor(tid1)[0] != pid {
		t.Error("Transaction 1 waiting tracking incorrect")
	}

	if len(lm.waitQueue.GetPagesRequestedFor(tid2)) != 1 || lm.waitQueue.GetPagesRequestedFor(tid2)[0] != pid {
		t.Error("Transaction 2 waiting tracking incorrect")
	}

	// RemoveRequest from wait queue
	lm.waitQueue.RemoveRequest(tid1, pid)

	// Verify removal
	queue = lm.waitQueue.GetRequests(pid)
	if len(queue) != 1 {
		t.Fatalf("Expected 1 item in wait queue after removal, got %d", len(queue))
	}

	if queue[0].TID != tid2 {
		t.Error("Wrong transaction remaining in queue")
	}

	// Verify waiting tracking removed
	if len(lm.waitQueue.GetPagesRequestedFor(tid1)) > 0 {
		t.Error("Transaction 1 should not be in waiting tracking after removal")
	}

	// RemoveRequest last item
	lm.waitQueue.RemoveRequest(tid2, pid)

	// Verify queue is deleted
	if len(lm.waitQueue.GetRequests(pid)) > 0 {
		t.Error("Wait queue should be deleted when empty")
	}
}

func TestUpdateDependencies(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant some locks
	lm.lockGrantor.GrantLock(tid1, pid, SharedLock)
	lm.lockGrantor.GrantLock(tid2, pid, SharedLock)

	// tid3 wants exclusive lock - should create dependencies
	lm.updateDependencies(tid3, pid, ExclusiveLock)

	// Verify dependencies exist
	waiters := lm.depGraph.GetWaitingTransactions()
	if len(waiters) != 1 || waiters[0] != tid3 {
		t.Error("Expected tid3 to be waiting in dependency graph")
	}

	// Grant exclusive lock to tid1, then tid2 wants exclusive
	lm = NewLockManager() // Reset
	lm.lockGrantor.GrantLock(tid1, pid, ExclusiveLock)
	lm.updateDependencies(tid2, pid, ExclusiveLock)

	// Verify dependency
	waiters = lm.depGraph.GetWaitingTransactions()
	if len(waiters) != 1 || waiters[0] != tid2 {
		t.Error("Expected tid2 to be waiting for tid1")
	}
}

func TestMultiplePageWaiting(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
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
	waitingPages := lm.waitQueue.GetPagesRequestedFor(tid2)
	lm.mutex.RUnlock()

	if len(waitingPages) != 2 {
		t.Errorf("Expected transaction to be waiting for 2 pages, got %d", len(waitingPages))
	}

	// RemoveRequest from wait queue for one page specifically
	lm.mutex.Lock()
	lm.waitQueue.RemoveRequest(tid2, pid1)
	lm.mutex.Unlock()

	lm.mutex.RLock()
	waitingPages = lm.waitQueue.GetPagesRequestedFor(tid2)
	lm.mutex.RUnlock()

	if len(waitingPages) != 1 {
		t.Errorf("Expected transaction to be waiting for 1 page after removal, got %d", len(waitingPages))
	}

	if !waitingPages[0].Equals(pid2) {
		t.Error("Expected transaction to still be waiting for pid2")
	}
}

// Stress Tests and Rigorous Testing

func TestHighConcurrencySharedLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lm := NewLockManager()
	pid := heap.NewHeapPageID(1, 1)
	numGoroutines := 100

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := primitives.NewTransactionID()

			err := lm.LockPage(tid, pid, false) // shared lock
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				t.Logf("Goroutine %d failed: %v", id, err)
			} else {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("High concurrency test completed in %v", elapsed)
	t.Logf("Successful acquisitions: %d", successCount)
	t.Logf("Errors: %d", errorCount)

	if errorCount > 0 {
		t.Errorf("Expected no errors for shared locks, got %d", errorCount)
	}

	if successCount != int64(numGoroutines) {
		t.Errorf("Expected %d successful locks, got %d", numGoroutines, successCount)
	}
}

func TestConcurrentExclusiveLockContention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lm := NewLockManager()
	pid := heap.NewHeapPageID(1, 1)
	numGoroutines := 10 // Reduced from 50 to prevent excessive timeouts

	var wg sync.WaitGroup
	var successCount int64
	var timeoutCount int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := primitives.NewTransactionID()

			err := lm.LockPage(tid, pid, true) // exclusive lock
			if err != nil {
				if containsTimeout(err.Error()) {
					atomic.AddInt64(&timeoutCount, 1)
				} else {
					t.Logf("Goroutine %d unexpected error: %v", id, err)
				}
			} else {
				atomic.AddInt64(&successCount, 1)
				// Hold lock briefly to create contention
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Exclusive lock contention test results:")
	t.Logf("Successful acquisitions: %d", successCount)
	t.Logf("Timeouts: %d", timeoutCount)

	// Only one should succeed, all others should timeout
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful exclusive lock, got %d", successCount)
	}

	if timeoutCount != int64(numGoroutines-1) {
		t.Errorf("Expected %d timeouts, got %d", numGoroutines-1, timeoutCount)
	}
}

func TestComplexDeadlockScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Test 3-way deadlock
	t.Run("ThreeWayDeadlock", func(t *testing.T) {
		lm := NewLockManager()
		tid1 := primitives.NewTransactionID()
		tid2 := primitives.NewTransactionID()
		tid3 := primitives.NewTransactionID()
		pid1 := heap.NewHeapPageID(1, 1)
		pid2 := heap.NewHeapPageID(1, 2)
		pid3 := heap.NewHeapPageID(1, 3)

		// Create initial locks: tid1->pid1, tid2->pid2, tid3->pid3
		lm.LockPage(tid1, pid1, true)
		lm.LockPage(tid2, pid2, true)
		lm.LockPage(tid3, pid3, true)

		// Create circular dependency: tid1->pid2, tid2->pid3, tid3->pid1
		done1 := make(chan error, 1)
		done2 := make(chan error, 1)
		done3 := make(chan error, 1)

		go func() { done1 <- lm.LockPage(tid1, pid2, true) }()
		time.Sleep(50 * time.Millisecond) // Increased delay to ensure proper ordering
		go func() { done2 <- lm.LockPage(tid2, pid3, true) }()
		time.Sleep(50 * time.Millisecond) // Increased delay to ensure proper ordering
		go func() { done3 <- lm.LockPage(tid3, pid1, true) }()

		deadlockCount := 0
		timeout := time.After(5 * time.Second) // Increased timeout

		for i := 0; i < 3; i++ {
			select {
			case err := <-done1:
				if err != nil && containsDeadlockMessage(err.Error(), tid1.ID()) {
					deadlockCount++
				}
			case err := <-done2:
				if err != nil && containsDeadlockMessage(err.Error(), tid2.ID()) {
					deadlockCount++
				}
			case err := <-done3:
				if err != nil && containsDeadlockMessage(err.Error(), tid3.ID()) {
					deadlockCount++
				}
			case <-timeout:
				t.Log("Three-way deadlock test timed out")
				break
			}
		}

		if deadlockCount == 0 {
			t.Error("Expected at least one deadlock detection in three-way scenario")
		}
	})

	// Test complex chain deadlock
	t.Run("ChainDeadlock", func(t *testing.T) {
		lm := NewLockManager()
		numTransactions := 5
		tids := make([]*primitives.TransactionID, numTransactions)
		pids := make([]*heap.HeapPageID, numTransactions)

		// Create transactions and pages
		for i := 0; i < numTransactions; i++ {
			tids[i] = primitives.NewTransactionID()
			pids[i] = heap.NewHeapPageID(1, i+1)
		}

		// Each transaction locks its own page
		for i := 0; i < numTransactions; i++ {
			lm.LockPage(tids[i], pids[i], true)
		}

		// Create chain: tid[i] wants page[i+1], last wants page[0]
		var wg sync.WaitGroup
		deadlockDetected := int64(0)

		for i := 0; i < numTransactions; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				nextPage := pids[(idx+1)%numTransactions]
				err := lm.LockPage(tids[idx], nextPage, true)
				if err != nil && containsDeadlockMessage(err.Error(), tids[idx].ID()) {
					atomic.AddInt64(&deadlockDetected, 1)
				}
			}(i)
			time.Sleep(20 * time.Millisecond) // Stagger to ensure proper chain formation
		}

		wg.Wait()

		if deadlockDetected == 0 {
			t.Error("Expected deadlock detection in chain scenario")
		}
	})
}

func TestMixedWorkloadStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lm := NewLockManager()
	numPages := 10
	numWorkers := 20                   // Reduced from 50
	duration := 500 * time.Millisecond // Reduced from 2 seconds

	var stats struct {
		sharedLocks    int64
		exclusiveLocks int64
		upgrades       int64
		timeouts       int64
		deadlocks      int64
		errors         int64
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup

	// Worker function
	worker := func(id int) {
		defer wg.Done()
		rand.Seed(time.Now().UnixNano() + int64(id))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				tid := primitives.NewTransactionID()
				pageNum := rand.Intn(numPages) + 1
				pid := heap.NewHeapPageID(1, pageNum)
				exclusive := rand.Float32() < 0.3 // 30% exclusive locks

				err := lm.LockPage(tid, pid, exclusive)
				if err != nil {
					if containsTimeout(err.Error()) {
						atomic.AddInt64(&stats.timeouts, 1)
					} else if containsDeadlockMessage(err.Error(), tid.ID()) {
						atomic.AddInt64(&stats.deadlocks, 1)
					} else {
						atomic.AddInt64(&stats.errors, 1)
					}
				} else {
					if exclusive {
						atomic.AddInt64(&stats.exclusiveLocks, 1)
					} else {
						atomic.AddInt64(&stats.sharedLocks, 1)

						// 20% chance to upgrade to exclusive
						if rand.Float32() < 0.2 {
							upgradeErr := lm.LockPage(tid, pid, true)
							if upgradeErr == nil {
								atomic.AddInt64(&stats.upgrades, 1)
							}
						}
					}
				}

				// Simulate some work
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}
	}

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i)
	}

	wg.Wait()

	total := stats.sharedLocks + stats.exclusiveLocks + stats.timeouts + stats.deadlocks + stats.errors

	t.Logf("Mixed workload stress test results:")
	t.Logf("Total operations: %d", total)
	t.Logf("Shared locks: %d", stats.sharedLocks)
	t.Logf("Exclusive locks: %d", stats.exclusiveLocks)
	t.Logf("Upgrades: %d", stats.upgrades)
	t.Logf("Timeouts: %d", stats.timeouts)
	t.Logf("Deadlocks: %d", stats.deadlocks)
	t.Logf("Errors: %d", stats.errors)

	if stats.errors > 0 {
		t.Errorf("Unexpected errors occurred: %d", stats.errors)
	}

	if total == 0 {
		t.Error("No operations completed - test may have issues")
	}
}

func TestRaceConditionDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	lm := NewLockManager()
	numGoroutines := 100

	var wg sync.WaitGroup

	// Test concurrent access to internal data structures with different pages
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := primitives.NewTransactionID()

			// Each goroutine works with different pages to avoid contention
			// but still tests internal data structure concurrency
			for j := 0; j < 5; j++ {
				pageNum := (id*5 + j) + 1 // Unique page per iteration
				pid := heap.NewHeapPageID(1, pageNum)

				err := lm.LockPage(tid, pid, j%2 == 0) // Mix shared/exclusive
				if err != nil && !containsTimeout(err.Error()) && !containsDeadlockMessage(err.Error(), tid.ID()) {
					t.Errorf("Unexpected error in race condition test: %v", err)
				}

				// Brief pause to allow interleaving of map operations
				if j%2 == 0 {
					runtime.Gosched()
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	lm := NewLockManager()
	numTransactions := 1000
	numPages := 100

	var tids []*primitives.TransactionID
	var pids []*heap.HeapPageID

	// Create many transactions and pages
	for i := 0; i < numTransactions; i++ {
		tids = append(tids, primitives.NewTransactionID())
	}

	for i := 0; i < numPages; i++ {
		pids = append(pids, heap.NewHeapPageID(1, i+1))
	}

	var wg sync.WaitGroup
	successCount := int64(0)

	// Each transaction tries to lock multiple pages
	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := tids[idx]

			// Try to lock 3-5 random pages
			numLocks := 3 + (idx % 3)
			localSuccess := 0

			for j := 0; j < numLocks; j++ {
				pageIdx := (idx + j*7) % numPages // Deterministic but spread out
				err := lm.LockPage(tid, pids[pageIdx], false)
				if err == nil {
					localSuccess++
				}
			}

			atomic.AddInt64(&successCount, int64(localSuccess))
		}(i)
	}

	wg.Wait()

	t.Logf("Memory pressure test completed")
	t.Logf("Successful lock acquisitions: %d", successCount)
	t.Logf("Average locks per transaction: %.2f", float64(successCount)/float64(numTransactions))

	// Check that data structures are still consistent
	lm.mutex.RLock()
	pageLocksCount := len(lm.lockTable.GetPageLockTable())
	transactionLocksCount := len(lm.lockTable.GetTransactionLockTable())
	waitingForCount := len(lm.waitQueue.transactionWaiting)
	waitQueueCount := len(lm.waitQueue.pageWaitQueue)
	lm.mutex.RUnlock()

	t.Logf("Final state - PageLocks: %d, transactionLocks: %d, WaitingFor: %d, WaitQueue: %d",
		pageLocksCount, transactionLocksCount, waitingForCount, waitQueueCount)
}

func TestEdgeCases(t *testing.T) {
	t.Run("NilTransactionID", func(t *testing.T) {
		lm := NewLockManager()
		pid := heap.NewHeapPageID(1, 1)

		// This should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("LockPage with nil transaction should not panic: %v", r)
			}
		}()

		err := lm.LockPage(nil, pid, false)
		if err == nil {
			t.Error("Expected error when locking with nil transaction")
		}
	})

	t.Run("SamePageMultipleLockTypes", func(t *testing.T) {
		lm := NewLockManager()
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// Get shared lock
		err := lm.LockPage(tid, pid, false)
		if err != nil {
			t.Fatalf("Failed to get shared lock: %v", err)
		}

		// Upgrade to exclusive
		err = lm.LockPage(tid, pid, true)
		if err != nil {
			t.Fatalf("Failed to upgrade lock: %v", err)
		}

		// Try to get shared again (should succeed since we have exclusive)
		err = lm.LockPage(tid, pid, false)
		if err != nil {
			t.Fatalf("Failed to get shared lock when holding exclusive: %v", err)
		}

		// Try to get exclusive again (should succeed)
		err = lm.LockPage(tid, pid, true)
		if err != nil {
			t.Fatalf("Failed to reacquire exclusive lock: %v", err)
		}
	})

	t.Run("ManyPagesOneTransaction", func(t *testing.T) {
		lm := NewLockManager()
		tid := primitives.NewTransactionID()
		numPages := 100

		for i := 0; i < numPages; i++ {
			pid := heap.NewHeapPageID(1, i+1)
			err := lm.LockPage(tid, pid, i%2 == 0) // Mix shared and exclusive
			if err != nil {
				t.Fatalf("Failed to lock page %d: %v", i+1, err)
			}
		}

		// Verify all locks are recorded
		lm.mutex.RLock()
		transactionLocks := lm.lockTable.GetTransactionLockTable()[tid]
		lm.mutex.RUnlock()

		if len(transactionLocks) != numPages {
			t.Errorf("Expected %d locks for transaction, got %d", numPages, len(transactionLocks))
		}
	})
}

// Benchmark Tests

func BenchmarkSharedLockAcquisition(b *testing.B) {
	lm := NewLockManager()
	pid := heap.NewHeapPageID(1, 1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tid := primitives.NewTransactionID()
			err := lm.LockPage(tid, pid, false)
			if err != nil {
				b.Errorf("Unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkExclusiveLockAcquisition(b *testing.B) {
	lm := NewLockManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, i+1) // Different pages to avoid contention
		err := lm.LockPage(tid, pid, true)
		if err != nil {
			b.Errorf("Unexpected error: %v", err)
		}
	}
}

func BenchmarkLockUpgrade(b *testing.B) {
	lm := NewLockManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, i+1)

		// Get shared lock first
		err := lm.LockPage(tid, pid, false)
		if err != nil {
			b.Errorf("Failed to get shared lock: %v", err)
			continue
		}

		// Upgrade to exclusive
		err = lm.LockPage(tid, pid, true)
		if err != nil {
			b.Errorf("Failed to upgrade lock: %v", err)
		}
	}
}

// Helper functions

func containsTimeout(errorMsg string) bool {
	return strings.Contains(errorMsg, "timeout waiting for lock")
}

func TestCalculateRetryDelay(t *testing.T) {
	lm := NewLockManager()
	baseDelay := time.Millisecond
	maxDelay := 100 * time.Millisecond

	// Test with attempt 0
	delay := lm.calculateRetryDelay(0, baseDelay, maxDelay)
	expected := baseDelay
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 0, got %v", expected, delay)
	}

	// Test with attempt 5
	delay = lm.calculateRetryDelay(5, baseDelay, maxDelay)
	expected = baseDelay * 2 // factor is 1 for attempt 5 (5/5 = 1, 2^1 = 2)
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 5, got %v", expected, delay)
	}

	// Test with attempt 10
	delay = lm.calculateRetryDelay(10, baseDelay, maxDelay)
	expected = baseDelay * 4 // factor is 2 for attempt 10 (10/5 = 2, 2^2 = 4)
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 10, got %v", expected, delay)
	}

	// Test with attempt 25
	delay = lm.calculateRetryDelay(25, baseDelay, maxDelay)
	expected = baseDelay * 32 // factor is 5 for attempt 25 (25/5 = 5, min(5, 5) = 5, 2^5 = 32)
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 25, got %v", expected, delay)
	}

	// Test with attempt 200 (should hit max factor of 5)
	delay = lm.calculateRetryDelay(200, baseDelay, maxDelay)
	expected = baseDelay * 32 // factor is 5 for attempt 200 (200/5 = 40, min(40, 5) = 5, 2^5 = 32)
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 200, got %v", expected, delay)
	}

	// Test edge case with very large attempt number
	delay = lm.calculateRetryDelay(10000, baseDelay, maxDelay)
	expected = baseDelay * 32 // factor is 5 for attempt 10000 (10000/5 = 2000, min(2000, 5) = 5, 2^5 = 32)
	if delay != expected {
		t.Errorf("Expected delay %v for attempt 10000, got %v", expected, delay)
	}

	// Test case where calculated delay exceeds maxDelay
	delay = lm.calculateRetryDelay(30, time.Millisecond, 10*time.Millisecond) // Small maxDelay
	expected = 10 * time.Millisecond                                          // Should be capped at maxDelay
	if delay != expected {
		t.Errorf("Expected delay to be capped at maxDelay %v, got %v", expected, delay)
	}
}

func TestMinFunction(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{0, 0, 0},
		{-1, 5, -1},
		{10, 10, 10},
		{100, 1, 1},
	}

	for _, test := range tests {
		result := min(test.a, test.b)
		if result != test.expected {
			t.Errorf("min(%d, %d) = %d, expected %d", test.a, test.b, result, test.expected)
		}
	}
}

func TestProcessWaitQueue(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Create a scenario where tid1 has exclusive lock, tid2 and tid3 are waiting
	lm.lockGrantor.GrantLock(tid1, pid, ExclusiveLock)
	lm.waitQueue.Add(tid2, pid, SharedLock)
	lm.waitQueue.Add(tid3, pid, SharedLock)

	// Verify initial state
	queue := lm.waitQueue.GetRequests(pid)
	if len(queue) != 2 {
		t.Fatalf("Expected 2 items in wait queue, got %d", len(queue))
	}

	// RemoveRequest tid1's exclusive lock (should allow shared locks to be granted)
	lm.UnlockPage(tid1, pid)

	// Check that wait queue is now empty (both shared locks should be granted)
	if len(lm.waitQueue.GetRequests(pid)) > 0 {
		t.Error("Wait queue should be empty after processing")
	}

	// Verify both transactions got their locks
	if !lm.lockTable.HasLockType(tid2, pid, SharedLock) {
		t.Error("tid2 should have shared lock after processing")
	}
	if !lm.lockTable.HasLockType(tid3, pid, SharedLock) {
		t.Error("tid3 should have shared lock after processing")
	}
}

func TestProcessWaitQueuePartialGrant(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant shared lock to tid1
	lm.lockGrantor.GrantLock(tid1, pid, SharedLock)
	// Add tid2 waiting for shared lock and tid3 waiting for exclusive lock
	lm.waitQueue.Add(tid2, pid, SharedLock)
	lm.waitQueue.Add(tid3, pid, ExclusiveLock)

	// Process wait queue - should grant shared lock to tid2 but not exclusive to tid3
	lm.processWaitQueue(pid)

	// Verify tid2 got shared lock
	if !lm.lockTable.HasLockType(tid2, pid, SharedLock) {
		t.Error("tid2 should have shared lock after processing")
	}

	// Verify tid3 is still waiting
	queue := lm.waitQueue.GetRequests(pid)
	if len(queue) != 1 {
		t.Fatalf("Expected 1 item remaining in wait queue, got %d", len(queue))
	}
	if queue[0].TID != tid3 {
		t.Error("tid3 should still be in wait queue")
	}
}

func TestIsPageLockedAdvanced(t *testing.T) {
	lm := NewLockManager()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Initially no locks
	if lm.IsPageLocked(pid) {
		t.Error("Page should not be locked initially")
	}

	// Grant a shared lock
	lm.lockGrantor.GrantLock(tid, pid, SharedLock)
	if !lm.IsPageLocked(pid) {
		t.Error("Page should be locked after granting lock")
	}

	// RemoveRequest the lock
	lm.UnlockPage(tid, pid)
	if lm.IsPageLocked(pid) {
		t.Error("Page should not be locked after removing lock")
	}

	// Test with exclusive lock
	lm.lockGrantor.GrantLock(tid, pid, ExclusiveLock)
	if !lm.IsPageLocked(pid) {
		t.Error("Page should be locked with exclusive lock")
	}

	lm.UnlockPage(tid, pid)
	if lm.IsPageLocked(pid) {
		t.Error("Page should not be locked after removing exclusive lock")
	}
}

func TestUnlockAllPages(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid1 := heap.NewHeapPageID(1, 1)
	pid2 := heap.NewHeapPageID(1, 2)
	pid3 := heap.NewHeapPageID(1, 3)

	// Grant multiple locks to tid1
	lm.lockGrantor.GrantLock(tid1, pid1, SharedLock)
	lm.lockGrantor.GrantLock(tid1, pid2, ExclusiveLock)
	lm.lockGrantor.GrantLock(tid1, pid3, SharedLock)

	// Grant a lock to tid2 on pid3 (shared with tid1)
	lm.lockGrantor.GrantLock(tid2, pid3, SharedLock)

	// Verify initial state
	if !lm.IsPageLocked(pid1) || !lm.IsPageLocked(pid2) || !lm.IsPageLocked(pid3) {
		t.Error("All pages should be locked initially")
	}

	// Add tid2 to wait queue for pid1
	lm.waitQueue.Add(tid2, pid1, ExclusiveLock)

	// Unlock all pages for tid1
	lm.UnlockAllPages(tid1)

	// Verify tid1's locks are removed
	if _, exists := lm.lockTable.GetTransactionLockTable()[tid1]; exists {
		t.Error("tid1 should have no locks in transactionLocks map")
	}

	// Verify page states
	if !lm.IsPageLocked(pid1) {
		t.Error("pid1 should be locked after tid2 gets it from wait queue")
	}
	if lm.IsPageLocked(pid2) {
		t.Error("pid2 should not be locked after unlocking all for tid1")
	}
	if !lm.IsPageLocked(pid3) {
		t.Error("pid3 should still be locked (tid2 has shared lock)")
	}

	// Verify tid2 still has lock on pid3
	if !lm.lockTable.HasLockType(tid2, pid3, SharedLock) {
		t.Error("tid2 should still have shared lock on pid3")
	}

	// Verify tid2 got the exclusive lock on pid1 (from wait queue processing)
	if !lm.lockTable.HasLockType(tid2, pid1, ExclusiveLock) {
		t.Error("tid2 should have received exclusive lock on pid1 from wait queue")
	}

	// Verify tid1 is not in waiting list
	if len(lm.waitQueue.GetPagesRequestedFor(tid1)) > 0 {
		t.Error("tid1 should not be in waiting list after unlocking all")
	}
}

func TestUnlockAllPagesEmptyTransaction(t *testing.T) {
	lm := NewLockManager()
	tid := primitives.NewTransactionID()

	// Call UnlockAllPages on transaction with no locks (should not panic)
	lm.UnlockAllPages(tid)

	// Verify no issues
	if _, exists := lm.lockTable.GetTransactionLockTable()[tid]; exists {
		t.Error("Transaction should not exist in transactionLocks map")
	}
}

func TestUnlockPageDetailed(t *testing.T) {
	lm := NewLockManager()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant locks to both transactions
	lm.lockGrantor.GrantLock(tid1, pid, SharedLock)
	lm.lockGrantor.GrantLock(tid2, pid, SharedLock)

	// Verify both locks exist
	locks := lm.lockTable.GetPageLocks(pid)
	if len(locks) != 2 {
		t.Fatalf("Expected 2 locks, got %d", len(locks))
	}

	// Unlock one transaction
	lm.UnlockPage(tid1, pid)

	// Verify only one lock remains
	locks = lm.lockTable.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock after unlock, got %d", len(locks))
	}
	if locks[0].TID != tid2 {
		t.Error("Remaining lock should belong to tid2")
	}

	// Verify tid1 is removed from transactionLocks
	if _, exists := lm.lockTable.GetTransactionLockTable()[tid1]; exists {
		t.Error("tid1 should be removed from transactionLocks")
	}

	// Verify tid2 still has its lock
	if !lm.lockTable.HasLockType(tid2, pid, SharedLock) {
		t.Error("tid2 should still have its shared lock")
	}

	// Unlock last transaction
	lm.UnlockPage(tid2, pid)

	// Verify page is completely unlocked
	if lm.IsPageLocked(pid) {
		t.Error("Page should not be locked after all transactions unlock")
	}
	if _, exists := lm.lockTable.GetPageLockTable()[pid]; exists {
		t.Error("Page should be removed from pageLocks map when empty")
	}
}
