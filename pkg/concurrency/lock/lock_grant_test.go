package lock

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"testing"
)

func TestNewLockGrantor(t *testing.T) {
	lockTable := NewLockTable()
	waitQueue := NewWaitQueue()
	depGraph := NewDependencyGraph()

	lg := NewLockGrantor(lockTable, waitQueue, depGraph)

	if lg == nil {
		t.Fatal("NewLockGrantor returned nil")
	}

	if lg.lockTable != lockTable {
		t.Error("LockGrantor lockTable not set correctly")
	}

	if lg.waitQueue != waitQueue {
		t.Error("LockGrantor waitQueue not set correctly")
	}

	if lg.depGraph != depGraph {
		t.Error("LockGrantor depGraph not set correctly")
	}
}

func TestCanGrantImmediately_EmptyPage(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Empty page should allow any lock
	if !lg.CanGrantImmediately(tid, pid, SharedLock) {
		t.Error("Should grant shared lock on empty page")
	}

	if !lg.CanGrantImmediately(tid, pid, ExclusiveLock) {
		t.Error("Should grant exclusive lock on empty page")
	}
}

func TestCanGrantImmediately_SharedLockScenarios(t *testing.T) {
	lg := setupLockGrantor()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant a shared lock to tid1
	lg.lockTable.AddLock(tid1, pid, SharedLock)

	// tid2 should be able to get shared lock
	if !lg.CanGrantImmediately(tid2, pid, SharedLock) {
		t.Error("Should grant shared lock when another shared lock exists")
	}

	// tid2 should not be able to get exclusive lock
	if lg.CanGrantImmediately(tid2, pid, ExclusiveLock) {
		t.Error("Should not grant exclusive lock when shared lock exists")
	}

	// tid1 should be able to get exclusive lock (upgrade)
	if !lg.CanGrantImmediately(tid1, pid, ExclusiveLock) {
		t.Error("Should allow same transaction to upgrade to exclusive lock")
	}
}

func TestCanGrantImmediately_ExclusiveLockScenarios(t *testing.T) {
	lg := setupLockGrantor()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant an exclusive lock to tid1
	lg.lockTable.AddLock(tid1, pid, ExclusiveLock)

	// tid2 should not be able to get any lock
	if lg.CanGrantImmediately(tid2, pid, SharedLock) {
		t.Error("Should not grant shared lock when exclusive lock exists")
	}

	if lg.CanGrantImmediately(tid2, pid, ExclusiveLock) {
		t.Error("Should not grant exclusive lock when exclusive lock exists")
	}

	// tid1 should be able to get any lock (already owns exclusive)
	if !lg.CanGrantImmediately(tid1, pid, SharedLock) {
		t.Error("Should allow same transaction to get shared lock when holding exclusive")
	}

	if !lg.CanGrantImmediately(tid1, pid, ExclusiveLock) {
		t.Error("Should allow same transaction to get exclusive lock when holding exclusive")
	}
}

func TestCanGrantImmediately_MultipleSharedLocks(t *testing.T) {
	lg := setupLockGrantor()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant shared locks to tid1 and tid2
	lg.lockTable.AddLock(tid1, pid, SharedLock)
	lg.lockTable.AddLock(tid2, pid, SharedLock)

	// tid3 should be able to get shared lock
	if !lg.CanGrantImmediately(tid3, pid, SharedLock) {
		t.Error("Should grant shared lock when multiple shared locks exist")
	}

	// tid3 should not be able to get exclusive lock
	if lg.CanGrantImmediately(tid3, pid, ExclusiveLock) {
		t.Error("Should not grant exclusive lock when multiple shared locks exist")
	}

	// tid1 should not be able to upgrade (other shared locks exist)
	if lg.CanGrantImmediately(tid1, pid, ExclusiveLock) {
		t.Error("Should not allow upgrade when other shared locks exist")
	}
}

func TestGrantLock_BasicFunctionality(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant a shared lock
	lg.GrantLock(tid, pid, SharedLock)

	// Verify lock was added to lock table
	if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
		t.Error("Lock should be added to lock table")
	}

	// Verify transaction appears in transaction locks
	transactionLocks := lg.lockTable.GetTransactionLockTable()[tid]
	if transactionLocks == nil {
		t.Fatal("Transaction should appear in transaction locks")
	}

	if transactionLocks[pid] != SharedLock {
		t.Error("Correct lock type should be recorded for transaction")
	}

	// Verify page appears in page locks
	locks := lg.lockTable.GetPageLocks(pid)
	if len(locks) != 1 {
		t.Fatalf("Expected 1 lock for page, got %d", len(locks))
	}

	if locks[0].TID != tid || locks[0].LockType != SharedLock {
		t.Error("Correct lock should be recorded for page")
	}
}

func TestGrantLock_RemovesFromWaitQueue(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Add transaction to wait queue
	lg.waitQueue.Add(tid, pid, SharedLock)

	// Verify it's in wait queue
	requests := lg.waitQueue.GetRequests(pid)
	if len(requests) != 1 {
		t.Fatalf("Expected 1 request in wait queue, got %d", len(requests))
	}

	// Grant the lock
	lg.GrantLock(tid, pid, SharedLock)

	// Verify it's removed from wait queue
	requests = lg.waitQueue.GetRequests(pid)
	if len(requests) != 0 {
		t.Errorf("Expected 0 requests in wait queue after granting, got %d", len(requests))
	}

	// Verify transaction is removed from waiting tracking
	waitingPages := lg.waitQueue.GetPagesRequestedFor(tid)
	if len(waitingPages) != 0 {
		t.Errorf("Transaction should not be waiting for any pages after grant, got %d", len(waitingPages))
	}
}

func TestGrantLock_ExclusiveLock(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant an exclusive lock
	lg.GrantLock(tid, pid, ExclusiveLock)

	// Verify lock was added correctly
	if !lg.lockTable.HasLockType(tid, pid, ExclusiveLock) {
		t.Error("Exclusive lock should be added to lock table")
	}

	// Verify it has sufficient lock for shared access too
	if !lg.lockTable.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Exclusive lock should provide sufficient access for shared lock")
	}
}

func TestGrantLock_LockUpgrade(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// First grant shared lock
	lg.GrantLock(tid, pid, SharedLock)

	// Verify shared lock
	if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
		t.Error("Should have shared lock initially")
	}

	// Upgrade to exclusive lock (this will add a new lock, not upgrade the existing one)
	lg.GrantLock(tid, pid, ExclusiveLock)

	// Verify exclusive lock is granted
	if !lg.lockTable.HasLockType(tid, pid, ExclusiveLock) {
		t.Error("Should have exclusive lock after upgrade")
	}

	// Should still provide shared access
	if !lg.lockTable.HasSufficientLock(tid, pid, SharedLock) {
		t.Error("Exclusive lock should provide shared access")
	}

	// Note: The current implementation adds locks rather than upgrading them
	// This is actually handled properly by the LockTable.AddLock method
	locks := lg.lockTable.GetPageLocks(pid)
	if len(locks) == 0 {
		t.Error("Should have at least one lock after granting")
	}

	// Verify the transaction has the correct lock type recorded
	transactionLocks := lg.lockTable.GetTransactionLockTable()[tid]
	if transactionLocks[pid] != ExclusiveLock {
		t.Error("Transaction should have exclusive lock type recorded")
	}
}

func TestCanUpgradeLock_ValidUpgrade(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant shared lock
	lg.lockTable.AddLock(tid, pid, SharedLock)

	// Should be able to upgrade when holding only lock
	if !lg.CanUpgradeLock(tid, pid) {
		t.Error("Should be able to upgrade when holding only lock on page")
	}
}

func TestCanUpgradeLock_NoLock(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Should not be able to upgrade without holding a lock
	if lg.CanUpgradeLock(tid, pid) {
		t.Error("Should not be able to upgrade without holding a lock")
	}
}

func TestCanUpgradeLock_ExclusiveLock(t *testing.T) {
	lg := setupLockGrantor()
	tid := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant exclusive lock
	lg.lockTable.AddLock(tid, pid, ExclusiveLock)

	// Should not be able to upgrade exclusive lock (already highest)
	if lg.CanUpgradeLock(tid, pid) {
		t.Error("Should not be able to upgrade exclusive lock")
	}
}

func TestCanUpgradeLock_OtherSharedLocks(t *testing.T) {
	lg := setupLockGrantor()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant shared locks to both transactions
	lg.lockTable.AddLock(tid1, pid, SharedLock)
	lg.lockTable.AddLock(tid2, pid, SharedLock)

	// tid1 should not be able to upgrade (tid2 holds shared lock)
	if lg.CanUpgradeLock(tid1, pid) {
		t.Error("Should not be able to upgrade when other transactions hold shared locks")
	}

	// tid2 should not be able to upgrade either
	if lg.CanUpgradeLock(tid2, pid) {
		t.Error("Should not be able to upgrade when other transactions hold shared locks")
	}
}

func TestCanUpgradeLock_AfterOtherRelease(t *testing.T) {
	lg := setupLockGrantor()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := heap.NewHeapPageID(1, 1)

	// Grant shared locks to both transactions
	lg.lockTable.AddLock(tid1, pid, SharedLock)
	lg.lockTable.AddLock(tid2, pid, SharedLock)

	// Neither should be able to upgrade initially
	if lg.CanUpgradeLock(tid1, pid) {
		t.Error("Should not be able to upgrade with other shared locks present")
	}

	// Release tid2's lock
	lg.lockTable.ReleaseLock(tid2, pid)

	// Now tid1 should be able to upgrade
	if !lg.CanUpgradeLock(tid1, pid) {
		t.Error("Should be able to upgrade after other shared locks are released")
	}
}

func TestSequentialGranting(t *testing.T) {
	lg := setupLockGrantor()
	pid := heap.NewHeapPageID(1, 1)
	numTransactions := 10

	var tids []*primitives.TransactionID

	// Grant shared locks to multiple transactions sequentially
	for i := 0; i < numTransactions; i++ {
		tid := primitives.NewTransactionID()
		tids = append(tids, tid)
		lg.GrantLock(tid, pid, SharedLock)
	}

	// Verify all locks are in the lock table
	locks := lg.lockTable.GetPageLocks(pid)
	if len(locks) != numTransactions {
		t.Errorf("Expected %d locks in lock table, got %d", numTransactions, len(locks))
	}

	// Verify all are shared locks
	for _, lock := range locks {
		if lock.LockType != SharedLock {
			t.Error("All locks should be shared locks")
		}
	}

	// Verify each transaction has its lock recorded
	for _, tid := range tids {
		if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
			t.Errorf("Transaction %v should have shared lock", tid.ID())
		}
	}
}

func TestComplexScenarios(t *testing.T) {
	t.Run("GrantAfterWaiting", func(t *testing.T) {
		lg := setupLockGrantor()
		tid1 := primitives.NewTransactionID()
		tid2 := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// tid1 gets exclusive lock
		lg.GrantLock(tid1, pid, ExclusiveLock)

		// tid2 wants shared lock but can't get it immediately
		if lg.CanGrantImmediately(tid2, pid, SharedLock) {
			t.Error("Should not be able to grant shared lock when exclusive exists")
		}

		// Add tid2 to wait queue
		lg.waitQueue.Add(tid2, pid, SharedLock)

		// Release tid1's lock
		lg.lockTable.ReleaseLock(tid1, pid)

		// Now tid2 should be able to get the lock
		if !lg.CanGrantImmediately(tid2, pid, SharedLock) {
			t.Error("Should be able to grant shared lock after exclusive is released")
		}

		// Grant the lock to tid2
		lg.GrantLock(tid2, pid, SharedLock)

		// Verify tid2 has the lock and is not in wait queue
		if !lg.lockTable.HasLockType(tid2, pid, SharedLock) {
			t.Error("tid2 should have shared lock")
		}

		if len(lg.waitQueue.GetRequests(pid)) != 0 {
			t.Error("Wait queue should be empty after granting")
		}
	})

	t.Run("MultipleUpgrades", func(t *testing.T) {
		lg := setupLockGrantor()
		tid := primitives.NewTransactionID()
		pid1 := heap.NewHeapPageID(1, 1)
		pid2 := heap.NewHeapPageID(1, 2)

		// Grant shared locks on multiple pages
		lg.GrantLock(tid, pid1, SharedLock)
		lg.GrantLock(tid, pid2, SharedLock)

		// Should be able to upgrade both (no other transactions)
		if !lg.CanUpgradeLock(tid, pid1) {
			t.Error("Should be able to upgrade lock on pid1")
		}

		if !lg.CanUpgradeLock(tid, pid2) {
			t.Error("Should be able to upgrade lock on pid2")
		}

		// Upgrade both
		lg.GrantLock(tid, pid1, ExclusiveLock)
		lg.GrantLock(tid, pid2, ExclusiveLock)

		// Verify upgrades
		if !lg.lockTable.HasLockType(tid, pid1, ExclusiveLock) {
			t.Error("Should have exclusive lock on pid1")
		}

		if !lg.lockTable.HasLockType(tid, pid2, ExclusiveLock) {
			t.Error("Should have exclusive lock on pid2")
		}
	})

	t.Run("WaitQueueInteraction", func(t *testing.T) {
		lg := setupLockGrantor()
		tid1 := primitives.NewTransactionID()
		tid2 := primitives.NewTransactionID()
		tid3 := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// Add transactions to wait queue for different lock types
		lg.waitQueue.Add(tid1, pid, SharedLock)
		lg.waitQueue.Add(tid2, pid, SharedLock)
		lg.waitQueue.Add(tid3, pid, ExclusiveLock)

		// Grant lock to tid1
		lg.GrantLock(tid1, pid, SharedLock)

		// Verify tid1 is removed from wait queue but others remain
		requests := lg.waitQueue.GetRequests(pid)
		if len(requests) != 2 {
			t.Errorf("Expected 2 requests in wait queue, got %d", len(requests))
		}

		// Verify tid1 is not in waiting tracking
		if len(lg.waitQueue.GetPagesRequestedFor(tid1)) != 0 {
			t.Error("tid1 should not be in waiting tracking after grant")
		}

		// Verify tid2 and tid3 are still waiting
		if len(lg.waitQueue.GetPagesRequestedFor(tid2)) != 1 {
			t.Error("tid2 should still be waiting")
		}

		if len(lg.waitQueue.GetPagesRequestedFor(tid3)) != 1 {
			t.Error("tid3 should still be waiting")
		}
	})
}

func TestLockGrantorEdgeCases(t *testing.T) {
	t.Run("GrantWithoutWaitQueueEntry", func(t *testing.T) {
		lg := setupLockGrantor()
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// Grant lock without adding to wait queue first (should not error)
		lg.GrantLock(tid, pid, SharedLock)

		// Verify lock was granted
		if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
			t.Error("Lock should be granted even without wait queue entry")
		}
	})

	t.Run("CanUpgradeWithEmptyPage", func(t *testing.T) {
		lg := setupLockGrantor()
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// Should not be able to upgrade on empty page
		if lg.CanUpgradeLock(tid, pid) {
			t.Error("Should not be able to upgrade on empty page")
		}
	})

	t.Run("CanGrantWithSameTransactionMultipleLocks", func(t *testing.T) {
		lg := setupLockGrantor()
		tid := primitives.NewTransactionID()
		pid := heap.NewHeapPageID(1, 1)

		// Grant shared lock
		lg.GrantLock(tid, pid, SharedLock)

		// Same transaction should be able to get exclusive lock
		if !lg.CanGrantImmediately(tid, pid, ExclusiveLock) {
			t.Error("Same transaction should be able to get exclusive lock")
		}

		// Same transaction should be able to get shared lock
		if !lg.CanGrantImmediately(tid, pid, SharedLock) {
			t.Error("Same transaction should be able to get shared lock")
		}
	})
}

// Helper function to create a LockGrantor with initialized components
func setupLockGrantor() *LockGrantor {
	lockTable := NewLockTable()
	waitQueue := NewWaitQueue()
	depGraph := NewDependencyGraph()
	return NewLockGrantor(lockTable, waitQueue, depGraph)
}
