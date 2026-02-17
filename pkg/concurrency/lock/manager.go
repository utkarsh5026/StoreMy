package lock

import (
	"fmt"
	"storemy/pkg/primitives"
	"sync"
	"time"
)

// LockManager manages page-level locks for database transactions.
// It provides deadlock detection, lock upgrade capabilities, and maintains
// transaction dependencies through a dependency graph.
type LockManager struct {
	depGraph    *DependencyGraph
	mutex       sync.RWMutex
	waitQueue   *WaitQueue
	lockTable   *LockTable
	lockGrantor *LockGrantor
}

// NewLockManager creates and initializes a new LockManager instance.
// All internal maps and the dependency graph are initialized as empty.
func NewLockManager() *LockManager {
	lockTable := NewLockTable()
	waitQueue := NewWaitQueue()
	depGraph := NewDependencyGraph()

	return &LockManager{
		depGraph:    depGraph,
		waitQueue:   waitQueue,
		lockTable:   lockTable,
		lockGrantor: NewLockGrantor(lockTable, waitQueue, depGraph),
	}
}

// LockPage attempts to acquire a lock on the specified page for the given primitives.
// The method handles lock upgrades, deadlock detection, and retry logic with exponential backoff.
//
// Parameters:
//   - tid: The transaction requesting the lock (must not be nil)
//   - pid: The page ID to lock
//   - exclusive: If true, requests an exclusive lock; otherwise requests a shared lock
//
// Returns:
//   - error: nil if lock acquired successfully, or error describing the failure
//
// Possible errors:
//   - Transaction ID is nil
//   - Deadlock detected
//   - Timeout waiting for lock (after 1000 attempts)
func (lm *LockManager) LockPage(tid *primitives.TransactionID, pid primitives.PageID, exclusive bool) error {
	if tid == nil {
		return fmt.Errorf("transaction ID cannot be nil")
	}

	lockType := SharedLock
	if exclusive {
		lockType = ExclusiveLock
	}

	lm.mutex.RLock()
	if lm.lockTable.HasSufficientLock(tid, pid, lockType) {
		lm.mutex.RUnlock()
		return nil
	}
	lm.mutex.RUnlock()

	return lm.attemptToAcquireLock(tid, pid, lockType)
}

// attemptToAcquireLock implements the main lock acquisition logic with retry and deadlock detection.
// Uses exponential backoff with a maximum retry limit to prevent infinite waiting.
//
// The algorithm:
// 1. Check if lock can be granted immediately
// 2. Handle lock upgrades (shared to exclusive)
// 3. Add to wait queue and update dependency graph
// 4. Check for deadlocks
// 5. Retry with exponential backoff
//
// Parameters:
//   - tid: The requesting transaction
//   - pid: The page to lock
//   - lockType: The type of lock requested
//
// Returns:
//   - error: nil on success, error on deadlock or timeout
func (lm *LockManager) attemptToAcquireLock(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) error {
	const maxRetryDelay = 50 * time.Millisecond
	maxRetries := 100
	retryDelay := time.Millisecond

	for attempt := range maxRetries {
		lm.mutex.Lock()

		if lm.lockTable.HasSufficientLock(tid, pid, lockType) {
			lm.mutex.Unlock()
			return nil
		}

		if lockType == ExclusiveLock && lm.lockTable.HasLockType(tid, pid, SharedLock) {
			if lm.lockGrantor.CanUpgradeLock(tid, pid) {
				lm.lockTable.UpgradeLock(tid, pid)
				lm.mutex.Unlock()
				return nil
			}
		}

		if lm.lockGrantor.CanGrantImmediately(tid, pid, lockType) {
			lm.lockGrantor.GrantLock(tid, pid, lockType)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return nil
		}

		if err := lm.waitQueue.Add(tid, pid, lockType); err != nil {
			lm.mutex.Unlock()
			return err
		}
		lm.updateDependencies(tid, pid, lockType)

		if lm.depGraph.HasCycle() {
			lm.waitQueue.RemoveRequest(tid, pid)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return fmt.Errorf("deadlock detected for transaction %d", tid.ID())
		}

		lm.mutex.Unlock()
		currentDelay := lm.calculateRetryDelay(attempt, retryDelay, maxRetryDelay)
		time.Sleep(currentDelay)
	}

	return fmt.Errorf("timeout waiting for lock on page %v", pid)
}

// updateDependencies updates the dependency graph based on lock conflicts.
// Creates edges from waiting transactions to lock holders when conflicts exist.
//
// Dependency rules:
// - Exclusive lock request creates dependency on all lock holders
// - Shared lock request creates dependency only on exclusive lock holders
func (lm *LockManager) updateDependencies(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) {
	locks := lm.lockTable.GetPageLocks(pid)

	for _, lock := range locks {
		if lock.TID == tid {
			continue
		}

		if lockType == ExclusiveLock || lock.LockType == ExclusiveLock {
			lm.depGraph.AddEdge(tid, lock.TID)
		}
	}
}

// calculateRetryDelay computes the delay for the next retry attempt using exponential backoff.
// The delay increases exponentially but is capped at maxDelay to prevent excessive waiting.
func (lm *LockManager) calculateRetryDelay(attemptNumber int, baseDelay, maxDelay time.Duration) time.Duration {
	// Use more gradual exponential backoff: every 5 attempts instead of 10
	// Cap the exponential factor at 5 to prevent excessive delays
	exponentialFactor := min(attemptNumber/5, 5)
	delay := min(baseDelay*time.Duration(1<<uint(exponentialFactor)), maxDelay) // #nosec G115

	return delay
}

// UnlockPage releases a specific lock held by a transaction on a page.
// Processes the wait queue after releasing the lock to grant pending requests.
func (lm *LockManager) UnlockPage(tid *primitives.TransactionID, pid primitives.PageID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	lm.lockTable.ReleaseLock(tid, pid)
	lm.depGraph.RemoveTransaction(tid)
	lm.processWaitQueue(pid)
}

// processWaitQueue processes pending lock requests for a page after a lock is released.
// Grants locks to waiting transactions in FIFO order when possible.
func (lm *LockManager) processWaitQueue(pid primitives.PageID) {
	requests := lm.waitQueue.GetRequests(pid)
	if len(requests) == 0 {
		return
	}
	grantedRequests := make([]*LockRequest, 0)
	for _, request := range requests {
		if lm.lockGrantor.CanGrantImmediately(request.TID, pid, request.LockType) {
			lm.lockGrantor.GrantLock(request.TID, pid, request.LockType)
			grantedRequests = append(grantedRequests, request)
			if request.Chan != nil {
				select {
				case request.Chan <- true:
				default:
					// Channel is full or closed, but we don't block
				}
			}
		}
	}

	for _, request := range grantedRequests {
		lm.waitQueue.RemoveRequest(request.TID, pid)
	}
}

// IsPageLocked checks if any locks are currently held on a page.
func (lm *LockManager) IsPageLocked(pid primitives.PageID) bool {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	return lm.lockTable.IsPageLocked(pid)
}

// UnlockAllPages releases all locks held by a primitives.
// This is typically called during transaction commit or abort.
// Processes wait queues for all affected pages after releasing locks.
func (lm *LockManager) UnlockAllPages(tid *primitives.TransactionID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	pagesToProcess := lm.lockTable.ReleaseAllLocks(tid)
	lm.depGraph.RemoveTransaction(tid)
	lm.waitQueue.RemoveAllForTransaction(tid)

	for _, pid := range pagesToProcess {
		lm.processWaitQueue(pid)
	}
}
