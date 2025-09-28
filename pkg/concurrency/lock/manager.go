package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

// LockManager manages page-level locks for database transactions.
// It provides deadlock detection, lock upgrade capabilities, and maintains
// transaction dependencies through a dependency graph.
type LockManager struct {
	// pageLocks maps each page ID to the list of locks currently held on that page
	pageLocks map[tuple.PageID][]*Lock

	// transactionLocks maps each transaction to the pages it has locked and their lock types
	transactionLocks map[*transaction.TransactionID]map[tuple.PageID]LockType

	// waitingFor maps each transaction to the pages it's currently waiting to lock
	waitingFor map[*transaction.TransactionID][]tuple.PageID

	// depGraph maintains the dependency relationships between transactions for deadlock detection
	depGraph *DependencyGraph

	// mutex provides thread-safe access to all internal data structures
	mutex sync.RWMutex

	// waitQueue maps each page to the queue of transactions waiting to acquire locks on it
	waitQueue map[tuple.PageID][]*LockRequest
}

// NewLockManager creates and initializes a new LockManager instance.
// All internal maps and the dependency graph are initialized as empty.
func NewLockManager() *LockManager {
	return &LockManager{
		pageLocks:        make(map[tuple.PageID][]*Lock),
		transactionLocks: make(map[*transaction.TransactionID]map[tuple.PageID]LockType),
		waitingFor:       make(map[*transaction.TransactionID][]tuple.PageID),
		depGraph:         NewDependencyGraph(),
		waitQueue:        make(map[tuple.PageID][]*LockRequest),
	}
}

// LockPage attempts to acquire a lock on the specified page for the given transaction.
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
func (lm *LockManager) LockPage(tid *transaction.TransactionID, pid tuple.PageID, exclusive bool) error {
	if tid == nil {
		return fmt.Errorf("transaction ID cannot be nil")
	}

	lockType := SharedLock
	if exclusive {
		lockType = ExclusiveLock
	}

	lm.mutex.RLock()
	if lm.alreadyHasLock(tid, pid, lockType) {
		lm.mutex.RUnlock()
		return nil
	}
	lm.mutex.RUnlock()

	return lm.attemptToAcquireLock(tid, pid, lockType)
}

// alreadyHasLock checks if the transaction already holds a sufficient lock on the page.
// A transaction has a sufficient lock if:
// - It holds an exclusive lock (covers all cases)
// - It holds a shared lock and only needs a shared lock
//
// Parameters:
//   - tid: The transaction to check
//   - pageID: The page to check
//   - reqLockType: The type of lock being requested
//
// Returns:
//   - bool: true if the transaction already has a sufficient lock
func (lm *LockManager) alreadyHasLock(tid *transaction.TransactionID, pageID tuple.PageID, reqLockType LockType) bool {
	transactionPages, transactionExists := lm.transactionLocks[tid]
	if !transactionExists {
		return false
	}

	currentLockType, pageIsLocked := transactionPages[pageID]
	if !pageIsLocked {
		return false
	}

	// Exclusive lock covers everything
	if currentLockType == ExclusiveLock {
		return true
	}

	return currentLockType == SharedLock && reqLockType == SharedLock
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
func (lm *LockManager) attemptToAcquireLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) error {
	const maxRetryDelay = 100 * time.Millisecond
	maxRetries := 1000
	retryDelay := time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		lm.mutex.Lock()

		// Double-check after acquiring lock
		if lm.alreadyHasLock(tid, pid, lockType) {
			lm.mutex.Unlock()
			return nil
		}

		// Handle lock upgrade (shared -> exclusive)
		if lockType == ExclusiveLock && lm.transactionHoldsLockType(tid, pid, SharedLock) {
			if lm.canUpgradeLock(tid, pid) {
				lm.upgradeLock(tid, pid)
				lm.mutex.Unlock()
				return nil
			}
		}

		// Try to grant lock immediately
		if lm.canGrantLockImmediately(tid, pid, lockType) {
			lm.grantLock(tid, pid, lockType)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return nil
		}

		// Add to wait queue and check for deadlocks
		lm.addToWaitQueue(tid, pid, lockType)
		lm.updateDependencies(tid, pid, lockType)

		if lm.depGraph.HasCycle() {
			lm.removeFromWaitQueue(tid, pid)
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

// transactionHoldsLockType checks if a transaction holds a specific type of lock on a page.
//
// Parameters:
//   - tid: The transaction to check
//   - pid: The page to check
//   - lockType: The specific lock type to look for
//
// Returns:
//   - bool: true if the transaction holds the specified lock type on the page
func (lm *LockManager) transactionHoldsLockType(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	if txPages, exists := lm.transactionLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid]; hasPage {
			return currentLock == lockType
		}
	}
	return false
}

// canGrantLockImmediately determines if a lock can be granted without waiting.
//
// Lock compatibility rules:
// - Multiple shared locks can coexist
// - Exclusive locks cannot coexist with any other locks
// - A transaction can always upgrade its own locks
//
// Parameters:
//   - tid: The requesting transaction
//   - pid: The page to lock
//   - lockType: The type of lock requested
//
// Returns:
//   - bool: true if the lock can be granted immediately
func (lm *LockManager) canGrantLockImmediately(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	locks, exists := lm.pageLocks[pid]
	if !exists || len(locks) == 0 {
		return true
	}

	if lockType == ExclusiveLock {
		for _, lock := range locks {
			if lock.TID != tid {
				return false
			}
		}
		return true
	}

	for _, lock := range locks {
		if lock.TID != tid && lock.LockType == ExclusiveLock {
			return false
		}
	}
	return true
}

// grantLock grants a lock to a transaction and updates all relevant data structures.
//
// Parameters:
//   - tid: The transaction to grant the lock to
//   - pid: The page being locked
//   - lockType: The type of lock being granted
func (lm *LockManager) grantLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	lock := NewLock(tid, lockType)
	lm.pageLocks[pid] = append(lm.pageLocks[pid], lock)

	if lm.transactionLocks[tid] == nil {
		lm.transactionLocks[tid] = make(map[tuple.PageID]LockType)
	}
	lm.transactionLocks[tid][pid] = lockType
	delete(lm.waitingFor, tid)
}

// canUpgradeLock checks if a lock can be upgraded from shared to exclusive.
// A lock can be upgraded only if the transaction is the sole holder of locks on the page.
//
// Parameters:
//   - tid: The transaction attempting the upgrade
//   - pid: The page whose lock is being upgraded
//
// Returns:
//   - bool: true if the lock can be upgraded
func (lm *LockManager) canUpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) bool {
	locks := lm.pageLocks[pid]
	for _, lock := range locks {
		if lock.TID != tid {
			return false
		}
	}
	return true
}

// upgradeLock upgrades a shared lock to an exclusive lock for the given transaction.
// Assumes canUpgradeLock has already been checked.
//
// Parameters:
//   - tid: The transaction upgrading the lock
//   - pid: The page whose lock is being upgraded
func (lm *LockManager) upgradeLock(tid *transaction.TransactionID, pid tuple.PageID) {
	for _, lock := range lm.pageLocks[pid] {
		if lock.TID == tid {
			lock.LockType = ExclusiveLock
			break
		}
	}

	lm.transactionLocks[tid][pid] = ExclusiveLock
}

// addToWaitQueue adds a transaction to the wait queue for a page.
// Prevents duplicate entries in both the wait queue and waiting list.
//
// Parameters:
//   - tid: The transaction to add to the wait queue
//   - pid: The page the transaction is waiting for
//   - lockType: The type of lock being requested
func (lm *LockManager) addToWaitQueue(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	if queue, exists := lm.waitQueue[pid]; exists {
		for _, req := range queue {
			if req.TID == tid {
				return
			}
		}
	}

	if waitingPages, exists := lm.waitingFor[tid]; exists {
		for _, waitingPid := range waitingPages {
			if pid.Equals(waitingPid) {
				return
			}
		}
	}

	request := NewLockRequest(tid, lockType)
	lm.waitQueue[pid] = append(lm.waitQueue[pid], request)
	lm.waitingFor[tid] = append(lm.waitingFor[tid], pid)
}

// removeFromWaitQueue removes a transaction from the wait queue for a specific page.
// Cleans up both the wait queue and the transaction's waiting list.
//
// Parameters:
//   - tid: The transaction to remove
//   - pid: The page to remove the transaction from
func (lm *LockManager) removeFromWaitQueue(tid *transaction.TransactionID, pid tuple.PageID) {
	if queue, exists := lm.waitQueue[pid]; exists {
		newQueue := make([]*LockRequest, 0)
		for _, req := range queue {
			if req.TID != tid {
				newQueue = append(newQueue, req)
			}
		}
		if len(newQueue) > 0 {
			lm.waitQueue[pid] = newQueue
		} else {
			delete(lm.waitQueue, pid)
		}
	}

	if waitingPages, exists := lm.waitingFor[tid]; exists {
		newWaitingPages := make([]tuple.PageID, 0)
		for _, waitingPid := range waitingPages {
			if !pid.Equals(waitingPid) {
				newWaitingPages = append(newWaitingPages, waitingPid)
			}
		}
		if len(newWaitingPages) > 0 {
			lm.waitingFor[tid] = newWaitingPages
		} else {
			delete(lm.waitingFor, tid)
		}
	}
}

// updateDependencies updates the dependency graph based on lock conflicts.
// Creates edges from waiting transactions to lock holders when conflicts exist.
//
// Dependency rules:
// - Exclusive lock request creates dependency on all lock holders
// - Shared lock request creates dependency only on exclusive lock holders
//
// Parameters:
//   - tid: The waiting transaction
//   - pid: The page being requested
//   - lockType: The type of lock being requested
func (lm *LockManager) updateDependencies(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	locks := lm.pageLocks[pid]

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
//
// Parameters:
//   - attemptNumber: The current attempt number (0-based)
//   - baseDelay: The base delay for the first retry
//   - maxDelay: The maximum allowed delay
//
// Returns:
//   - time.Duration: The calculated delay for this attempt
func (lm *LockManager) calculateRetryDelay(attemptNumber int, baseDelay, maxDelay time.Duration) time.Duration {
	exponentialFactor := min(attemptNumber/10, 10)
	delay := baseDelay * time.Duration(1<<uint(exponentialFactor))

	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// UnlockPage releases a specific lock held by a transaction on a page.
// Processes the wait queue after releasing the lock to grant pending requests.
//
// Parameters:
//   - tid: The transaction releasing the lock
//   - pid: The page to unlock
func (lm *LockManager) UnlockPage(tid *transaction.TransactionID, pid tuple.PageID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Remove lock from page's lock list
	if locks, exists := lm.pageLocks[pid]; exists {
		newLocks := make([]*Lock, 0)
		for _, lock := range locks {
			if lock.TID != tid {
				newLocks = append(newLocks, lock)
			}
		}
		if len(newLocks) > 0 {
			lm.pageLocks[pid] = newLocks
		} else {
			delete(lm.pageLocks, pid)
		}
	}

	// Remove page from transaction's lock map
	if txPages, exists := lm.transactionLocks[tid]; exists {
		delete(txPages, pid)
		if len(txPages) == 0 {
			delete(lm.transactionLocks, tid)
		}
	}

	lm.depGraph.RemoveTransaction(tid)
	lm.processWaitQueue(pid)
}

// processWaitQueue processes pending lock requests for a page after a lock is released.
// Grants locks to waiting transactions in FIFO order when possible.
//
// Parameters:
//   - pid: The page whose wait queue should be processed
func (lm *LockManager) processWaitQueue(pid tuple.PageID) {
	queue, exists := lm.waitQueue[pid]
	if !exists || len(queue) == 0 {
		return
	}

	remaining := make([]*LockRequest, 0)
	for _, request := range queue {
		if lm.canGrantLockImmediately(request.TID, pid, request.LockType) {
			lm.grantLock(request.TID, pid, request.LockType)
			// Signal waiting transaction if channel exists
			if request.Chan != nil {
				select {
				case request.Chan <- true:
				default:
					// Channel is full or closed, but we don't block
				}
			}
		} else {
			remaining = append(remaining, request)
		}
	}

	if len(remaining) > 0 {
		lm.waitQueue[pid] = remaining
	} else {
		delete(lm.waitQueue, pid)
	}
}

// IsPageLocked checks if any locks are currently held on a page.
//
// Parameters:
//   - pid: The page to check
//
// Returns:
//   - bool: true if the page has any locks, false otherwise
func (lm *LockManager) IsPageLocked(pid tuple.PageID) bool {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	locks, exists := lm.pageLocks[pid]
	return exists && len(locks) > 0
}

// UnlockAllPages releases all locks held by a transaction.
// This is typically called during transaction commit or abort.
// Processes wait queues for all affected pages after releasing locks.
//
// Parameters:
//   - tid: The transaction whose locks should be released
func (lm *LockManager) UnlockAllPages(tid *transaction.TransactionID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	pagesToProcess := lm.releaseAllLocks(tid)
	delete(lm.transactionLocks, tid)
	lm.depGraph.RemoveTransaction(tid)
	delete(lm.waitingFor, tid)

	for _, pid := range pagesToProcess {
		lm.processWaitQueue(pid)
	}
}

// releaseAllLocks releases all locks held by transaction and returns affected pages.
func (lm *LockManager) releaseAllLocks(tid *transaction.TransactionID) []tuple.PageID {
	txPages, exists := lm.transactionLocks[tid]
	if !exists {
		return nil
	}

	pagesToProcess := make([]tuple.PageID, 0, len(txPages))
	for pid := range txPages {
		pagesToProcess = append(pagesToProcess, pid)
	}

	for _, pid := range pagesToProcess {
		if locks, exists := lm.pageLocks[pid]; exists {
			newLocks := make([]*Lock, 0, len(locks))
			for _, lock := range locks {
				if lock.TID != tid {
					newLocks = append(newLocks, lock)
				}
			}

			if len(newLocks) > 0 {
				lm.pageLocks[pid] = newLocks
			} else {
				delete(lm.pageLocks, pid)
			}
		}
	}

	delete(lm.transactionLocks, tid)
	return pagesToProcess
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
