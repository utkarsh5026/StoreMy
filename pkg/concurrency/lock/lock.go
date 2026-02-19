package lock

import (
	"fmt"
	"slices"
	"storemy/pkg/primitives"
	"sync"
	"time"
)

type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

type Lock struct {
	TID       *primitives.TransactionID
	LockType  LockType
	GrantTime time.Time
}

type LockRequest struct {
	TID      *primitives.TransactionID
	LockType LockType
	Chan     chan bool // Channel to signal when lock is granted
}

func NewLock(tid *primitives.TransactionID, lockType LockType) *Lock {
	return &Lock{
		TID:       tid,
		LockType:  lockType,
		GrantTime: time.Now(),
	}
}

func NewLockRequest(tid *primitives.TransactionID, lockType LockType) *LockRequest {
	return &LockRequest{
		TID:      tid,
		LockType: lockType,
		Chan:     make(chan bool, 1),
	}
}

type LockGrantor struct {
	lockTable *LockTable
	waitQueue *WaitQueue
	depGraph  *DependencyGraph
}

// NewLockGrantor creates a new lock grantor.
func NewLockGrantor(l *LockTable, w *WaitQueue, d *DependencyGraph) *LockGrantor {
	return &LockGrantor{
		lockTable: l,
		waitQueue: w,
		depGraph:  d,
	}
}

// CanGrantImmediately determines if a lock can be granted without waiting.
// For exclusive locks, no other transaction can hold any lock on the page.
// For shared locks, no other transaction can hold an exclusive lock on the page.
func (lg *LockGrantor) CanGrantImmediately(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) bool {
	locks := lg.lockTable.GetPageLocks(pid)
	if len(locks) == 0 {
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

	return !slices.ContainsFunc(locks, func(l *Lock) bool {
		return l.TID != tid && l.LockType == ExclusiveLock
	})
}

// GrantLock grants a lock to a transaction and removes its wait queue entry.
func (lg *LockGrantor) GrantLock(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) {
	lg.lockTable.AddLock(tid, pid, lockType)
	lg.waitQueue.RemoveRequest(tid, pid)
}

// CanUpgradeLock checks if a lock can be upgraded from shared to exclusive.
// A lock can only be upgraded if the transaction holds a shared lock and
// no other transactions hold any locks on the page.
func (lg *LockGrantor) CanUpgradeLock(tid *primitives.TransactionID, pid primitives.PageID) bool {
	if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
		return false
	}

	locks := lg.lockTable.GetPageLocks(pid)
	return !slices.ContainsFunc(locks, func(l *Lock) bool {
		return l.TID != tid
	})
}

// WaitQueue implements a two-way mapping system for managing lock request queues in a database system.
//
// The WaitQueue maintains two critical data structures:
//
//   - pageWaitQueue: A map from PageID to an ordered slice of LockRequest pointers, representing
//     the FIFO queue of transactions waiting to acquire locks on each page. The order matters
//     as it determines which transaction gets the lock next when it becomes available.
//
//   - transactionWaiting: A reverse index mapping each TransactionID to all PageIDs it's currently
//     waiting for. This enables efficient transaction cleanup and deadlock cycle detection.
type WaitQueue struct {
	pageWaitQueue      map[primitives.PageID][]*LockRequest
	transactionWaiting map[*primitives.TransactionID][]primitives.PageID
}

// NewWaitQueue creates and initializes a new WaitQueue instance with empty internal maps.
func NewWaitQueue() *WaitQueue {
	return &WaitQueue{
		pageWaitQueue:      make(map[primitives.PageID][]*LockRequest),
		transactionWaiting: make(map[*primitives.TransactionID][]primitives.PageID),
	}
}

// Add enqueues a transaction's lock request for a specific page, maintaining FIFO ordering.
//
// The method updates both internal data structures atomically (from the caller's perspective):
// 1. Adds the lock request to the end of the page's wait queue (FIFO)
// 2. Records that this transaction is now waiting for this page
func (wq *WaitQueue) Add(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) error {
	if wq.alreadyInPageQueue(tid, pid) {
		return fmt.Errorf("already in the Page queue")
	}

	if wq.isInTransactionQueue(tid, pid) {
		return fmt.Errorf("already in transaction queue")
	}

	request := NewLockRequest(tid, lockType)
	wq.pageWaitQueue[pid] = append(wq.pageWaitQueue[pid], request)
	wq.transactionWaiting[tid] = append(wq.transactionWaiting[tid], pid)
	return nil
}

// RemoveRequest atomically removes a single lock request for a specific (transaction, page) pair
// from both internal data structures. This method is typically called when:
//
// 1. A transaction successfully acquires the lock it was waiting for
// 2. A specific lock request times out
// 3. A transaction voluntarily cancels a specific lock request
func (wq *WaitQueue) RemoveRequest(tid *primitives.TransactionID, pid primitives.PageID) {
	wq.removeFromPageQueue(tid, pid)
	wq.removeFromTransactionQueue(tid, pid)
}

// RemoveAllForTransaction completely removes a transaction from all wait queues it's participating in.
//
// This is a comprehensive cleanup operation typically performed during:
// 1. Transaction abort - release all pending lock requests
// 2. Transaction commit - clean up any remaining requests (shouldn't happen in normal flow)
// 3. Deadlock resolution - victim transaction needs complete cleanup
// 4. Transaction timeout - remove all pending requests
func (wq *WaitQueue) RemoveAllForTransaction(tid *primitives.TransactionID) {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return
	}

	for _, pid := range waitingPages {
		wq.RemoveRequest(tid, pid)
	}
}

// GetRequests returns an ordered slice of all lock requests waiting for the specified page.
func (wq *WaitQueue) GetRequests(pid primitives.PageID) []*LockRequest {
	return slices.Clone(wq.pageWaitQueue[pid])
}

// GetPagesRequestedFor returns all pages that a specific transaction is currently waiting
// to acquire locks on.
func (wq *WaitQueue) GetPagesRequestedFor(tid *primitives.TransactionID) []primitives.PageID {
	return slices.Clone(wq.transactionWaiting[tid])
}

// removeFromPageQueue removes a specific transaction from a page's wait queue while preserving
// FIFO ordering for remaining requests.
func (wq *WaitQueue) removeFromPageQueue(tid *primitives.TransactionID, pid primitives.PageID) {
	requestQueue, exists := wq.pageWaitQueue[pid]
	if !exists {
		return
	}

	newQueue := slices.DeleteFunc(slices.Clone(requestQueue), func(req *LockRequest) bool {
		return req.TID == tid
	})
	updateOrDelete(wq.pageWaitQueue, pid, newQueue)
}

// removeFromTransactionQueue removes a specific page from a transaction's waiting list,
// which is the reverse-index side of the queue removal operation. This method maintains
// the consistency of the transactionWaiting map by either updating the page list or
// removing the transaction entry entirely if it's no longer waiting for any pages.
func (wq *WaitQueue) removeFromTransactionQueue(tid *primitives.TransactionID, pid primitives.PageID) {
	pages, exists := wq.transactionWaiting[tid]
	if !exists {
		return
	}

	updated := slices.DeleteFunc(slices.Clone(pages), pid.Equals)
	updateOrDelete(wq.transactionWaiting, tid, updated)
}

// alreadyInPageQueue checks if a transaction already has a pending lock request in the
// specified page's wait queue.
func (wq *WaitQueue) alreadyInPageQueue(tid *primitives.TransactionID, pid primitives.PageID) bool {
	queue, exists := wq.pageWaitQueue[pid]
	if !exists {
		return false
	}

	return slices.ContainsFunc(queue, func(req *LockRequest) bool {
		return req.TID == tid
	})
}

// isInTransactionQueue checks if a transaction is already recorded as waiting for the
// specified page in the reverse index.
func (wq *WaitQueue) isInTransactionQueue(tid *primitives.TransactionID, pid primitives.PageID) bool {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return false
	}

	return slices.ContainsFunc(waitingPages, pid.Equals)
}

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
	addedToWaitQueue := false

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

		if !addedToWaitQueue {
			if err := lm.waitQueue.Add(tid, pid, lockType); err != nil {
				lm.mutex.Unlock()
				return err
			}
			lm.updateDependencies(tid, pid, lockType)
			addedToWaitQueue = true
		}

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

	lm.mutex.Lock()
	lm.waitQueue.RemoveRequest(tid, pid)
	lm.depGraph.RemoveTransaction(tid)
	lm.mutex.Unlock()

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
