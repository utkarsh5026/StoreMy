package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

type LockManager struct {
	pageLocks        map[tuple.PageID][]*Lock                                 // Page -> list of locks
	transactionLocks map[*transaction.TransactionID]map[tuple.PageID]LockType // Transaction -> pages it has locked
	waitingFor       map[*transaction.TransactionID][]tuple.PageID            // Transaction -> pages it's waiting for
	depGraph         *DependencyGraph
	mutex            sync.RWMutex
	waitQueue        map[tuple.PageID][]*LockRequest // Page -> waiting transactions
}

func NewLockManager() *LockManager {
	return &LockManager{
		pageLocks:        make(map[tuple.PageID][]*Lock),
		transactionLocks: make(map[*transaction.TransactionID]map[tuple.PageID]LockType),
		waitingFor:       make(map[*transaction.TransactionID][]tuple.PageID),
		depGraph:         NewDependencyGraph(),
		waitQueue:        make(map[tuple.PageID][]*LockRequest),
	}
}

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

func (lm *LockManager) alreadyHasLock(tid *transaction.TransactionID, pageID tuple.PageID, reqLockType LockType) bool {
	transactionPages, transactionExists := lm.transactionLocks[tid]
	if !transactionExists {
		return false
	}

	currentLockType, pageIsLocked := transactionPages[pageID]
	if !pageIsLocked {
		return false
	}

	if currentLockType == ExclusiveLock {
		return true
	}

	return currentLockType == SharedLock && reqLockType == SharedLock
}

func (lm *LockManager) attemptToAcquireLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) error {
	const maxRetryDelay = 100 * time.Millisecond
	maxRetries := 1000
	retryDelay := time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		lm.mutex.Lock()

		if lm.alreadyHasLock(tid, pid, lockType) {
			lm.mutex.Unlock()
			return nil
		}

		if lockType == ExclusiveLock && lm.transactionHoldsLockType(tid, pid, SharedLock) {
			if lm.canUpgradeLock(tid, pid) {
				lm.upgradeLock(tid, pid)
				lm.mutex.Unlock()
				return nil
			}
		}

		if lm.canGrantLockImmediately(tid, pid, lockType) {
			lm.grantLock(tid, pid, lockType)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return nil
		}

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

func (lm *LockManager) transactionHoldsLockType(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	if txPages, exists := lm.transactionLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid]; hasPage {
			return currentLock == lockType
		}
	}
	return false
}

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

func (lm *LockManager) grantLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	lock := NewLock(tid, lockType)
	lm.pageLocks[pid] = append(lm.pageLocks[pid], lock)

	if lm.transactionLocks[tid] == nil {
		lm.transactionLocks[tid] = make(map[tuple.PageID]LockType)
	}
	lm.transactionLocks[tid][pid] = lockType
	delete(lm.waitingFor, tid)
}

func (lm *LockManager) canUpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) bool {
	locks := lm.pageLocks[pid]
	for _, lock := range locks {
		if lock.TID != tid {
			return false
		}
	}
	return true
}

func (lm *LockManager) upgradeLock(tid *transaction.TransactionID, pid tuple.PageID) {
	for _, lock := range lm.pageLocks[pid] {
		if lock.TID == tid {
			lock.LockType = ExclusiveLock
			break
		}
	}

	lm.transactionLocks[tid][pid] = ExclusiveLock
}

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

func (lm *LockManager) calculateRetryDelay(attemptNumber int, baseDelay, maxDelay time.Duration) time.Duration {
	exponentialFactor := min(attemptNumber/10, 10)
	delay := baseDelay * time.Duration(1<<uint(exponentialFactor))

	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// UnlockPage releases a lock on a page
func (lm *LockManager) UnlockPage(tid *transaction.TransactionID, pid tuple.PageID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

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

	if txPages, exists := lm.transactionLocks[tid]; exists {
		delete(txPages, pid)
		if len(txPages) == 0 {
			delete(lm.transactionLocks, tid)
		}
	}

	lm.depGraph.RemoveTransaction(tid)
	lm.processWaitQueue(pid)
}

func (lm *LockManager) processWaitQueue(pid tuple.PageID) {
	queue, exists := lm.waitQueue[pid]
	if !exists || len(queue) == 0 {
		return
	}

	remaining := make([]*LockRequest, 0)
	for _, request := range queue {
		if lm.canGrantLockImmediately(request.TID, pid, request.LockType) {
			lm.grantLock(request.TID, pid, request.LockType)
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

func (lm *LockManager) IsPageLocked(pid tuple.PageID) bool {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	locks, exists := lm.pageLocks[pid]
	return exists && len(locks) > 0
}

func (lm *LockManager) UnlockAllPages(tid *transaction.TransactionID) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	txPages, exists := lm.transactionLocks[tid]
	if !exists {
		return
	}

	pagesToUnlock := make([]tuple.PageID, 0)
	for pid := range txPages {
		pagesToUnlock = append(pagesToUnlock, pid)
	}

	for _, pid := range pagesToUnlock {
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
	}

	delete(lm.transactionLocks, tid)
	lm.depGraph.RemoveTransaction(tid)
	delete(lm.waitingFor, tid)

	for _, pid := range pagesToUnlock {
		lm.processWaitQueue(pid)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
