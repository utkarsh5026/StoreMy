package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

type LockManager struct {
	pageLocks  map[tuple.PageID][]*Lock                                 // Page -> list of locks
	txLocks    map[*transaction.TransactionID]map[tuple.PageID]LockType // Transaction -> pages it has locked
	waitingFor map[*transaction.TransactionID][]tuple.PageID            // Transaction -> pages it's waiting for
	depGraph   *DependencyGraph
	mutex      sync.RWMutex
	waitQueue  map[tuple.PageID][]*LockRequest // Page -> waiting transactions
}

func NewLockManager() *LockManager {
	return &LockManager{
		pageLocks:  make(map[tuple.PageID][]*Lock),
		txLocks:    make(map[*transaction.TransactionID]map[tuple.PageID]LockType),
		waitingFor: make(map[*transaction.TransactionID][]tuple.PageID),
		depGraph:   NewDependencyGraph(),
		waitQueue:  make(map[tuple.PageID][]*LockRequest),
	}
}

func (lm *LockManager) LockPage(tid *transaction.TransactionID, pid tuple.PageID, exclusive bool) error {
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

	return lm.acquireLock(tid, pid, lockType)
}

func (lm *LockManager) alreadyHasLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	if txPages, exists := lm.txLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid]; hasPage {
			if currentLock == ExclusiveLock {
				return true
			}

			if currentLock == SharedLock && lockType == SharedLock {
				return true
			}
		}
	}
	return false
}

func (lm *LockManager) acquireLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) error {
	maxRetries := 1000
	retryDelay := time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		lm.mutex.Lock()

		if lm.alreadyHasLock(tid, pid, lockType) {
			lm.mutex.Unlock()
			return nil
		}

		// Try to upgrade lock if we have a shared lock and need exclusive
		if lockType == ExclusiveLock && lm.hasLock(tid, pid, SharedLock) {
			if lm.canUpgradeLock(tid, pid) {
				lm.upgradeLock(tid, pid)
				lm.mutex.Unlock()
				return nil
			}
		}

		if lm.canGrantLock(tid, pid, lockType) {
			lm.grantLock(tid, pid, lockType)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return nil
		}

		lm.addToWaitQueue(tid, pid, lockType)
		lm.updateDependencies(tid, pid, lockType)

		if lm.depGraph.HasCycle() {
			// Remove from wait queue and dependencies
			lm.removeFromWaitQueue(tid, pid)
			lm.depGraph.RemoveTransaction(tid)
			lm.mutex.Unlock()
			return fmt.Errorf("deadlock detected for transaction %d", tid.ID())
		}

		lm.mutex.Unlock()

		// Wait with exponential backoff
		delay := retryDelay * time.Duration(1<<uint(min(attempt/10, 10)))
		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
		time.Sleep(delay)
	}

	return fmt.Errorf("timeout waiting for lock on page %v", pid)
}

func (lm *LockManager) hasLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	if txPages, exists := lm.txLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid]; hasPage {
			return currentLock == lockType
		}
	}
	return false
}

func (lm *LockManager) canGrantLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	locks, exists := lm.pageLocks[pid]
	if !exists || len(locks) == 0 {
		return true // No locks on this page
	}

	if lockType == ExclusiveLock {
		// Exclusive lock requires no other locks (except if we already have one)
		for _, lock := range locks {
			if lock.TID != tid {
				return false
			}
		}
		return true
	}

	// Shared lock can coexist with other shared locks
	for _, lock := range locks {
		if lock.TID != tid && lock.LockType == ExclusiveLock {
			return false
		}
	}
	return true
}

func (lm *LockManager) grantLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	// Create new lock
	lock := &Lock{
		TID:       tid,
		LockType:  lockType,
		GrantTime: time.Now(),
	}

	// Add to page locks
	lm.pageLocks[pid] = append(lm.pageLocks[pid], lock)

	// Add to transaction locks
	if lm.txLocks[tid] == nil {
		lm.txLocks[tid] = make(map[tuple.PageID]LockType)
	}
	lm.txLocks[tid][pid] = lockType

	// Remove from waiting list
	delete(lm.waitingFor, tid)
}

func (lm *LockManager) canUpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) bool {
	locks := lm.pageLocks[pid]

	// Can upgrade if we're the only one holding a lock on this page
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

	// Update transaction's lock record
	lm.txLocks[tid][pid] = ExclusiveLock
}

func (lm *LockManager) addToWaitQueue(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	// Check if already in wait queue for this page
	if queue, exists := lm.waitQueue[pid]; exists {
		for _, req := range queue {
			if req.TID == tid {
				return // Already waiting for this page
			}
		}
	}

	// Check if already waiting for this page
	if waitingPages, exists := lm.waitingFor[tid]; exists {
		for _, waitingPid := range waitingPages {
			if pid.Equals(waitingPid) {
				return // Already waiting for this page
			}
		}
	}

	request := &LockRequest{
		TID:      tid,
		LockType: lockType,
		Chan:     make(chan bool, 1),
	}

	lm.waitQueue[pid] = append(lm.waitQueue[pid], request)

	// Track what this transaction is waiting for
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

	// Remove this specific page from waiting list
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

		// Add dependency based on lock compatibility
		if lockType == ExclusiveLock || lock.LockType == ExclusiveLock {
			// tid is waiting for lock.TID
			lm.depGraph.AddEdge(tid, lock.TID)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
