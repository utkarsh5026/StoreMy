package lock

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

type LockGrantor struct {
	lockTable *LockTable
	waitQueue *WaitQueue
	depGraph  *DependencyGraph
}

// NewLockGrantor creates a new lock grantor.
func NewLockGrantor(lockTable *LockTable, waitQueue *WaitQueue, depGraph *DependencyGraph) *LockGrantor {
	return &LockGrantor{
		lockTable: lockTable,
		waitQueue: waitQueue,
		depGraph:  depGraph,
	}
}

// CanGrantImmediately determines if a lock can be granted without waiting.
func (lg *LockGrantor) CanGrantImmediately(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
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

	for _, lock := range locks {
		if lock.TID != tid && lock.LockType == ExclusiveLock {
			return false
		}
	}
	return true
}

// GrantLock grants a lock to a transaction.
func (lg *LockGrantor) GrantLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	lg.lockTable.AddLock(tid, pid, lockType)
	lg.waitQueue.Remove(tid, pid)
}

// CanUpgradeLock checks if a lock can be upgraded from shared to exclusive.
func (lg *LockGrantor) CanUpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) bool {
	if !lg.lockTable.HasLockType(tid, pid, SharedLock) {
		return false
	}

	locks := lg.lockTable.GetPageLocks(pid)
	for _, lock := range locks {
		if lock.TID != tid {
			return false
		}
	}
	return true
}
