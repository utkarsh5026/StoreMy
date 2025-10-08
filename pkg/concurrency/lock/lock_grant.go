package lock

import (
	"slices"
	"storemy/pkg/primitives"
)

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

	return !slices.ContainsFunc(locks, func(lock *Lock) bool {
		return lock.TID != tid && lock.LockType == ExclusiveLock
	})
}

// GrantLock grants a lock to a primitives.
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
	return !slices.ContainsFunc(locks, func(lock *Lock) bool {
		return lock.TID != tid
	})
}
