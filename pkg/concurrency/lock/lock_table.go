package lock

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// LockTable manages the mapping of pages to locks and transactions to their held locks.
type LockTable struct {
	pageLocks        map[tuple.PageID][]*Lock
	transactionLocks map[*transaction.TransactionID]map[tuple.PageID]LockType
}

func NewLockTable() *LockTable {
	return &LockTable{
		pageLocks:        make(map[tuple.PageID][]*Lock),
		transactionLocks: make(map[*transaction.TransactionID]map[tuple.PageID]LockType),
	}
}

func (lt *LockTable) GetPageLockTable() map[tuple.PageID][]*Lock {
	return lt.pageLocks
}

func (lt *LockTable) GetTransactionLockTable() map[*transaction.TransactionID]map[tuple.PageID]LockType {
	return lt.transactionLocks
}

// HasSufficientLock checks if the transaction already holds a sufficient lock on the page.
func (lt *LockTable) HasSufficientLock(tid *transaction.TransactionID, pageID tuple.PageID, reqLockType LockType) bool {
	transactionPages, exists := lt.transactionLocks[tid]
	if !exists {
		return false
	}

	currentLockType, hasPage := transactionPages[pageID]
	if !hasPage {
		return false
	}

	if currentLockType == ExclusiveLock {
		return true
	}

	return currentLockType == SharedLock && reqLockType == SharedLock
}

func (lt *LockTable) HasLockType(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) bool {
	if txPages, exists := lt.transactionLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid]; hasPage {
			return currentLock == lockType
		}
	}
	return false
}

func (lt *LockTable) GetPageLocks(pid tuple.PageID) []*Lock {
	return lt.pageLocks[pid]
}

func (lt *LockTable) AddLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	lock := NewLock(tid, lockType)
	lt.pageLocks[pid] = append(lt.pageLocks[pid], lock)

	if lt.transactionLocks[tid] == nil {
		lt.transactionLocks[tid] = make(map[tuple.PageID]LockType)
	}
	lt.transactionLocks[tid][pid] = lockType
}

func (lt *LockTable) IsPageLocked(pid tuple.PageID) bool {
	locks, exists := lt.pageLocks[pid]
	return exists && len(locks) > 0
}

func (lt *LockTable) UpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) {
	for _, lock := range lt.pageLocks[pid] {
		if lock.TID == tid {
			lock.LockType = ExclusiveLock
			break
		}
	}

	lt.transactionLocks[tid][pid] = ExclusiveLock
}

func (lt *LockTable) ReleaseAllLocks(tid *transaction.TransactionID) []tuple.PageID {
	txPages, exists := lt.transactionLocks[tid]
	if !exists {
		return nil
	}

	affectedPages := make([]tuple.PageID, 0, len(txPages))
	for pid := range txPages {
		affectedPages = append(affectedPages, pid)
	}

	for _, pid := range affectedPages {
		if locks, exists := lt.pageLocks[pid]; exists {
			newLocks := make([]*Lock, 0, len(locks))
			for _, lock := range locks {
				if lock.TID != tid {
					newLocks = append(newLocks, lock)
				}
			}

			if len(newLocks) > 0 {
				lt.pageLocks[pid] = newLocks
			} else {
				delete(lt.pageLocks, pid)
			}
		}
	}

	delete(lt.transactionLocks, tid)
	return affectedPages
}

func (lt *LockTable) ReleaseLock(tid *transaction.TransactionID, pageID tuple.PageID) {
	// Remove from page locks
	if locks, exists := lt.pageLocks[pageID]; exists {
		newLocks := make([]*Lock, 0, len(locks))
		for _, lock := range locks {
			if lock.TID != tid {
				newLocks = append(newLocks, lock)
			}
		}

		if len(newLocks) > 0 {
			lt.pageLocks[pageID] = newLocks
		} else {
			delete(lt.pageLocks, pageID)
		}
	}

	// Remove from transaction locks
	if txPages, exists := lt.transactionLocks[tid]; exists {
		delete(txPages, pageID)
		if len(txPages) == 0 {
			delete(lt.transactionLocks, tid)
		}
	}
}
