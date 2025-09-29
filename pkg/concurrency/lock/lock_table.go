package lock

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// LockTable manages the mapping of pages to locks and transactions to their held locks in a database system.
// It maintains a dual-index structure that allows efficient lookups in both directions:
// 1. Finding all locks held on a specific page (for conflict detection and lock granting)
// 2. Finding all locks held by a specific transaction (for cleanup and deadlock detection)
type LockTable struct {
	pageLocks        map[tuple.PageID][]*Lock
	transactionLocks map[*transaction.TransactionID]map[tuple.PageID]LockType
}

// NewLockTable creates and initializes a new LockTable instance with empty internal maps.
// This constructor ensures proper initialization of both indexing structures to prevent
// nil pointer dereferences during lock operations.
func NewLockTable() *LockTable {
	return &LockTable{
		pageLocks:        make(map[tuple.PageID][]*Lock),
		transactionLocks: make(map[*transaction.TransactionID]map[tuple.PageID]LockType),
	}
}

// GetPageLockTable returns the internal page-to-locks mapping for read-only access.
func (lt *LockTable) GetPageLockTable() map[tuple.PageID][]*Lock {
	return lt.pageLocks
}

// GetTransactionLockTable returns the internal transaction-to-locks mapping for read-only access.
func (lt *LockTable) GetTransactionLockTable() map[*transaction.TransactionID]map[tuple.PageID]LockType {
	return lt.transactionLocks
}

// HasSufficientLock checks if a transaction already holds a sufficient lock on a page
// for the requested operation. This method implements lock compatibility rules:
//
// 1. If the transaction holds an exclusive lock, it's sufficient for any request
// 2. If the transaction holds a shared lock, it's sufficient only for shared lock requests
// 3. If the transaction holds no lock, the request is not sufficient
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

// HasLockType checks if a transaction holds a specific type of lock on a specific page.
// This method provides exact lock type matching, unlike HasSufficientLock which checks
// compatibility.
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

// AddLock grants a new lock to a transaction on a specific page and updates both
// internal data structures atomically. This method assumes that compatibility
// checking has already been performed by the caller (typically the lock manager).
func (lt *LockTable) AddLock(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) {
	lock := NewLock(tid, lockType)
	lt.pageLocks[pid] = append(lt.pageLocks[pid], lock)

	if lt.transactionLocks[tid] == nil {
		lt.transactionLocks[tid] = make(map[tuple.PageID]LockType)
	}
	lt.transactionLocks[tid][pid] = lockType
}

// IsPageLocked checks if any locks are currently held on the specified page.
// This is a quick way to determine if a page is free or if there are potential
// conflicts that need to be resolved before granting new locks.
func (lt *LockTable) IsPageLocked(pid tuple.PageID) bool {
	locks, exists := lt.pageLocks[pid]
	return exists && len(locks) > 0
}

// UpgradeLock promotes a transaction's shared lock to an exclusive lock on the specified page.
func (lt *LockTable) UpgradeLock(tid *transaction.TransactionID, pid tuple.PageID) {
	for _, lock := range lt.pageLocks[pid] {
		if lock.TID == tid {
			lock.LockType = ExclusiveLock
			break
		}
	}

	lt.transactionLocks[tid][pid] = ExclusiveLock
}

// ReleaseAllLocks removes all locks held by a specific transaction and returns the list
// of affected pages. This method is typically called during transaction commit or abort
// to clean up all resources held by the transaction.
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

// ReleaseLock removes a specific lock held by a transaction on a specific page.
// This method provides fine-grained lock release, unlike ReleaseAllLocks which
// removes all locks for a transaction. It's useful for early lock release
// optimizations and selective lock management.
func (lt *LockTable) ReleaseLock(tid *transaction.TransactionID, pageID tuple.PageID) {
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

	if txPages, exists := lt.transactionLocks[tid]; exists {
		delete(txPages, pageID)
		if len(txPages) == 0 {
			delete(lt.transactionLocks, tid)
		}
	}
}
