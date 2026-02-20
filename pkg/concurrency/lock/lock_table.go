package lock

import (
	"maps"
	"slices"
	"storemy/pkg/primitives"
)

// LockTable manages the mapping of pages to locks and transactions to their held locks in a database system.
// It maintains a dual-index structure that allows efficient lookups in both directions:
// 1. Finding all locks held on a specific page (for conflict detection and lock granting)
// 2. Finding all locks held by a specific transaction (for cleanup and deadlock detection)
//
// Note: Pages are keyed by HashCode (content-based) rather than PageID (pointer-based)
// to ensure that different *PageDescriptor instances for the same logical page are
// treated as equivalent, matching the behavior of LRUPageCache.
type LockTable struct {
	pageLocks        map[primitives.HashCode][]*Lock
	transactionLocks map[*primitives.TransactionID]map[primitives.HashCode]LockType
	// hashToPageID records one canonical PageID per HashCode so ReleaseAllLocks
	// can return PageIDs for processWaitQueue even though internal keys are HashCodes.
	hashToPageID map[primitives.HashCode]primitives.PageID
}

// NewLockTable creates and initializes a new LockTable instance with empty internal maps.
// This constructor ensures proper initialization of both indexing structures to prevent
// nil pointer dereferences during lock operations.
func NewLockTable() *LockTable {
	return &LockTable{
		pageLocks:        make(map[primitives.HashCode][]*Lock),
		transactionLocks: make(map[*primitives.TransactionID]map[primitives.HashCode]LockType),
		hashToPageID:     make(map[primitives.HashCode]primitives.PageID),
	}
}

// GetPageLockTable returns a page-to-locks mapping keyed by PageID for read-only access.
// The returned map uses the canonical PageID stored when each lock was added,
// so callers that reuse the same *PageDescriptor instance will get correct lookups.
func (lt *LockTable) GetPageLockTable() map[primitives.PageID][]*Lock {
	result := make(map[primitives.PageID][]*Lock, len(lt.pageLocks))
	for hash, locks := range lt.pageLocks {
		if pid, ok := lt.hashToPageID[hash]; ok {
			result[pid] = locks
		}
	}
	return result
}

// GetTransactionLockTable returns a transaction-to-locks mapping for read-only access.
// The inner map is keyed by the canonical PageID stored when each lock was added,
// so callers that reuse the same *PageDescriptor instance will get correct lookups.
func (lt *LockTable) GetTransactionLockTable() map[*primitives.TransactionID]map[primitives.PageID]LockType {
	result := make(map[*primitives.TransactionID]map[primitives.PageID]LockType, len(lt.transactionLocks))
	for tid, hashMap := range lt.transactionLocks {
		pageMap := make(map[primitives.PageID]LockType, len(hashMap))
		for hash, lockType := range hashMap {
			if pid, ok := lt.hashToPageID[hash]; ok {
				pageMap[pid] = lockType
			}
		}
		result[tid] = pageMap
	}
	return result
}

// HasSufficientLock checks if a transaction already holds a sufficient lock on a page
// for the requested operation. This method implements lock compatibility rules:
//
// 1. If the transaction holds an exclusive lock, it's sufficient for any request
// 2. If the transaction holds a shared lock, it's sufficient only for shared lock requests
// 3. If the transaction holds no lock, the request is not sufficient
func (lt *LockTable) HasSufficientLock(tid *primitives.TransactionID, pageID primitives.PageID, reqLockType LockType) bool {
	transactionPages, exists := lt.transactionLocks[tid]
	if !exists {
		return false
	}

	currentLockType, hasPage := transactionPages[pageID.HashCode()]
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
func (lt *LockTable) HasLockType(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) bool {
	if txPages, exists := lt.transactionLocks[tid]; exists {
		if currentLock, hasPage := txPages[pid.HashCode()]; hasPage {
			return currentLock == lockType
		}
	}
	return false
}

func (lt *LockTable) GetPageLocks(pid primitives.PageID) []*Lock {
	return slices.Clone(lt.pageLocks[pid.HashCode()])
}

// AddLock grants a new lock to a transaction on a specific page and updates both
// internal data structures atomically. This method assumes that compatibility
// checking has already been performed by the caller (typically the lock manager).
func (lt *LockTable) AddLock(tid *primitives.TransactionID, pid primitives.PageID, lockType LockType) {
	hash := pid.HashCode()
	lock := NewLock(tid, lockType)
	lt.pageLocks[hash] = append(lt.pageLocks[hash], lock)

	if lt.transactionLocks[tid] == nil {
		lt.transactionLocks[tid] = make(map[primitives.HashCode]LockType)
	}
	lt.transactionLocks[tid][hash] = lockType
	lt.hashToPageID[hash] = pid
}

// IsPageLocked checks if any locks are currently held on the specified page.
// This is a quick way to determine if a page is free or if there are potential
// conflicts that need to be resolved before granting new locks.
func (lt *LockTable) IsPageLocked(pid primitives.PageID) bool {
	locks, exists := lt.pageLocks[pid.HashCode()]
	return exists && len(locks) > 0
}

// UpgradeLock promotes a transaction's shared lock to an exclusive lock on the specified page.
func (lt *LockTable) UpgradeLock(tid *primitives.TransactionID, pid primitives.PageID) {
	hash := pid.HashCode()
	for _, lock := range lt.pageLocks[hash] {
		if lock.TID == tid {
			lock.LockType = ExclusiveLock
			break
		}
	}

	lt.transactionLocks[tid][hash] = ExclusiveLock
}

// ReleaseAllLocks removes all locks held by a specific transaction and returns the list
// of affected pages. This method is typically called during transaction commit or abort
// to clean up all resources held by the transaction.
func (lt *LockTable) ReleaseAllLocks(tid *primitives.TransactionID) []primitives.PageID {
	txPages, exists := lt.transactionLocks[tid]
	if !exists {
		return nil
	}

	affectedHashes := slices.Collect(maps.Keys(txPages))
	affectedPages := make([]primitives.PageID, 0, len(affectedHashes))

	for _, hash := range affectedHashes {
		if locks, exists := lt.pageLocks[hash]; exists {
			updated := slices.DeleteFunc(slices.Clone(locks), func(l *Lock) bool {
				return l.TID == tid
			})
			updateOrDelete(lt.pageLocks, hash, updated)
		}
		if pid, ok := lt.hashToPageID[hash]; ok {
			affectedPages = append(affectedPages, pid)
		}
	}

	delete(lt.transactionLocks, tid)
	return affectedPages
}

// ReleaseLock removes a specific lock held by a transaction on a specific page.
// This method provides fine-grained lock release, unlike ReleaseAllLocks which
// removes all locks for a transaction. It's useful for early lock release
// optimizations and selective lock management.
func (lt *LockTable) ReleaseLock(tid *primitives.TransactionID, pageID primitives.PageID) {
	hash := pageID.HashCode()
	if locks, exists := lt.pageLocks[hash]; exists {
		newLocks := slices.DeleteFunc(slices.Clone(locks), func(l *Lock) bool {
			return l.TID == tid
		})

		updateOrDelete(lt.pageLocks, hash, newLocks)
	}

	if txPages, exists := lt.transactionLocks[tid]; exists {
		delete(txPages, hash)
		if len(txPages) == 0 {
			delete(lt.transactionLocks, tid)
		}
	}
}
