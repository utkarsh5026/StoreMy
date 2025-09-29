package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// WaitQueue implements a two-way mapping system for managing lock request queues in a database system.
// It tracks which transactions are waiting for locks on specific pages and which pages
// each transaction is waiting for. This dual indexing allows for fast lookups in both directions
// and is essential for deadlock detection and lock management.
//
// The WaitQueue maintains two critical data structures:
//   - pageWaitQueue: A map from PageID to an ordered slice of LockRequest pointers, representing
//     the FIFO queue of transactions waiting to acquire locks on each page. The order matters
//     as it determines which transaction gets the lock next when it becomes available.
//   - transactionWaiting: A reverse index mapping each TransactionID to all PageIDs it's currently
//     waiting for. This enables efficient transaction cleanup and deadlock cycle detection.
type WaitQueue struct {
	pageWaitQueue      map[tuple.PageID][]*LockRequest               // Page -> Queue of waiting transactions
	transactionWaiting map[*transaction.TransactionID][]tuple.PageID // Transaction -> Pages it's waiting for
}

// NewWaitQueue creates and initializes a new WaitQueue instance with empty internal maps.
func NewWaitQueue() *WaitQueue {
	return &WaitQueue{
		pageWaitQueue:      make(map[tuple.PageID][]*LockRequest),
		transactionWaiting: make(map[*transaction.TransactionID][]tuple.PageID),
	}
}

// Add enqueues a transaction's lock request for a specific page, maintaining FIFO ordering.
// This method performs duplicate checking to ensure a transaction doesn't get added multiple
// times for the same page, which would corrupt the queue semantics and potentially cause
// deadlocks or inconsistent lock granting.
//
// The method updates both internal data structures atomically (from the caller's perspective):
// 1. Adds the lock request to the end of the page's wait queue (FIFO)
// 2. Records that this transaction is now waiting for this page
func (wq *WaitQueue) Add(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) error {
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

// Remove atomically removes a transaction's lock request for a specific page from both
// internal data structures. This method is typically called when:
// 1. A transaction successfully acquires the lock it was waiting for
// 2. A transaction is aborted and needs to release its pending requests
// 3. Lock timeout occurs and the request needs to be cancelled
func (wq *WaitQueue) Remove(tid *transaction.TransactionID, pid tuple.PageID) {
	wq.removeFromPageQueue(tid, pid)
	wq.removeFromTransactionQueue(tid, pid)
}

// RemoveTransaction completely removes a transaction from all wait queues it's participating in.
// This is a comprehensive cleanup operation typically performed during:
// 1. Transaction abort - need to release all pending lock requests
// 2. Transaction commit - clean up any remaining requests (shouldn't happen in normal flow)
// 3. Deadlock resolution - victim transaction needs complete cleanup
// 4. Transaction timeout - remove all pending requests
func (wq *WaitQueue) RemoveTransaction(tid *transaction.TransactionID) {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return
	}

	for _, pid := range waitingPages {
		wq.Remove(tid, pid)
	}
}

// GetRequests returns an ordered slice of all lock requests waiting for the specified page.
// The returned slice maintains FIFO ordering where the first element is the next transaction
// in line to receive the lock when it becomes available.
func (wq *WaitQueue) GetRequests(pid tuple.PageID) []*LockRequest {
	return wq.pageWaitQueue[pid]
}

// GetPagesRequestedFor returns all pages that a specific transaction is currently waiting
// to acquire locks on.
func (wq *WaitQueue) GetPagesRequestedFor(tid *transaction.TransactionID) []tuple.PageID {
	return wq.transactionWaiting[tid]
}

// removeFromPageQueue removes a specific transaction from a page's wait queue while preserving
// FIFO ordering for remaining requests. This is an internal helper method that handles the
// page-side cleanup when a transaction's request is cancelled or fulfilled.
func (wq *WaitQueue) removeFromPageQueue(tid *transaction.TransactionID, pid tuple.PageID) {
	requestQueue, exists := wq.pageWaitQueue[pid]
	if !exists {
		return
	}

	newQueue := wq.filterPageQueue(requestQueue, tid)
	if len(newQueue) > 0 {
		wq.pageWaitQueue[pid] = newQueue
	} else {
		delete(wq.pageWaitQueue, pid) // Clean up empty queue
	}
}

// filterPageQueue creates a new queue slice excluding all requests from the specified transaction.
// This helper method ensures that FIFO ordering is preserved for all remaining transactions
// while efficiently removing the target transaction's requests.
func (wq *WaitQueue) filterPageQueue(requestQueue []*LockRequest, tid *transaction.TransactionID) []*LockRequest {
	newQueue := make([]*LockRequest, 0)
	for _, req := range requestQueue {
		if req.TID != tid {
			newQueue = append(newQueue, req)
		}
	}
	return newQueue
}

// removeFromTransactionQueue removes a specific page from a transaction's waiting list,
// which is the reverse-index side of the queue removal operation. This method maintains
// the consistency of the transactionWaiting map by either updating the page list or
// removing the transaction entry entirely if it's no longer waiting for any pages.
func (wq *WaitQueue) removeFromTransactionQueue(tid *transaction.TransactionID, pid tuple.PageID) {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return
	}

	newWaitingPages := wq.filterTransactionQueue(waitingPages, pid)
	if len(newWaitingPages) > 0 {
		wq.transactionWaiting[tid] = newWaitingPages
	} else {
		delete(wq.transactionWaiting, tid)
	}
}

// filterTransactionQueue creates a new page list excluding the specified page from
// a transaction's waiting list. This maintains the order in which the transaction
// requested locks on the remaining pages.
func (wq *WaitQueue) filterTransactionQueue(waitingPages []tuple.PageID, pid tuple.PageID) []tuple.PageID {
	newWaitingPages := make([]tuple.PageID, 0)
	for _, waitingPid := range waitingPages {
		if !pid.Equals(waitingPid) {
			newWaitingPages = append(newWaitingPages, waitingPid)
		}
	}
	return newWaitingPages
}

// alreadyInPageQueue checks if a transaction already has a pending lock request in the
// specified page's wait queue. This is used to prevent duplicate requests which could
// corrupt the queue semantics and cause incorrect lock granting behavior.
func (wq *WaitQueue) alreadyInPageQueue(tid *transaction.TransactionID, pid tuple.PageID) bool {
	queue, exists := wq.pageWaitQueue[pid]
	if !exists {
		return false
	}

	for _, req := range queue {
		if req.TID == tid {
			return true
		}
	}
	return false
}

// isInTransactionQueue checks if a transaction is already recorded as waiting for the
// specified page in the reverse index. This provides a consistency check against the
// transactionWaiting map and helps detect data structure corruption or race conditions.
func (wq *WaitQueue) isInTransactionQueue(tid *transaction.TransactionID, pid tuple.PageID) bool {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return false
	}

	for _, waitingPid := range waitingPages {
		if pid.Equals(waitingPid) {
			return true
		}
	}
	return false
}
