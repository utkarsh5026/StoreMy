package lock

import (
	"fmt"
	"slices"
	"storemy/pkg/primitives"
)

// WaitQueue implements a two-way mapping system for managing lock request queues in a database system.
//
// The WaitQueue maintains two critical data structures:
//   - pageWaitQueue: A map from PageID to an ordered slice of LockRequest pointers, representing
//     the FIFO queue of transactions waiting to acquire locks on each page. The order matters
//     as it determines which transaction gets the lock next when it becomes available.
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
// 1. A transaction successfully acquires the lock it was waiting for
// 2. A specific lock request times out
// 3. A transaction voluntarily cancels a specific lock request
func (wq *WaitQueue) RemoveRequest(tid *primitives.TransactionID, pid primitives.PageID) {
	wq.removeFromPageQueue(tid, pid)
	wq.removeFromTransactionQueue(tid, pid)
}

// RemoveAllForTransaction completely removes a transaction from all wait queues it's participating in.
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
// FIFO ordering for remaining requests. This is an internal helper method that handles the
// page-side cleanup when a transaction's request is cancelled or fulfilled.
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

// updateOrDelete updates the map with the new slice, or deletes the key if the slice is empty.
// This maintains map cleanliness by avoiding storage of empty slices.
func updateOrDelete[K comparable, V any](m map[K][]V, key K, newSlice []V) {
	if len(newSlice) > 0 {
		m[key] = newSlice
	} else {
		delete(m, key)
	}
}
