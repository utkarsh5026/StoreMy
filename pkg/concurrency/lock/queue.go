package lock

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// WaitQueue manages transactions waiting for locks on pages.
type WaitQueue struct {
	pageWaitQueue      map[tuple.PageID][]*LockRequest
	transactionWaiting map[*transaction.TransactionID][]tuple.PageID
}

func NewWaitQueue() *WaitQueue {
	return &WaitQueue{
		pageWaitQueue:      make(map[tuple.PageID][]*LockRequest),
		transactionWaiting: make(map[*transaction.TransactionID][]tuple.PageID),
	}
}

func (wq *WaitQueue) Add(tid *transaction.TransactionID, pid tuple.PageID, lockType LockType) error {
	if wq.alreadyInPageQueue(tid, pid) {
		return fmt.Errorf("Already in the Page queue")
	}

	if wq.isInTransactionQueue(tid, pid) {
		return fmt.Errorf("Already in transaction queue")
	}

	request := NewLockRequest(tid, lockType)
	wq.pageWaitQueue[pid] = append(wq.pageWaitQueue[pid], request)
	wq.transactionWaiting[tid] = append(wq.transactionWaiting[tid], pid)
	return nil
}

func (wq *WaitQueue) Remove(tid *transaction.TransactionID, pid tuple.PageID) {
	wq.removeFromPageQueue(tid, pid)
	wq.removeFromTransactionQueue(tid, pid)
}

func (wq *WaitQueue) RemoveTransaction(tid *transaction.TransactionID) {
	waitingPages, exists := wq.transactionWaiting[tid]
	if !exists {
		return
	}

	for _, pid := range waitingPages {
		wq.Remove(tid, pid)
	}
}

func (wq *WaitQueue) GetRequests(pid tuple.PageID) []*LockRequest {
	return wq.pageWaitQueue[pid]
}

func (wq *WaitQueue) removeFromPageQueue(tid *transaction.TransactionID, pid tuple.PageID) {
	requestQueue, exists := wq.pageWaitQueue[pid]
	if !exists {
		return
	}

	newQueue := wq.filterPageQueue(requestQueue, tid)
	if len(newQueue) > 0 {
		wq.pageWaitQueue[pid] = newQueue
	} else {
		delete(wq.pageWaitQueue, pid)
	}
}

func (wq *WaitQueue) filterPageQueue(requestQueue []*LockRequest, tid *transaction.TransactionID) []*LockRequest {
	newQueue := make([]*LockRequest, 0)
	for _, req := range requestQueue {
		if req.TID != tid {
			newQueue = append(newQueue, req)
		}
	}
	return newQueue
}

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

func (wq *WaitQueue) filterTransactionQueue(waitingPages []tuple.PageID, pid tuple.PageID) []tuple.PageID {
	newWaitingPages := make([]tuple.PageID, 0)
	for _, waitingPid := range waitingPages {
		if !pid.Equals(waitingPid) {
			newWaitingPages = append(newWaitingPages, waitingPid)
		}
	}
	return newWaitingPages
}

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
