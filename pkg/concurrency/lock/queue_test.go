package lock

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"sync"
	"testing"
)

func TestNewWaitQueue(t *testing.T) {
	wq := NewWaitQueue()

	if wq == nil {
		t.Fatal("NewWaitQueue() returned nil")
	}

	if wq.pageWaitQueue == nil {
		t.Error("pageWaitQueue map not initialized")
	}

	if wq.transactionWaiting == nil {
		t.Error("transactionWaiting map not initialized")
	}

	// Verify empty state
	if len(wq.pageWaitQueue) != 0 {
		t.Error("pageWaitQueue should be empty initially")
	}

	if len(wq.transactionWaiting) != 0 {
		t.Error("transactionWaiting should be empty initially")
	}
}

func TestWaitQueueAdd(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Test adding first request
	err := wq.Add(tid, pid, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add first request: %v", err)
	}

	// Verify request was added to page queue
	requests := wq.GetRequests(pid)
	if len(requests) != 1 {
		t.Fatalf("Expected 1 request in page queue, got %d", len(requests))
	}

	if requests[0].TID != tid {
		t.Error("Request transaction ID doesn't match")
	}

	if requests[0].LockType != SharedLock {
		t.Error("Request lock type doesn't match")
	}

	// Verify transaction is tracked
	waitingPages := wq.GetPagesRequestedFor(tid)
	if len(waitingPages) != 1 {
		t.Fatalf("Expected transaction to be waiting for 1 page, got %d", len(waitingPages))
	}

	if !waitingPages[0].Equals(pid) {
		t.Error("Transaction not waiting for correct page")
	}
}

func TestWaitQueueAddMultiple(t *testing.T) {
	wq := NewWaitQueue()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Add multiple transactions to same page queue
	err := wq.Add(tid1, pid, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add tid1: %v", err)
	}

	err = wq.Add(tid2, pid, ExclusiveLock)
	if err != nil {
		t.Fatalf("Failed to add tid2: %v", err)
	}

	err = wq.Add(tid3, pid, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add tid3: %v", err)
	}

	// Verify FIFO ordering
	requests := wq.GetRequests(pid)
	if len(requests) != 3 {
		t.Fatalf("Expected 3 requests in page queue, got %d", len(requests))
	}

	if requests[0].TID != tid1 {
		t.Error("First request should be tid1")
	}

	if requests[1].TID != tid2 {
		t.Error("Second request should be tid2")
	}

	if requests[2].TID != tid3 {
		t.Error("Third request should be tid3")
	}

	// Verify each transaction is tracked
	for _, tid := range []*primitives.TransactionID{tid1, tid2, tid3} {
		waitingPages := wq.GetPagesRequestedFor(tid)
		if len(waitingPages) != 1 {
			t.Errorf("Transaction %d should be waiting for 1 page, got %d", tid.ID(), len(waitingPages))
		}
	}
}

func TestWaitQueueAddDuplicatePrevention(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Add first request
	err := wq.Add(tid, pid, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add first request: %v", err)
	}

	// Try to add duplicate - should fail
	err = wq.Add(tid, pid, ExclusiveLock)
	if err == nil {
		t.Error("Expected error when adding duplicate request")
	}

	// Verify only one request exists
	requests := wq.GetRequests(pid)
	if len(requests) != 1 {
		t.Fatalf("Expected 1 request after duplicate prevention, got %d", len(requests))
	}

	waitingPages := wq.GetPagesRequestedFor(tid)
	if len(waitingPages) != 1 {
		t.Fatalf("Expected transaction to be waiting for 1 page after duplicate prevention, got %d", len(waitingPages))
	}
}

func TestWaitQueueAddMultiplePages(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid1 := page.NewPageDescriptor(1, 1)
	pid2 := page.NewPageDescriptor(1, 2)
	pid3 := page.NewPageDescriptor(1, 3)

	// Transaction waiting for multiple pages
	err := wq.Add(tid, pid1, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add request for pid1: %v", err)
	}

	err = wq.Add(tid, pid2, ExclusiveLock)
	if err != nil {
		t.Fatalf("Failed to add request for pid2: %v", err)
	}

	err = wq.Add(tid, pid3, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add request for pid3: %v", err)
	}

	// Verify transaction is waiting for all pages
	waitingPages := wq.GetPagesRequestedFor(tid)
	if len(waitingPages) != 3 {
		t.Fatalf("Expected transaction to be waiting for 3 pages, got %d", len(waitingPages))
	}

	// Verify each page has the transaction in its queue
	for _, pid := range []primitives.PageID{pid1, pid2, pid3} {
		requests := wq.GetRequests(pid)
		if len(requests) != 1 {
			t.Errorf("Expected 1 request for page %v, got %d", pid, len(requests))
		}
		if len(requests) > 0 && requests[0].TID != tid {
			t.Errorf("Wrong transaction in queue for page %v", pid)
		}
	}
}

func TestWaitQueueRemove(t *testing.T) {
	wq := NewWaitQueue()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Add multiple requests
	wq.Add(tid1, pid, SharedLock)
	wq.Add(tid2, pid, ExclusiveLock)
	wq.Add(tid3, pid, SharedLock)

	// RemoveRequest middle transaction
	wq.RemoveRequest(tid2, pid)

	// Verify removal
	requests := wq.GetRequests(pid)
	if len(requests) != 2 {
		t.Fatalf("Expected 2 requests after removal, got %d", len(requests))
	}

	if requests[0].TID != tid1 {
		t.Error("First request should be tid1 after removal")
	}

	if requests[1].TID != tid3 {
		t.Error("Second request should be tid3 after removal")
	}

	// Verify tid2 is not tracked
	waitingPages := wq.GetPagesRequestedFor(tid2)
	if len(waitingPages) != 0 {
		t.Error("tid2 should not be waiting for any pages after removal")
	}

	// Verify tid1 and tid3 still tracked
	for _, tid := range []*primitives.TransactionID{tid1, tid3} {
		waitingPages := wq.GetPagesRequestedFor(tid)
		if len(waitingPages) != 1 {
			t.Errorf("Transaction %d should still be waiting for 1 page", tid.ID())
		}
	}
}

func TestWaitQueueRemoveLastRequest(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Add single request
	wq.Add(tid, pid, SharedLock)

	// RemoveRequest it
	wq.RemoveRequest(tid, pid)

	// Verify complete cleanup
	requests := wq.GetRequests(pid)
	if len(requests) != 0 {
		t.Error("Page queue should be empty after removing last request")
	}

	waitingPages := wq.GetPagesRequestedFor(tid)
	if len(waitingPages) != 0 {
		t.Error("Transaction should not be waiting for any pages after removal")
	}

	// Verify internal cleanup - page should be removed from map
	if _, exists := wq.pageWaitQueue[pid]; exists {
		t.Error("Page should be completely removed from pageWaitQueue when empty")
	}

	if _, exists := wq.transactionWaiting[tid]; exists {
		t.Error("Transaction should be completely removed from transactionWaiting when not waiting")
	}
}

func TestWaitQueueRemoveNonExistent(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Try to remove from empty queue - should not panic
	wq.RemoveRequest(tid, pid)

	// Add a different transaction
	tid2 := primitives.NewTransactionID()
	wq.Add(tid2, pid, SharedLock)

	// Try to remove non-existent transaction - should not affect existing one
	wq.RemoveRequest(tid, pid)

	requests := wq.GetRequests(pid)
	if len(requests) != 1 {
		t.Error("Removing non-existent transaction should not affect existing requests")
	}
}

func TestWaitQueueRemoveTransaction(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()
	pid1 := page.NewPageDescriptor(1, 1)
	pid2 := page.NewPageDescriptor(1, 2)
	pid3 := page.NewPageDescriptor(1, 3)

	// Transaction waiting for multiple pages
	wq.Add(tid, pid1, SharedLock)
	wq.Add(tid, pid2, ExclusiveLock)
	wq.Add(tid, pid3, SharedLock)

	// Add other transactions to same pages
	tid2 := primitives.NewTransactionID()
	wq.Add(tid2, pid1, SharedLock)
	wq.Add(tid2, pid2, SharedLock)

	// RemoveRequest entire transaction
	wq.RemoveAllForTransaction(tid)

	// Verify transaction completely removed
	waitingPages := wq.GetPagesRequestedFor(tid)
	if len(waitingPages) != 0 {
		t.Error("Transaction should not be waiting for any pages after RemoveAllForTransaction")
	}

	// Verify transaction removed from all page queues
	for _, pid := range []primitives.PageID{pid1, pid2, pid3} {
		requests := wq.GetRequests(pid)
		for _, req := range requests {
			if req.TID == tid {
				t.Errorf("Transaction should be removed from page %v queue", pid)
			}
		}
	}

	// Verify other transaction unaffected
	waitingPages2 := wq.GetPagesRequestedFor(tid2)
	if len(waitingPages2) != 2 {
		t.Errorf("Other transaction should still be waiting for 2 pages, got %d", len(waitingPages2))
	}

	// Verify pid3 queue is empty (only had tid)
	requests3 := wq.GetRequests(pid3)
	if len(requests3) != 0 {
		t.Error("pid3 queue should be empty after removing only transaction")
	}
}

func TestWaitQueueRemoveTransactionNonExistent(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()

	// RemoveRequest non-existent transaction - should not panic
	wq.RemoveAllForTransaction(tid)

	// Add some transactions
	tid2 := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)
	wq.Add(tid2, pid, SharedLock)

	// RemoveRequest non-existent transaction - should not affect existing ones
	wq.RemoveAllForTransaction(tid)

	requests := wq.GetRequests(pid)
	if len(requests) != 1 {
		t.Error("Removing non-existent transaction should not affect existing requests")
	}
}

func TestWaitQueueGetRequests(t *testing.T) {
	wq := NewWaitQueue()
	pid := page.NewPageDescriptor(1, 1)

	// Empty queue
	requests := wq.GetRequests(pid)
	if len(requests) != 0 {
		t.Error("GetRequests should return empty slice for non-existent page")
	}

	// Add requests and verify order
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	wq.Add(tid1, pid, SharedLock)
	wq.Add(tid2, pid, ExclusiveLock)

	requests = wq.GetRequests(pid)
	if len(requests) != 2 {
		t.Fatalf("Expected 2 requests, got %d", len(requests))
	}

	if requests[0].TID != tid1 || requests[0].LockType != SharedLock {
		t.Error("First request doesn't match expected values")
	}

	if requests[1].TID != tid2 || requests[1].LockType != ExclusiveLock {
		t.Error("Second request doesn't match expected values")
	}
}

func TestWaitQueueGetPagesRequestedFor(t *testing.T) {
	wq := NewWaitQueue()
	tid := primitives.NewTransactionID()

	// Non-existent transaction
	pages := wq.GetPagesRequestedFor(tid)
	if len(pages) != 0 {
		t.Error("GetPagesRequestedFor should return empty slice for non-existent transaction")
	}

	// Add requests
	pid1 := page.NewPageDescriptor(1, 1)
	pid2 := page.NewPageDescriptor(1, 2)
	wq.Add(tid, pid1, SharedLock)
	wq.Add(tid, pid2, ExclusiveLock)

	pages = wq.GetPagesRequestedFor(tid)
	if len(pages) != 2 {
		t.Fatalf("Expected 2 pages, got %d", len(pages))
	}

	// Verify pages are present (order might vary based on implementation)
	found1, found2 := false, false
	for _, page := range pages {
		if page.Equals(pid1) {
			found1 = true
		}
		if page.Equals(pid2) {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Error("Not all expected pages found in GetPagesRequestedFor result")
	}
}

func TestWaitQueueConsistency(t *testing.T) {
	wq := NewWaitQueue()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid1 := page.NewPageDescriptor(1, 1)
	pid2 := page.NewPageDescriptor(1, 2)

	// Build complex state
	wq.Add(tid1, pid1, SharedLock)
	wq.Add(tid1, pid2, ExclusiveLock)
	wq.Add(tid2, pid1, SharedLock)

	// Verify consistency between both data structures
	// Check pageWaitQueue vs transactionWaiting consistency
	for pid, requests := range wq.pageWaitQueue {
		for _, req := range requests {
			waitingPages := wq.GetPagesRequestedFor(req.TID)
			found := false
			for _, waitingPid := range waitingPages {
				if waitingPid.Equals(pid) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Inconsistency: transaction %d in page %v queue but not in transactionWaiting", req.TID.ID(), pid)
			}
		}
	}

	// Check transactionWaiting vs pageWaitQueue consistency
	for tid, waitingPages := range wq.transactionWaiting {
		for _, pid := range waitingPages {
			requests := wq.GetRequests(pid)
			found := false
			for _, req := range requests {
				if req.TID == tid {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Inconsistency: transaction %d waiting for page %v but not in page queue", tid.ID(), pid)
			}
		}
	}
}

func TestWaitQueueWithExternalSynchronization(t *testing.T) {
	wq := NewWaitQueue()
	var mutex sync.RWMutex
	numGoroutines := 10
	numOperationsPerGoroutine := 20

	var wg sync.WaitGroup

	// Concurrent adds and removes with external synchronization
	// This tests that the WaitQueue works correctly when properly synchronized
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := primitives.NewTransactionID()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				pid := page.NewPageDescriptor(1, primitives.PageNumber((id*numOperationsPerGoroutine+j)%5+1)) // Use pages 1-5

				// Add request with synchronization
				mutex.Lock()
				err := wq.Add(tid, pid, SharedLock)
				mutex.Unlock()

				if err != nil {
					// Might fail due to duplicates, which is expected
					continue
				}

				// Immediately remove it with synchronization
				mutex.Lock()
				wq.RemoveRequest(tid, pid)
				mutex.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// After all concurrent operations, queues should be empty
	mutex.RLock()
	for i := 1; i <= 5; i++ {
		pid := page.NewPageDescriptor(1, primitives.PageNumber(i))
		requests := wq.GetRequests(pid)
		if len(requests) > 0 {
			t.Errorf("Page %d should have empty queue after concurrent operations, got %d requests", i, len(requests))
		}
	}
	mutex.RUnlock()
}

func TestWaitQueueStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	wq := NewWaitQueue()
	numTransactions := 100
	numPages := 10

	var transactions []*primitives.TransactionID
	var pages []primitives.PageID

	// Create transactions and pages
	for i := 0; i < numTransactions; i++ {
		transactions = append(transactions, primitives.NewTransactionID())
	}

	for i := 0; i < numPages; i++ {
		pages = append(pages, page.NewPageDescriptor(1, primitives.PageNumber(i+1)))
	}

	// Each transaction requests locks on multiple pages
	for i, tid := range transactions {
		for j := 0; j < 3; j++ { // Each transaction requests 3 pages
			pageIdx := (i + j) % numPages
			err := wq.Add(tid, pages[pageIdx], SharedLock)
			if err != nil {
				t.Fatalf("Failed to add request for transaction %d, page %d: %v", i, pageIdx, err)
			}
		}
	}

	// Verify all requests were added
	totalRequests := 0
	for _, pid := range pages {
		requests := wq.GetRequests(pid)
		totalRequests += len(requests)
	}

	expectedRequests := numTransactions * 3
	if totalRequests != expectedRequests {
		t.Errorf("Expected %d total requests, got %d", expectedRequests, totalRequests)
	}

	// RemoveRequest half the transactions
	for i := 0; i < numTransactions/2; i++ {
		wq.RemoveAllForTransaction(transactions[i])
	}

	// Verify removals
	remainingRequests := 0
	for _, pid := range pages {
		requests := wq.GetRequests(pid)
		remainingRequests += len(requests)
	}

	expectedRemaining := (numTransactions - numTransactions/2) * 3
	if remainingRequests != expectedRemaining {
		t.Errorf("Expected %d remaining requests after removals, got %d", expectedRemaining, remainingRequests)
	}

	// Verify removed transactions are not tracked
	for i := 0; i < numTransactions/2; i++ {
		waitingPages := wq.GetPagesRequestedFor(transactions[i])
		if len(waitingPages) != 0 {
			t.Errorf("Removed transaction %d should not be waiting for any pages", i)
		}
	}
}

func TestWaitQueueLockTypePreservation(t *testing.T) {
	wq := NewWaitQueue()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pid := page.NewPageDescriptor(1, 1)

	// Add requests with different lock types
	err := wq.Add(tid1, pid, SharedLock)
	if err != nil {
		t.Fatalf("Failed to add shared lock request: %v", err)
	}

	err = wq.Add(tid2, pid, ExclusiveLock)
	if err != nil {
		t.Fatalf("Failed to add exclusive lock request: %v", err)
	}

	// Verify lock types are preserved
	requests := wq.GetRequests(pid)
	if len(requests) != 2 {
		t.Fatalf("Expected 2 requests, got %d", len(requests))
	}

	if requests[0].LockType != SharedLock {
		t.Error("First request should be SharedLock")
	}

	if requests[1].LockType != ExclusiveLock {
		t.Error("Second request should be ExclusiveLock")
	}
}

func TestWaitQueueFIFOOrdering(t *testing.T) {
	wq := NewWaitQueue()
	pid := page.NewPageDescriptor(1, 1)
	var transactions []*primitives.TransactionID

	// Add multiple transactions
	for i := 0; i < 5; i++ {
		tid := primitives.NewTransactionID()
		transactions = append(transactions, tid)
		err := wq.Add(tid, pid, SharedLock)
		if err != nil {
			t.Fatalf("Failed to add transaction %d: %v", i, err)
		}
	}

	// Verify FIFO ordering
	requests := wq.GetRequests(pid)
	if len(requests) != 5 {
		t.Fatalf("Expected 5 requests, got %d", len(requests))
	}

	for i, req := range requests {
		if req.TID != transactions[i] {
			t.Errorf("Request %d should be transaction %d", i, i)
		}
	}

	// RemoveRequest middle transaction and verify ordering is preserved
	wq.RemoveRequest(transactions[2], pid)

	requests = wq.GetRequests(pid)
	if len(requests) != 4 {
		t.Fatalf("Expected 4 requests after removal, got %d", len(requests))
	}

	expectedOrder := []*primitives.TransactionID{
		transactions[0], transactions[1], transactions[3], transactions[4],
	}

	for i, req := range requests {
		if req.TID != expectedOrder[i] {
			t.Errorf("After removal, position %d should be transaction at original position %d", i, []int{0, 1, 3, 4}[i])
		}
	}
}
