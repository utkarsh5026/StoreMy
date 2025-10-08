package lock

import (
	"storemy/pkg/primitives"
	"sync"
	"testing"
)

func TestNewDependencyGraph(t *testing.T) {
	dg := NewDependencyGraph()
	if dg == nil {
		t.Fatal("NewDependencyGraph returned nil")
	}
	if dg.edges == nil {
		t.Error("edges map not initialized")
	}
	if dg.cacheValid {
		t.Error("cacheValid should be false initially")
	}
	if len(dg.edges) != 0 {
		t.Error("edges should be empty initially")
	}
}

func TestAddEdge(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()

	// Test adding first edge
	dg.AddEdge(tid1, tid2)

	if len(dg.edges) != 1 {
		t.Errorf("Expected 1 waiter, got %d", len(dg.edges))
	}
	if len(dg.edges[tid1]) != 1 {
		t.Errorf("Expected 1 holder for tid1, got %d", len(dg.edges[tid1]))
	}
	if !dg.edges[tid1][tid2] {
		t.Error("Edge from tid1 to tid2 not found")
	}
	if dg.cacheValid {
		t.Error("cacheValid should be false after adding edge")
	}

	// Test adding second edge from same waiter
	tid3 := primitives.NewTransactionID()
	dg.AddEdge(tid1, tid3)

	if len(dg.edges[tid1]) != 2 {
		t.Errorf("Expected 2 holders for tid1, got %d", len(dg.edges[tid1]))
	}
	if !dg.edges[tid1][tid3] {
		t.Error("Edge from tid1 to tid3 not found")
	}

	// Test adding duplicate edge
	dg.AddEdge(tid1, tid2)
	if len(dg.edges[tid1]) != 2 {
		t.Error("Duplicate edge should not increase count")
	}
}

func TestRemoveTransaction(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()

	// Setup: tid1 -> tid2, tid2 -> tid3, tid3 -> tid1
	dg.AddEdge(tid1, tid2)
	dg.AddEdge(tid2, tid3)
	dg.AddEdge(tid3, tid1)

	// Remove tid2 (both as waiter and holder)
	dg.RemoveTransaction(tid2)

	// tid1 -> tid2 should be gone
	if _, exists := dg.edges[tid1][tid2]; exists {
		t.Error("Edge from tid1 to tid2 should be removed")
	}
	// tid2 -> tid3 should be gone (tid2 as waiter)
	if _, exists := dg.edges[tid2]; exists {
		t.Error("tid2 should not exist as waiter")
	}
	// tid3 -> tid1 should remain
	if !dg.edges[tid3][tid1] {
		t.Error("Edge from tid3 to tid1 should remain")
	}
	if dg.cacheValid {
		t.Error("cacheValid should be false after removing transaction")
	}
}

func TestHasCycleSimple(t *testing.T) {
	dg := NewDependencyGraph()

	// No cycle initially
	if dg.HasCycle() {
		t.Error("Empty graph should not have cycle")
	}

	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()

	// Add single edge: tid1 -> tid2
	dg.AddEdge(tid1, tid2)
	if dg.HasCycle() {
		t.Error("Single edge should not create cycle")
	}

	// Create cycle: tid2 -> tid1
	dg.AddEdge(tid2, tid1)
	if !dg.HasCycle() {
		t.Error("Two-node cycle should be detected")
	}
}

func TestHasCycleComplex(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()
	tid4 := primitives.NewTransactionID()

	// Create chain: tid1 -> tid2 -> tid3 -> tid4
	dg.AddEdge(tid1, tid2)
	dg.AddEdge(tid2, tid3)
	dg.AddEdge(tid3, tid4)

	if dg.HasCycle() {
		t.Error("Chain should not have cycle")
	}

	// Create cycle: tid4 -> tid2
	dg.AddEdge(tid4, tid2)
	if !dg.HasCycle() {
		t.Error("Complex cycle should be detected")
	}

	// Remove edge to break cycle
	dg.RemoveTransaction(tid4)
	if dg.HasCycle() {
		t.Error("Cycle should be broken after removing transaction")
	}
}

func TestHasCycleCache(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()

	// First call should set cache
	result1 := dg.HasCycle()
	if !dg.cacheValid {
		t.Error("Cache should be valid after first HasCycle call")
	}
	if dg.wasCycleLastTime != result1 {
		t.Error("Cached result should match returned result")
	}

	// Second call should use cache
	result2 := dg.HasCycle()
	if result1 != result2 {
		t.Error("Cached result should be consistent")
	}

	// Adding edge should invalidate cache
	dg.AddEdge(tid1, tid2)
	if dg.cacheValid {
		t.Error("Cache should be invalidated after adding edge")
	}
}

func TestGetWaitingTransactions(t *testing.T) {
	dg := NewDependencyGraph()

	// Empty graph
	waiters := dg.GetWaitingTransactions()
	if len(waiters) != 0 {
		t.Error("Empty graph should have no waiters")
	}

	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()

	// Add edges: tid1 -> tid2, tid3 -> tid1
	dg.AddEdge(tid1, tid2)
	dg.AddEdge(tid3, tid1)

	waiters = dg.GetWaitingTransactions()
	if len(waiters) != 2 {
		t.Errorf("Expected 2 waiters, got %d", len(waiters))
	}

	// Check that both tid1 and tid3 are in waiters
	waiterMap := make(map[*primitives.TransactionID]bool)
	for _, w := range waiters {
		waiterMap[w] = true
	}
	if !waiterMap[tid1] {
		t.Error("tid1 should be in waiters")
	}
	if !waiterMap[tid3] {
		t.Error("tid3 should be in waiters")
	}
	if waiterMap[tid2] {
		t.Error("tid2 should not be in waiters (only a holder)")
	}
}

func TestConcurrentAccess(t *testing.T) {
	dg := NewDependencyGraph()
	var wg sync.WaitGroup

	// Create multiple transactions
	transactions := make([]*primitives.TransactionID, 10)
	for i := range transactions {
		transactions[i] = primitives.NewTransactionID()
	}

	// Concurrent edge additions
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				dg.AddEdge(transactions[i], transactions[(i+1)%10])
			}
		}(i)
	}

	// Concurrent cycle detection
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				dg.HasCycle()
			}
		}()
	}

	// Concurrent transaction removal
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()
			dg.RemoveTransaction(transactions[i*3])
		}(i)
	}

	wg.Wait()

	// Graph should still be in valid state
	waiters := dg.GetWaitingTransactions()
	if waiters == nil {
		t.Error("GetWaitingTransactions should not return nil")
	}
}

func TestSelfLoop(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()

	// Add self-loop
	dg.AddEdge(tid1, tid1)

	if !dg.HasCycle() {
		t.Error("Self-loop should create cycle")
	}
}

func TestMultipleDisconnectedCycles(t *testing.T) {
	dg := NewDependencyGraph()

	// First cycle: tid1 -> tid2 -> tid1
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	dg.AddEdge(tid1, tid2)
	dg.AddEdge(tid2, tid1)

	// Second cycle: tid3 -> tid4 -> tid3
	tid3 := primitives.NewTransactionID()
	tid4 := primitives.NewTransactionID()
	dg.AddEdge(tid3, tid4)
	dg.AddEdge(tid4, tid3)

	if !dg.HasCycle() {
		t.Error("Graph with multiple disconnected cycles should detect cycle")
	}

	// Remove one cycle
	dg.RemoveTransaction(tid1)
	dg.RemoveTransaction(tid2)

	if !dg.HasCycle() {
		t.Error("Should still detect remaining cycle")
	}

	// Remove second cycle
	dg.RemoveTransaction(tid3)
	dg.RemoveTransaction(tid4)

	if dg.HasCycle() {
		t.Error("No cycles should remain")
	}
}

func TestLargeGraph(t *testing.T) {
	dg := NewDependencyGraph()
	transactions := make([]*primitives.TransactionID, 100)

	// Create 100 transactions
	for i := range transactions {
		transactions[i] = primitives.NewTransactionID()
	}

	// Create a long chain
	for i := 0; i < 99; i++ {
		dg.AddEdge(transactions[i], transactions[i+1])
	}

	if dg.HasCycle() {
		t.Error("Chain should not have cycle")
	}

	// Close the loop to create cycle
	dg.AddEdge(transactions[99], transactions[0])

	if !dg.HasCycle() {
		t.Error("Large cycle should be detected")
	}

	// Remove one transaction to break cycle
	dg.RemoveTransaction(transactions[50])

	if dg.HasCycle() {
		t.Error("Cycle should be broken")
	}
}

func TestEmptyHoldersCleanup(t *testing.T) {
	dg := NewDependencyGraph()
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	tid3 := primitives.NewTransactionID()

	// Setup: tid1 -> tid2, tid1 -> tid3
	dg.AddEdge(tid1, tid2)
	dg.AddEdge(tid1, tid3)

	// Remove tid2
	dg.RemoveTransaction(tid2)

	// tid1 should still exist with one holder
	if len(dg.edges[tid1]) != 1 {
		t.Errorf("Expected 1 holder for tid1, got %d", len(dg.edges[tid1]))
	}

	// Remove tid3
	dg.RemoveTransaction(tid3)

	// tid1 should be removed completely since it has no holders
	if _, exists := dg.edges[tid1]; exists {
		t.Error("tid1 should be removed when it has no holders")
	}
}
