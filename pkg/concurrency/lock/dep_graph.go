package lock

import (
	"storemy/pkg/concurrency/transaction"
	"sync"
)

// DependencyGraph tracks wait-for relationships between transactions for deadlock detection.
// It maintains a directed graph where edges represent "waits-for" relationships - if transaction A
// is waiting for a lock held by transaction B, there will be an edge from A to B.
//
// The graph is used by the deadlock detection system to identify circular dependencies
// that indicate deadlocks. When a cycle is detected, one of the transactions in the
// cycle must be aborted to break the deadlock.
type DependencyGraph struct {
	edges      map[*transaction.TransactionID]map[*transaction.TransactionID]bool // Adjacency list representation of the graph
	mutex      sync.RWMutex                                                       // Protects concurrent access to the graph
	cacheValid bool                                                               // Track if cycle cache is valid
	lastResult bool                                                               // Cache the last cycle detection result
}

// NewDependencyGraph creates and initializes a new dependency graph for deadlock detection.
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		edges:      make(map[*transaction.TransactionID]map[*transaction.TransactionID]bool),
		cacheValid: false,
	}
}

// AddEdge creates a wait-for relationship in the dependency graph.
// This indicates that the waiter transaction is blocked waiting for a resource
// held by the holder transaction.
func (dg *DependencyGraph) AddEdge(waiter, holder *transaction.TransactionID) {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	if dg.edges[waiter] == nil {
		dg.edges[waiter] = make(map[*transaction.TransactionID]bool)
	}
	dg.edges[waiter][holder] = true
	dg.cacheValid = false
}

// RemoveTransaction completely removes a transaction from the dependency graph.
// This removes all edges where the transaction appears as either a waiter or holder,
// effectively cleaning up all wait-for relationships involving the transaction.
func (dg *DependencyGraph) RemoveTransaction(tid *transaction.TransactionID) {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	delete(dg.edges, tid)
	for waiter, holders := range dg.edges {
		delete(holders, tid)
		if len(holders) == 0 {
			delete(dg.edges, waiter)
		}
	}
	dg.cacheValid = false
}

// HasCycle detects whether the dependency graph contains a cycle, which indicates
// a deadlock condition. The method uses depth-first search with a recursion stack
// to efficiently detect cycles in the directed graph.
//
// The result is cached to avoid redundant computation when the graph hasn't changed.
// The cache is invalidated whenever edges are added or removed.
//
// Returns true if a cycle (deadlock) is detected, false otherwise.
func (dg *DependencyGraph) HasCycle() bool {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	if dg.cacheValid {
		return dg.lastResult
	}

	visited := make(map[*transaction.TransactionID]bool)
	recStack := make(map[*transaction.TransactionID]bool)

	for tid := range dg.edges {
		if !visited[tid] {
			if dg.hasCycleDFS(tid, visited, recStack) {
				dg.lastResult = true
				dg.cacheValid = true
				return true
			}
		}
	}

	dg.lastResult = false
	dg.cacheValid = true
	return false
}

// hasCycleDFS performs depth-first search to detect cycles in the dependency graph.
// It uses a recursion stack to track the current path and detect back edges,
// which indicate cycles.
func (dg *DependencyGraph) hasCycleDFS(tid *transaction.TransactionID, visited, recStack map[*transaction.TransactionID]bool) bool {
	visited[tid] = true
	recStack[tid] = true

	if neighbors, exists := dg.edges[tid]; exists {
		for neighbor := range neighbors {
			if !visited[neighbor] {
				if dg.hasCycleDFS(neighbor, visited, recStack) {
					return true
				}
			} else if recStack[neighbor] {
				return true
			}
		}
	}

	recStack[tid] = false
	return false
}

// GetWaitingTransactions returns a list of all transactions that are currently
// waiting for resources held by other transactions. These are the transactions
// that have outgoing edges in the dependency graph.
func (dg *DependencyGraph) GetWaitingTransactions() []*transaction.TransactionID {
	dg.mutex.RLock()
	defer dg.mutex.RUnlock()

	var waiters []*transaction.TransactionID
	for tid := range dg.edges {
		waiters = append(waiters, tid)
	}
	return waiters
}
