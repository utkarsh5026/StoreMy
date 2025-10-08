package lock

import (
	"maps"
	"slices"
	"storemy/pkg/primitives"
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
	edges            map[*primitives.TransactionID]map[*primitives.TransactionID]bool
	mutex            sync.RWMutex
	cacheValid       bool
	wasCycleLastTime bool
}

// NewDependencyGraph creates and initializes a new dependency graph for deadlock detection.
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		edges:      make(map[*primitives.TransactionID]map[*primitives.TransactionID]bool),
		cacheValid: false,
	}
}

// AddEdge creates a wait-for relationship in the dependency graph.
// This indicates that the waiter transaction is blocked waiting for a resource
// held by the holder primitives.
func (dg *DependencyGraph) AddEdge(waiter, holder *primitives.TransactionID) {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	if dg.edges[waiter] == nil {
		dg.edges[waiter] = make(map[*primitives.TransactionID]bool)
	}
	dg.edges[waiter][holder] = true
	dg.cacheValid = false
}

// RemoveTransaction completely removes a transaction from the dependency graph.
// This removes all edges where the transaction appears as either a waiter or holder,
// effectively cleaning up all wait-for relationships involving the primitives.
func (dg *DependencyGraph) RemoveTransaction(tid *primitives.TransactionID) {
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
		return dg.wasCycleLastTime
	}

	visited := make(map[*primitives.TransactionID]bool)
	recStack := make(map[*primitives.TransactionID]bool)

	for tid := range dg.edges {
		if visited[tid] {
			continue
		}

		if dg.detectCycleDFS(tid, visited, recStack) {
			dg.wasCycleLastTime = true
			dg.cacheValid = true
			return true
		}
	}

	dg.wasCycleLastTime = false
	dg.cacheValid = true
	return false
}

// detectCycleDFS performs depth-first search to detect cycles in the dependency graph.
// It uses a recursion stack to track the current path and detect back edges,
// which indicate cycles.
func (dg *DependencyGraph) detectCycleDFS(tid *primitives.TransactionID, visited, recStack map[*primitives.TransactionID]bool) bool {
	visited[tid] = true
	recStack[tid] = true

	if neighbors, exists := dg.edges[tid]; exists {
		for neighbor := range neighbors {
			if !visited[neighbor] {
				if dg.detectCycleDFS(neighbor, visited, recStack) {
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
func (dg *DependencyGraph) GetWaitingTransactions() []*primitives.TransactionID {
	dg.mutex.RLock()
	defer dg.mutex.RUnlock()

	return slices.Collect(maps.Keys(dg.edges))
}
