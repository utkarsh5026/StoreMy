package lock

import (
	"storemy/pkg/concurrency/transaction"
	"sync"
)

// DependencyGraph tracks wait-for relationships for deadlock detection
type DependencyGraph struct {
	edges      map[*transaction.TransactionID]map[*transaction.TransactionID]bool
	mutex      sync.RWMutex
	cacheValid bool // Track if cycle cache is valid
	lastResult bool // Cache the last cycle detection result
}

func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		edges:      make(map[*transaction.TransactionID]map[*transaction.TransactionID]bool),
		cacheValid: false,
	}
}

func (dg *DependencyGraph) AddEdge(waiter, holder *transaction.TransactionID) {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	if dg.edges[waiter] == nil {
		dg.edges[waiter] = make(map[*transaction.TransactionID]bool)
	}
	dg.edges[waiter][holder] = true
	dg.cacheValid = false
}

func (dg *DependencyGraph) RemoveTransaction(tid *transaction.TransactionID) {
	dg.mutex.Lock()
	defer dg.mutex.Unlock()

	// Remove as waiter
	delete(dg.edges, tid)

	for waiter, holders := range dg.edges {
		delete(holders, tid)
		if len(holders) == 0 {
			delete(dg.edges, waiter)
		}
	}
	dg.cacheValid = false
}

func (dg *DependencyGraph) HasCycle() bool {
	dg.mutex.RLock()
	defer dg.mutex.RUnlock()

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
				return true // Cycle detected
			}
		}
	}

	recStack[tid] = false
	return false
}

func (dg *DependencyGraph) GetWaitingTransactions() []*transaction.TransactionID {
	dg.mutex.RLock()
	defer dg.mutex.RUnlock()

	var waiters []*transaction.TransactionID
	for tid := range dg.edges {
		waiters = append(waiters, tid)
	}
	return waiters
}
