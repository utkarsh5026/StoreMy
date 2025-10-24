package transaction

import (
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/log/wal"
	"storemy/pkg/primitives"
	"sync"
)

// TransactionRegistry manages all active transaction contexts
// This is the single global registry that replaces scattered transaction maps
type TransactionRegistry struct {
	contexts map[*primitives.TransactionID]*TransactionContext
	mutex    sync.RWMutex
	walInstance      *wal.WAL
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry(w *wal.WAL) *TransactionRegistry {
	return &TransactionRegistry{
		contexts: make(map[*primitives.TransactionID]*TransactionContext),
		walInstance:      w,
	}
}

// Begin creates a new transaction context and registers it
func (tr *TransactionRegistry) Begin() (*TransactionContext, error) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	tr.mutex.Lock()
	tr.contexts[tid] = ctx
	tr.mutex.Unlock()

	return ctx, nil
}

// Get retrieves a transaction context by ID
func (tr *TransactionRegistry) Get(tid *primitives.TransactionID) (*TransactionContext, error) {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	ctx, exists := tr.contexts[tid]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", tid.String())
	}
	return ctx, nil
}

// GetOrCreate gets an existing context or creates a new one
func (tr *TransactionRegistry) GetOrCreate(tid *primitives.TransactionID) *TransactionContext {
	tr.mutex.Lock()
	defer tr.mutex.Unlock()

	ctx, exists := tr.contexts[tid]
	if exists {
		return ctx
	}

	ctx = NewTransactionContext(tid)
	tr.contexts[tid] = ctx
	return ctx
}

// Remove removes a transaction context from the registry
func (tr *TransactionRegistry) Remove(tid *primitives.TransactionID) {
	tr.mutex.Lock()
	defer tr.mutex.Unlock()
	delete(tr.contexts, tid)
}

// GetActive returns all active transaction contexts
func (tr *TransactionRegistry) GetActive() []*TransactionContext {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	return slices.DeleteFunc(slices.Collect(maps.Values(tr.contexts)), func(ctx *TransactionContext) bool {
		return !ctx.IsActive()
	})
}

// Count returns the number of registered transactions
func (tr *TransactionRegistry) Count() int {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()
	return len(tr.contexts)
}

// GetAllTransactionIDs returns all registered transaction IDs
func (tr *TransactionRegistry) GetAllTransactionIDs() []*primitives.TransactionID {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()
	return slices.Collect(maps.Keys(tr.contexts))
}
