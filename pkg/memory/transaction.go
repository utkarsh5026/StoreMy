package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

// TransactionInfo holds metadata about an active transaction.
// It tracks the transaction's lifecycle, dirty pages, and locked resources.
type TransactionInfo struct {
	startTime   time.Time                    // When the transaction was created
	dirtyPages  map[tuple.PageID]bool        // Pages modified by this transaction
	lockedPages map[tuple.PageID]Permissions // Pages locked by this transaction with their permission levels
	hasBegun    bool                         // Whether BEGIN has been logged to WAL
}

// TransactionManager manages the lifecycle and metadata of active transactions.
// It provides thread-safe access to transaction information and integrates
// with the Write-Ahead Log (WAL) for durability guarantees.
type TransactionManager struct {
	transactions map[*transaction.TransactionID]*TransactionInfo // Active transaction metadata
	wal          *log.WAL                                        // Write-Ahead Log for transaction logging
	mutex        sync.RWMutex                                    // Protects concurrent access to transactions map
}

// NewTransactionManager creates a new TransactionManager instance.
func NewTransactionManager(wal *log.WAL) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[*transaction.TransactionID]*TransactionInfo),
		wal:          wal,
	}
}

// GetOrCreate retrieves existing transaction info or creates new info for the given transaction ID.
func (tm *TransactionManager) GetOrCreate(tid *transaction.TransactionID) *TransactionInfo {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, exists := tm.transactions[tid]; !exists {
		tm.transactions[tid] = &TransactionInfo{
			startTime:   time.Now(),
			dirtyPages:  make(map[tuple.PageID]bool),
			lockedPages: make(map[tuple.PageID]Permissions),
		}
	}
	return tm.transactions[tid]
}

// EnsureBegun ensures that a transaction's BEGIN record has been logged to the WAL.
// This method is idempotent - it won't log BEGIN multiple times for the same transaction.
func (tm *TransactionManager) EnsureBegun(tid *transaction.TransactionID) error {
	txInfo := tm.GetOrCreate(tid)
	if txInfo.hasBegun {
		return nil
	}

	if _, err := tm.wal.LogBegin(tid); err != nil {
		return fmt.Errorf("failed to log transaction BEGIN: %v", err)
	}

	tm.mutex.Lock()
	txInfo.hasBegun = true
	tm.mutex.Unlock()
	return nil
}

// Remove removes a transaction's metadata from the manager.
// This should be called when a transaction commits or aborts to clean up resources.
func (tm *TransactionManager) Remove(tid *transaction.TransactionID) {
	tm.mutex.Lock()
	delete(tm.transactions, tid)
	tm.mutex.Unlock()
}

// GetDirtyPages returns a list of all pages that have been modified by the given transaction.
// This is used during transaction commit/abort to determine which pages need to be flushed
// or rolled back.
func (tm *TransactionManager) GetDirtyPages(tid *transaction.TransactionID) []tuple.PageID {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	txInfo, exists := tm.transactions[tid]
	if !exists {
		return nil
	}

	pages := make([]tuple.PageID, 0, len(txInfo.dirtyPages))
	for pid := range txInfo.dirtyPages {
		pages = append(pages, pid)
	}
	return pages
}
