package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

// TransactionInfo holds metadata about an active transaction
type TransactionInfo struct {
	startTime   time.Time
	dirtyPages  map[tuple.PageID]bool
	lockedPages map[tuple.PageID]Permissions
	hasBegun    bool // Whether BEGIN has been logged to WAL
}

type TransactionManager struct {
	transactions map[*transaction.TransactionID]*TransactionInfo
	wal          *log.WAL
	mutex        sync.RWMutex
}

func NewTransactionManager(wal *log.WAL) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[*transaction.TransactionID]*TransactionInfo),
		wal:          wal,
	}
}

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

func (tm *TransactionManager) Remove(tid *transaction.TransactionID) {
	tm.mutex.Lock()
	delete(tm.transactions, tid)
	tm.mutex.Unlock()
}

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
