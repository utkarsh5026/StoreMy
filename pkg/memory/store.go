package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

type Permissions int

const (
	ReadOnly Permissions = iota
	ReadWrite
)

type TransactionInfo struct {
	startTime   time.Time
	dirtyPages  map[tuple.PageID]bool
	lockedPages map[tuple.PageID]Permissions
}

type PageStore struct {
	numPages     int
	pageCache    map[tuple.PageID]page.Page
	tableManager *TableManager
	mutex        sync.RWMutex
	transactions map[*transaction.TransactionID]*TransactionInfo
}

func NewPageStore(tm *TableManager) *PageStore {
	return &PageStore{
		pageCache:    make(map[tuple.PageID]page.Page),
		transactions: make(map[*transaction.TransactionID]*TransactionInfo),
		tableManager: tm,
	}
}

func (p *PageStore) InsertTuple(tid *transaction.TransactionID, tableID int, t *tuple.Tuple) error {
	dbFile, err := p.tableManager.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("table with ID %d not found", tableID)
	}

	modifiedPages, err := dbFile.AddTuple(tid, t)
	if err != nil {
		return fmt.Errorf("failed to add tuple: %v", err)
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, page := range modifiedPages {
		page.MarkDirty(true, tid)
		p.pageCache[page.GetID()] = page

		if txInfo, exists := p.transactions[tid]; exists {
			txInfo.dirtyPages[page.GetID()] = true
		}
	}
	return nil
}
