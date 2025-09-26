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

func (p *PageStore) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) error {
	if t == nil {
		return fmt.Errorf("tuple cannot be nil")
	}
	if t.RecordID == nil {
		return fmt.Errorf("tuple has no record ID")
	}

	tableID := t.RecordID.PageID.GetTableID()
	dbFile, err := p.tableManager.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("table with ID %d not found", tableID)
	}

	modifiedPage, err := dbFile.DeleteTuple(tid, t)
	if err != nil {
		return fmt.Errorf("failed to delete tuple: %v", err)
	}

	modifiedPage.MarkDirty(true, tid)
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.pageCache[modifiedPage.GetID()] = modifiedPage
	if txInfo, exists := p.transactions[tid]; exists {
		txInfo.dirtyPages[modifiedPage.GetID()] = true
	}

	return nil
}

func (p *PageStore) UpdateTuple(tid *transaction.TransactionID, oldTuple *tuple.Tuple, newTuple *tuple.Tuple) error {
	if err := p.DeleteTuple(tid, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %v", err)
	}

	tableID := oldTuple.RecordID.PageID.GetTableID()
	if err := p.InsertTuple(tid, tableID, newTuple); err != nil {
		p.InsertTuple(tid, tableID, oldTuple)
		return fmt.Errorf("failed to insert updated tuple: %v", err)
	}

	return nil
}
