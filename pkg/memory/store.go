package memory

import (
	"fmt"
	"storemy/pkg/concurrency/lock"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"sync"
)

const (
	MaxPageCount = 50
)

type OperationType uint8

const (
	InsertOperation OperationType = iota
	DeleteOperation
	UpdateOperation
	CommitOperation
	AbortOperation
)

func (o OperationType) String() string {
	switch o {
	case InsertOperation:
		return "INSERT"
	case DeleteOperation:
		return "DELETE"
	case UpdateOperation:
		return "UPDATE"
	case CommitOperation:
		return "COMMIT"
	case AbortOperation:
		return "ABORT"
	}
	return "UNKNOWN"
}

// PageStore manages an in-memory cache of database pages and handles transaction-aware page operations.
// It serves as the main interface between the database engine and the underlying storage layer,
// providing ACID compliance through transaction tracking and page lifecycle management.
type PageStore struct {
	tableManager *TableManager
	mutex        sync.RWMutex
	lockManager  *lock.LockManager
	cache        PageCache
	wal          *log.WAL
}

// NewPageStore creates and initializes a new PageStore instance
func NewPageStore(tm *TableManager, wal *log.WAL) *PageStore {
	return &PageStore{
		cache:        NewLRUPageCache(MaxPageCount),
		lockManager:  lock.NewLockManager(),
		tableManager: tm,
		wal:          wal,
	}
}

// GetPage retrieves a page with specified permissions for a transaction
// This is the main entry point for all page access in the database
func (p *PageStore) GetPage(ctx *transaction.TransactionContext, pid tuple.PageID, perm transaction.Permissions) (page.Page, error) {
	tid := ctx.ID
	if err := p.lockManager.LockPage(tid, pid, perm == transaction.ReadWrite); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}

	ctx.RecordPageAccess(pid, perm)
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if page, exists := p.cache.Get(pid); exists {
		return page, nil
	}

	if p.cache.Size() >= MaxPageCount {
		if err := p.evictPage(); err != nil {
			return nil, fmt.Errorf("buffer pool full, cannot evict: %v", err)
		}
	}

	dbFile, err := p.getDbFile(pid.GetTableID())
	if err != nil {
		return nil, err
	}

	page, err := dbFile.ReadPage(pid)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %v", err)
	}

	if err := p.cache.Put(pid, page); err != nil {
		return nil, fmt.Errorf("failed to add page to cache: %v", err)
	}

	return page, nil
}

// evictPage implements NO-STEAL policy
// We never evict dirty pages to simplify recovery
func (p *PageStore) evictPage() error {
	allPages := p.cache.GetAll()

	for _, pid := range allPages {
		page, exists := p.cache.Get(pid)
		if !exists {
			continue
		}

		if page.IsDirty() != nil {
			continue
		}

		if p.lockManager.IsPageLocked(pid) {
			continue
		}
		p.cache.Remove(pid)
		return nil
	}

	return fmt.Errorf("all pages are dirty or locked, cannot evict (NO-STEAL policy)")
}

// InsertTuple adds a new tuple to the specified table within the given transaction context.
func (p *PageStore) InsertTuple(ctx *transaction.TransactionContext, tableID int, t *tuple.Tuple) error {
	return p.performDataOperation(InsertOperation, ctx, tableID, t)
}

// DeleteTuple removes a tuple from its table within the given transaction context.
func (p *PageStore) DeleteTuple(ctx *transaction.TransactionContext, t *tuple.Tuple) error {
	if t == nil {
		return fmt.Errorf("tuple cannot be nil")
	}

	if t.RecordID == nil {
		return fmt.Errorf("tuple must have a valid record ID")
	}

	tableID := t.RecordID.PageID.GetTableID()
	return p.performDataOperation(DeleteOperation, ctx, tableID, t)
}

func (p *PageStore) performDataOperation(operation OperationType, ctx *transaction.TransactionContext, tableID int, t *tuple.Tuple) error {
	if err := ctx.EnsureBegunInWAL(p.wal); err != nil {
		return err
	}

	dbFile, err := p.getDbFile(tableID)
	if err != nil {
		return err
	}

	var modifiedPages []page.Page
	switch operation {
	case InsertOperation:
		modifiedPages, err = p.handleInsert(ctx, t, dbFile)

	case DeleteOperation:
		modifiedPages, err = p.handleDelete(ctx, t, dbFile)

	default:
		return fmt.Errorf("unsupported operation: %s", operation.String())
	}

	if err != nil {
		return err
	}

	p.markPagesAsDirty(ctx, modifiedPages)
	return nil
}

func (p *PageStore) handleInsert(ctx *transaction.TransactionContext, t *tuple.Tuple, dbFile page.DbFile) ([]page.Page, error) {
	modifiedPages, err := dbFile.AddTuple(ctx.ID, t)
	if err != nil {
		return nil, fmt.Errorf("failed to add tuple: %v", err)
	}

	for _, pg := range modifiedPages {
		if err := p.logOperation(InsertOperation, ctx.ID, pg.GetID(), pg.GetPageData()); err != nil {
			return nil, err
		}
	}
	return modifiedPages, nil
}

func (p *PageStore) handleDelete(ctx *transaction.TransactionContext, t *tuple.Tuple, dbFile page.DbFile) ([]page.Page, error) {
	pageID := t.RecordID.PageID
	pg, err := p.GetPage(ctx, pageID, transaction.ReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to get page for delete: %v", err)
	}

	if err := p.logOperation(DeleteOperation, ctx.ID, pageID, pg.GetPageData()); err != nil {
		return nil, err
	}

	modifiedPage, err := dbFile.DeleteTuple(ctx.ID, t)
	if err != nil {
		return nil, fmt.Errorf("failed to delete tuple: %v", err)
	}

	return []page.Page{modifiedPage}, nil
}

// UpdateTuple replaces an existing tuple with a new version within the given transaction.
func (p *PageStore) UpdateTuple(ctx *transaction.TransactionContext, oldTuple *tuple.Tuple, newTuple *tuple.Tuple) error {
	if oldTuple.RecordID == nil {
		return fmt.Errorf("old tuple has no RecordID")
	}

	tableID := oldTuple.RecordID.PageID.GetTableID()

	if err := p.DeleteTuple(ctx, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %v", err)
	}

	if err := p.InsertTuple(ctx, tableID, newTuple); err != nil {
		p.InsertTuple(ctx, tableID, oldTuple)
		return fmt.Errorf("failed to insert updated tuple: %v", err)
	}

	return nil
}

// FlushAllPages writes all dirty pages in the cache to persistent storage.
// This operation ensures data durability by synchronizing the in-memory cache
// with the underlying database files. Pages are unmarked as dirty after successful writes.
func (p *PageStore) FlushAllPages() error {
	p.mutex.RLock()
	pids := make([]tuple.PageID, 0, p.cache.Size())
	pids = append(pids, p.cache.GetAll()...)
	p.mutex.RUnlock()

	for _, pid := range pids {
		if err := p.flushPage(pid); err != nil {
			return fmt.Errorf("failed to flush page %v: %v", pid, err)
		}
	}

	return nil
}

// CommitTransaction finalizes all changes made by a transaction and makes them durable.
// This implements the commit phase with Write-Ahead Logging protocol.
func (p *PageStore) CommitTransaction(ctx *transaction.TransactionContext) error {
	return p.finalizeTransaction(ctx, CommitOperation)
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism with Write-Ahead Logging protocol.
func (p *PageStore) AbortTransaction(ctx *transaction.TransactionContext) error {
	return p.finalizeTransaction(ctx, AbortOperation)
}

func (p *PageStore) finalizeTransaction(ctx *transaction.TransactionContext, operation OperationType) error {
	if ctx == nil || ctx.ID == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	dirtyPageIDs := ctx.GetDirtyPages()
	if len(dirtyPageIDs) == 0 {
		p.lockManager.UnlockAllPages(ctx.ID)
		return nil
	}

	if err := p.logOperation(operation, ctx.ID, nil, nil); err != nil {
		return err
	}

	var err error
	switch operation {
	case CommitOperation:
		err = p.handleCommit(dirtyPageIDs)

	case AbortOperation:
		err = p.handleAbort(dirtyPageIDs)

	default:
		return fmt.Errorf("unknown operation: %s", operation.String())
	}

	if err != nil {
		return err
	}

	p.lockManager.UnlockAllPages(ctx.ID)
	return nil
}

func (p *PageStore) handleCommit(dirtyPageIDs []tuple.PageID) error {
	p.mutex.Lock()
	for _, pid := range dirtyPageIDs {
		if page, exists := p.cache.Get(pid); exists {
			page.SetBeforeImage()
			p.cache.Put(pid, page)
		}
	}
	p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		if err := p.flushPage(pid); err != nil {
			return fmt.Errorf("commit failed: unable to flush page %v: %v", pid, err)
		}
	}
	return nil
}

func (p *PageStore) handleAbort(dirtyPageIDs []tuple.PageID) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		page, exists := p.cache.Get(pid)
		if !exists {
			continue
		}

		if beforeImage := page.GetBeforeImage(); beforeImage != nil {
			p.cache.Put(pid, beforeImage)
		} else {
			p.cache.Remove(pid)
		}
	}
	return nil
}

// flushPage writes a specific page to disk if it has been modified (is dirty).
// This is an internal helper method used by FlushAllPages and other flush operations.
// The page is unmarked as dirty after a successful write operation.
//
// Returns an error if the page write fails, or nil if the page doesn't exist or isn't dirty.
func (p *PageStore) flushPage(pid tuple.PageID) error {
	p.mutex.RLock()
	page, exists := p.cache.Get(pid)
	p.mutex.RUnlock()

	if !exists {
		return nil
	}

	if page.IsDirty() == nil {
		return nil
	}

	dbFile, err := p.getDbFile(pid.GetTableID())
	if err != nil {
		return err
	}

	if err := dbFile.WritePage(page); err != nil {
		return fmt.Errorf("failed to write page to disk: %v", err)
	}
	page.MarkDirty(false, nil)

	p.mutex.Lock()
	p.cache.Put(pid, page)
	p.mutex.Unlock()

	return nil
}

func (p *PageStore) markPagesAsDirty(ctx *transaction.TransactionContext, pages []page.Page) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, pg := range pages {
		pg.MarkDirty(true, ctx.ID)
		p.cache.Put(pg.GetID(), pg)
		ctx.MarkPageDirty(pg.GetID())
	}
}

// Close gracefully shuts down the PageStore, flushing all pending data and closing the WAL.
// This method should be called when the database is shutting down.
func (p *PageStore) Close() error {
	if err := p.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages during shutdown: %v", err)
	}

	if err := p.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %v", err)
	}

	return nil
}

func (p *PageStore) logOperation(operation OperationType, tid *primitives.TransactionID, pageID tuple.PageID, data []byte) error {
	var err error
	switch operation {
	case InsertOperation:
		_, err = p.wal.LogInsert(tid, pageID, data)
	case DeleteOperation:
		_, err = p.wal.LogDelete(tid, pageID, data)
	case CommitOperation:
		_, err = p.wal.LogCommit(tid)
	case AbortOperation:
		_, err = p.wal.LogAbort(tid)
	default:
		return fmt.Errorf("unknown operation: %s", operation.String())
	}

	if err != nil {
		return fmt.Errorf("failed to log %s to WAL: %v", operation, err)
	}
	return nil
}

func (p *PageStore) getDbFile(tableID int) (page.DbFile, error) {
	dbFile, err := p.tableManager.GetDbFile(tableID)
	if err != nil {
		return nil, fmt.Errorf("table with ID %d not found: %v", tableID, err)
	}
	return dbFile, nil
}

// ensureTransactionBegun ensures that a transaction's BEGIN record has been logged to the WAL.
// This is a helper method for tests to directly ensure a transaction has begun.
func (p *PageStore) ensureTransactionBegun(ctx *transaction.TransactionContext) error {
	return ctx.EnsureBegunInWAL(p.wal)
}
