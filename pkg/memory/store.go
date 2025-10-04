package memory

import (
	"fmt"
	"storemy/pkg/concurrency/lock"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

const (
	MaxPageCount = 50
)

// Permissions represents the access level for database operations
type Permissions int

const (
	ReadOnly Permissions = iota
	ReadWrite
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
	// transactions map[*transaction.TransactionID]*TransactionInfo
	lockManager *lock.LockManager
	cache       PageCache
	wal         *log.WAL // Write-Ahead Log for durability and recovery
	txManager   *TransactionManager
}

// NewPageStore creates and initializes a new PageStore instance with the given TableManager and WAL.
// The PageStore will use the TableManager to access database files and manage table operations.
// walPath specifies the location of the write-ahead log file.
// bufferSize determines the WAL buffer size in bytes (e.g., 8192 for 8KB buffer).
func NewPageStore(tm *TableManager, walPath string, bufferSize int) (*PageStore, error) {
	wal, err := log.NewWAL(walPath, bufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}

	return &PageStore{
		cache:        NewLRUPageCache(MaxPageCount),
		txManager:    NewTransactionManager(wal),
		lockManager:  lock.NewLockManager(),
		tableManager: tm,
		wal:          wal,
	}, nil
}

// GetPage retrieves a page with specified permissions for a transaction
// This is the main entry point for all page access in the database
func (p *PageStore) GetPage(tid *transaction.TransactionID, pid tuple.PageID, perm Permissions) (page.Page, error) {
	if err := p.lockManager.LockPage(tid, pid, perm == ReadWrite); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}

	p.trackPageAccess(tid, pid, perm)
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

	dbFile, err := p.tableManager.GetDbFile(pid.GetTableID())
	if err != nil {
		return nil, fmt.Errorf("table with ID %d not found: %v", pid.GetTableID(), err)
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

func (p *PageStore) trackPageAccess(tid *transaction.TransactionID, pid tuple.PageID, perm Permissions) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, exists := p.transactions[tid]; !exists {
		p.transactions[tid] = &TransactionInfo{
			startTime:   time.Now(),
			dirtyPages:  make(map[tuple.PageID]bool),
			lockedPages: make(map[tuple.PageID]Permissions),
			hasBegun:    false,
		}
	}

	p.transactions[tid].lockedPages[pid] = perm
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
// Following the Write-Ahead Logging protocol:
// 1. Log BEGIN if this is the first operation
// 2. Log INSERT record with after-image BEFORE modifying the page
// 3. Perform the actual insertion
// 4. Mark pages as dirty
func (p *PageStore) InsertTuple(tid *transaction.TransactionID, tableID int, t *tuple.Tuple) error {
	return p.performDataOperation(InsertOperation, tid, tableID, t, nil)

}

// DeleteTuple removes a tuple from its table within the given transaction context.
// The tuple must have a valid RecordID indicating its location in the database.
// Following the Write-Ahead Logging protocol:
// 1. Log BEGIN if this is the first operation
// 2. Capture before-image of the page
// 3. Log DELETE record with before-image BEFORE modifying the page
// 4. Perform the actual deletion
// 5. Mark pages as dirty
func (p *PageStore) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) error {
	if t.RecordID == nil {
		return fmt.Errorf("tuple must have a valid record ID")
	}

	tableID := t.RecordID.PageID.GetTableID()
	return p.performDataOperation(DeleteOperation, tid, tableID, t, nil)
}

func (p *PageStore) performDataOperation(operation OperationType, tid *transaction.TransactionID, tableID int, t *tuple.Tuple, beforeImage []byte) error {
	if err := p.txManager.EnsureBegun(tid); err != nil {
		return err
	}

	dbFile, err := p.tableManager.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("table with ID %d not found: %v", tableID, err)
	}

	var modifiedPages []page.Page
	switch operation {
	case InsertOperation:
		modifiedPages, err = dbFile.AddTuple(tid, t)
		if err != nil {
			return fmt.Errorf("failed to add tuple: %v", err)
		}

		for _, pg := range modifiedPages {
			if err := p.logOperation(InsertOperation, tid, pg.GetID(), pg.GetPageData()); err != nil {
				return err
			}
		}

	case DeleteOperation:
		pageID := t.RecordID.PageID
		pg, err := p.GetPage(tid, pageID, ReadWrite)
		if err != nil {
			return fmt.Errorf("failed to get page for delete: %v", err)
		}

		if err := p.logOperation(DeleteOperation, tid, pageID, pg.GetPageData()); err != nil {
			return err
		}

		modifiedPage, err := dbFile.DeleteTuple(tid, t)
		if err != nil {
			return fmt.Errorf("failed to delete tuple: %v", err)
		}
		modifiedPages = []page.Page{modifiedPage}
	}

	p.markPagesAsDirty(tid, modifiedPages)

	return nil
}

// UpdateTuple replaces an existing tuple with a new version within the given transaction.
// Following the Write-Ahead Logging protocol:
// 1. Log BEGIN if this is the first operation
// 2. Capture before-image of the page
// 3. Perform the update (delete + insert)
// 4. Capture after-image
// 5. Log UPDATE record with both images
// 6. Mark pages as dirty
//
// Note: This is implemented as delete followed by insert, so the individual operations
// will log their own records. In a more optimized implementation, we'd log a single UPDATE.
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
//
// Commit Process (WAL-enhanced):
// 1. Validate transaction exists
// 2. Log COMMIT record to WAL
// 3. Force WAL to disk (CRITICAL - ensures durability even before pages are written)
// 4. Update before-images for committed pages (new baseline for future aborts)
// 5. Flush all dirty pages to disk (FORCE policy)
// 6. Mark pages as clean
// 7. Remove transaction from active tracking
// 8. Release all locks
//
// WAL Protocol: The commit is durable once the COMMIT record is forced to disk,
// even if the system crashes before pages are flushed. Recovery will REDO the changes.
//
// Parameters:
//   - tid: The transaction ID to commit (must not be nil)
//
// Returns:
//   - error: nil on successful commit, or error if WAL or flush fails
func (p *PageStore) CommitTransaction(tid *transaction.TransactionID) error {
	if tid == nil {
		return fmt.Errorf("transaction ID cannot be nil")
	}

	p.mutex.Lock()
	txInfo, exists := p.transactions[tid]
	if !exists {
		p.mutex.Unlock()
		p.lockManager.UnlockAllPages(tid)
		return nil
	}

	dirtyPageIDs := make([]tuple.PageID, 0, len(txInfo.dirtyPages))
	for pid := range txInfo.dirtyPages {
		dirtyPageIDs = append(dirtyPageIDs, pid)
	}
	p.mutex.Unlock()

	if txInfo.hasBegun {
		_, err := p.wal.LogCommit(tid)
		if err != nil {
			return fmt.Errorf("commit failed: unable to log COMMIT record: %v", err)
		}
	}

	p.mutex.Lock()
	for _, pid := range dirtyPageIDs {
		page, exists := p.cache.Get(pid)
		if exists {
			page.SetBeforeImage()
			p.cache.Put(pid, page)
		}
	}
	p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		if err := p.flushPage(pid); err != nil {
			return fmt.Errorf("commit failed: unable to flush page %v: %v (transaction must be aborted)", pid, err)
		}
	}

	p.mutex.Lock()
	p.txManager.Remove(tid)
	p.mutex.Unlock()

	p.lockManager.UnlockAllPages(tid)
	return nil
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism with Write-Ahead Logging protocol.
//
// Abort Process (WAL-enhanced):
// 1. Validate transaction exists
// 2. Log ABORT record to WAL (marks transaction as aborted)
// 3. Restore all dirty pages to their before-image state (undo changes in memory)
// 4. Discard dirty pages from cache (changes never hit disk)
// 5. Remove transaction from active tracking
// 6. Release all locks
//
// WAL Protocol: The ABORT record allows recovery to know this transaction should be undone.
// NO-STEAL Policy: Since dirty pages are never evicted, all changes exist only in memory,
// making abort simple - just restore from before-images in the cache.
//
// Parameters:
//   - tid: The transaction ID to abort (must not be nil)
//
// Returns:
//   - error: Returns error if WAL logging fails, nil otherwise
func (p *PageStore) AbortTransaction(tid *transaction.TransactionID) error {
	if tid == nil {
		return fmt.Errorf("transaction ID cannot be nil")
	}

	p.mutex.Lock()
	txInfo, exists := p.transactions[tid]
	if !exists {
		p.mutex.Unlock()
		p.lockManager.UnlockAllPages(tid)
		return nil
	}

	if txInfo.hasBegun {
		p.mutex.Unlock()
		if _, err := p.wal.LogAbort(tid); err != nil {
			return fmt.Errorf("failed to log ABORT to WAL: %v", err)
		}
		p.mutex.Lock()
	}

	for pid := range txInfo.dirtyPages {
		page, pageExists := p.cache.Get(pid)
		if !pageExists {
			continue
		}

		beforeImage := page.GetBeforeImage()
		if beforeImage == nil {
			fmt.Printf("Warning: no before-image for page %v during abort of transaction %v\n",
				pid, tid)
			p.cache.Remove(pid)
			continue
		}

		p.cache.Put(pid, beforeImage)
	}

	p.txManager.Remove(tid)
	p.mutex.Unlock()
	p.lockManager.UnlockAllPages(tid)

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

	dbFile, err := p.tableManager.GetDbFile(pid.GetTableID())
	if err != nil {
		return fmt.Errorf("table for page %v not found: %v", pid, err)
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

func (p *PageStore) markPagesAsDirty(tid *transaction.TransactionID, pages []page.Page) {
	txInfo := p.txManager.GetOrCreate(tid)

	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, pg := range pages {
		pg.MarkDirty(true, tid)
		p.cache.Put(pg.GetID(), pg)
		txInfo.dirtyPages[pg.GetID()] = true
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

func (p *PageStore) logOperation(operation OperationType, tid *transaction.TransactionID, pageID tuple.PageID, data []byte) error {
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
		return fmt.Errorf("unknown operation: %s", operation)
	}

	if err != nil {
		return fmt.Errorf("failed to log %s to WAL: %v", operation, err)
	}
	return nil
}
