package memory

import (
	"fmt"
	"storemy/pkg/concurrency/lock"
	"storemy/pkg/concurrency/transaction"
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

// TransactionInfo holds metadata about an active transaction
type TransactionInfo struct {
	startTime   time.Time                    // When the transaction started
	dirtyPages  map[tuple.PageID]bool        // Pages modified by this transaction
	lockedPages map[tuple.PageID]Permissions // Pages locked by this transaction with their permission level
}

// PageStore manages an in-memory cache of database pages and handles transaction-aware page operations.
// It serves as the main interface between the database engine and the underlying storage layer,
// providing ACID compliance through transaction tracking and page lifecycle management.
type PageStore struct {
	tableManager *TableManager
	mutex        sync.RWMutex
	transactions map[*transaction.TransactionID]*TransactionInfo
	lockManager  *lock.LockManager
	cache        PageCache
}

// NewPageStore creates and initializes a new PageStore instance with the given TableManager.
// The PageStore will use the TableManager to access database files and manage table operations.
func NewPageStore(tm *TableManager) *PageStore {
	return &PageStore{
		cache:        NewLRUPageCache(MaxPageCount),
		transactions: make(map[*transaction.TransactionID]*TransactionInfo),
		lockManager:  lock.NewLockManager(),
		tableManager: tm,
	}
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
// It handles page allocation, marks modified pages as dirty, and updates transaction metadata.
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
		p.cache.Put(page.GetID(), page)

		if txInfo, exists := p.transactions[tid]; exists {
			txInfo.dirtyPages[page.GetID()] = true
		}
	}
	return nil
}

// DeleteTuple removes a tuple from its table within the given transaction context.
// The tuple must have a valid RecordID indicating its location in the database.
// Modified pages are marked as dirty and tracked for the transaction.
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

	p.cache.Put(modifiedPage.GetID(), modifiedPage)
	if txInfo, exists := p.transactions[tid]; exists {
		txInfo.dirtyPages[modifiedPage.GetID()] = true
	}

	return nil
}

// UpdateTuple replaces an existing tuple with a new version within the given transaction.
// This operation is implemented as a delete followed by an insert. If the insertion fails,
// it attempts to restore the original tuple to maintain consistency.
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
//
// Returns an error if any page write operation fails.
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
// This implements the commit phase of two-phase locking protocol.
//
// Commit Process:
// 1. Validate transaction exists and has changes to commit
// 2. Flush all dirty pages to disk (FORCE policy for durability)
// 3. Update before-images for committed pages (new baseline for future aborts)
// 4. Mark pages as clean (no longer associated with this transaction)
// 5. Remove transaction from active tracking
// 6. Release all locks (allowing waiting transactions to proceed)
//
// FORCE Policy: We write all dirty pages before commit completes.
// This ensures durability but may impact performance.
//
// Parameters:
//   - tid: The transaction ID to commit (must not be nil)
//
// Returns:
//   - error: nil on successful commit, or error if flush fails
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
	// Set before images before flushing
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

	delete(p.transactions, tid)
	p.mutex.Unlock()

	p.lockManager.UnlockAllPages(tid)
	return nil
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism using before-images (STEAL policy support).
//
// Abort Process:
// 1. Validate transaction exists
// 2. Restore all dirty pages to their before-image state (undo changes)
// 3. Discard dirty pages from cache (changes never hit disk)
// 4. Remove transaction from active tracking
// 5. Release all locks (allowing waiting transactions to proceed)
//
// NO-STEAL Policy Note: We never evict dirty pages, so all changes exist only in memory.
// This makes abort simple - just restore from before-images in the cache.
//
// Parameters:
//   - tid: The transaction ID to abort (must not be nil)
//
// Returns:
//   - error: Always returns nil (abort cannot fail)
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

	delete(p.transactions, tid)
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
