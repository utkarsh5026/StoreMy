package memory

import (
	"fmt"
	"storemy/pkg/concurrency/lock"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"sync"
)

type TxContext = *transaction.TransactionContext

const (
	MaxPageCount = 1000
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
	default:
		return "UNKNOWN"
	}
}

// PageStore manages an in-memory cache of database pages and handles transaction-aware page operations.
// It serves as the main interface between the database engine and the underlying storage layer,
// providing ACID compliance through:
//   - Page-level locking via LockManager (2PL protocol)
//   - Write-Ahead Logging (WAL) for durability and recovery
//   - NO-STEAL buffer policy (dirty pages never evicted before commit)
//   - FORCE policy at commit (all dirty pages flushed to disk)
//   - MVCC support through before-images for transaction rollback
//
// The PageStore coordinates with:
//   - LockManager: Ensures serializable isolation through page locks
//   - WAL: Records all modifications for crash recovery
//   - PageCache: LRU cache for minimizing disk I/O
//   - DbFiles: Actual heap file access for page read/write
type PageStore struct {
	mutex       sync.RWMutex
	lockManager *lock.LockManager
	cache       PageCache
	wal         *wal.WAL
	dbFiles     map[int]page.DbFile // tableID -> DbFile mapping
}

// NewPageStore creates and initializes a new PageStore instance
func NewPageStore(wal *wal.WAL) *PageStore {
	return &PageStore{
		cache:       NewLRUPageCache(MaxPageCount),
		lockManager: lock.NewLockManager(),
		wal:         wal,
		dbFiles:     make(map[int]page.DbFile),
	}
}

// RegisterDbFile registers a DbFile for a specific table ID
// This allows PageStore to look up DbFiles when needed for flushing pages
func (p *PageStore) RegisterDbFile(tableID int, dbFile page.DbFile) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.dbFiles[tableID] = dbFile
}

// UnregisterDbFile removes a DbFile registration for a specific table ID
func (p *PageStore) UnregisterDbFile(tableID int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.dbFiles, tableID)
}

// GetPage retrieves a page with specified permissions for a transaction.
// This is the main entry point for all page access in the database, enforcing:
//   - Lock acquisition through LockManager (shared for READ, exclusive for READ_WRITE)
//   - Page loading from disk if not in cache
//   - LRU eviction when cache is full (respecting NO-STEAL policy)
//   - Transaction tracking of accessed pages for commit/abort
//
// Parameters:
//   - ctx: Transaction context (must not be nil)
//   - dbFile: Heap file containing the requested page
//   - pid: Page identifier (contains table ID and page number)
//   - perm: Access permissions (ReadOnly or ReadWrite)
func (p *PageStore) GetPage(ctx TxContext, dbFile page.DbFile, pid primitives.PageID, perm transaction.Permissions) (page.Page, error) {
	if ctx == nil {
		return nil, fmt.Errorf("transaction context cannot be nil")
	}

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

	page, err := dbFile.ReadPage(pid)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %v", err)
	}

	if err := p.cache.Put(pid, page); err != nil {
		return nil, fmt.Errorf("failed to add page to cache: %v", err)
	}

	return page, nil
}

func (p *PageStore) HandlePageChange(ctx TxContext, op OperationType, getDirtyPages func() ([]page.Page, error)) error {

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if ctx == nil || ctx.ID == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	dirtyPages, err := getDirtyPages()
	if err != nil {
		return fmt.Errorf("failed to get dirty pages: %v", err)
	}

	for _, pg := range dirtyPages {
		pid := pg.GetID()

		// Handle UpdateOperation specially - it needs both before and after images
		if op == UpdateOperation {
			var beforeImage []byte
			if beforePage := pg.GetBeforeImage(); beforePage != nil {
				beforeImage = beforePage.GetPageData()
			}
			afterImage := pg.GetPageData()

			if _, err := p.wal.LogUpdate(ctx.ID, pid, beforeImage, afterImage); err != nil {
				return fmt.Errorf("failed to log update operation: %v", err)
			}
		} else {
			if err := p.logOperation(op, ctx.ID, pid, pg.GetPageData()); err != nil {
				return fmt.Errorf("failed to log %s operation: %v", op, err)
			}
		}

		pg.MarkDirty(true, ctx.ID)
		p.cache.Put(pid, pg)
		ctx.MarkPageDirty(pid)
	}
	return nil
}

// evictPage implements NO-STEAL buffer management policy.
// This policy never evicts dirty pages - only clean pages that have been flushed
// to disk are eligible for eviction. This simplifies recovery at the cost of potentially
// blocking transactions when the buffer pool is full of dirty pages.
//
// Eviction algorithm:
//  1. Scan all cached pages
//  2. Skip dirty pages (modified but not committed)
//  3. Skip locked pages (currently in use by transactions)
//  4. Evict first clean, unlocked page found
//
// Returns an error if no pages can be evicted (all are dirty or locked).
// This forces transactions to commit or abort to free up buffer space.
//
// Called by GetPage when cache is at MaxPageCount capacity.
//
// Note: Caller must hold p.mutex lock.
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

// FlushAllPages writes all dirty pages to persistent storage.
// This operation ensures data durability by synchronizing the in-memory cache
// with the underlying database files. Pages are unmarked as dirty after successful writes.
//
// Used during:
//   - Database shutdown (ensure all changes persisted)
//   - Checkpoint operations (reduce recovery time)
//   - Manual flush commands
//
// Flush Order:
//   - No specific ordering (all dirty pages flushed)
//   - WAL must be synced before this call (enforced by caller)
//   - FORCE policy at commit ensures consistency
//
// Returns an error if any page write fails. Some pages may be flushed before
// the error occurs, leaving the cache in a partially flushed state.
//
// Thread-safe: Acquires read lock to snapshot page IDs, then processes each page.
func (p *PageStore) FlushAllPages() error {
	p.mutex.RLock()
	pids := make([]primitives.PageID, 0, p.cache.Size())
	pids = append(pids, p.cache.GetAll()...)
	p.mutex.RUnlock()

	for _, pid := range pids {
		dbFile, err := p.getDbFileForPage(pid)
		if err != nil {
			return fmt.Errorf("failed to get dbFile for page %v: %v", pid, err)
		}
		if err := p.flushPage(dbFile, pid); err != nil {
			return fmt.Errorf("failed to flush page %v: %v", pid, err)
		}
	}

	return nil
}

// flushPage writes a specific page to disk if it has been modified (is dirty).
// This is an internal helper method used by FlushAllPages and commit processing.
// The page is unmarked as dirty after a successful write operation.
//
// Algorithm:
//  1. Check if page exists in cache
//  2. Skip if page is not dirty
//  3. Write page data to heap file
//  4. Unmark dirty flag
//  5. Update cached copy
//
// Parameters:
//   - dbFile: Heap file to write the page to (must match page's table ID)
//   - pid: Page identifier to flush
//
// Returns:
//   - nil if page doesn't exist or isn't dirty (no-op)
//   - error if disk write fails
//
// Note: Caller must ensure dbFile matches the page's table.
// Thread-safe: Acquires locks for cache access and page updates.
func (p *PageStore) flushPage(dbFile page.DbFile, pid primitives.PageID) error {
	p.mutex.RLock()
	page, exists := p.cache.Get(pid)
	p.mutex.RUnlock()

	if !exists {
		return nil
	}

	if page.IsDirty() == nil {
		return nil
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

// Close gracefully shuts down the PageStore, flushing all pending data and closing the WAL.
// This method should be called when the database is shutting down to ensure:
//   - All dirty pages are written to disk (durability)
//   - WAL is properly closed and synced
//   - No data loss on clean shutdown
//
// Shutdown sequence:
//  1. Flush all dirty pages from cache
//  2. Close WAL (sync and close file)
//
// Returns an error if flushing or WAL closure fails. This may leave the database
// in an inconsistent state requiring recovery on restart.
//
// Note: All active transactions should be committed or aborted before calling Close.
func (p *PageStore) Close() error {
	if err := p.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages during shutdown: %v", err)
	}

	if err := p.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %v", err)
	}

	return nil
}

// logOperation writes an operation record to the Write-Ahead Log.
// This enforces the WAL protocol: log records must be written before page modifications.
//
// Record types:
//   - INSERT: Records new tuple data with page ID
//   - DELETE: Records before-image with page ID for UNDO
//   - COMMIT: Records transaction commit decision
//   - ABORT: Records transaction abort decision
//
// Parameters:
//   - operation: Type of operation to log
//   - tid: Transaction ID performing the operation
//   - pageID: Page affected (nil for COMMIT/ABORT)
//   - data: Page or tuple data (nil for COMMIT/ABORT)
//
// Returns an error if WAL write fails. This is a critical error that should
// cause the operation to be aborted.
func (p *PageStore) logOperation(operation OperationType, tid *primitives.TransactionID, pageID primitives.PageID, data []byte) error {
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

// getDbFileForPage retrieves the DbFile for a given page ID
// This is used internally for operations like flushing that need the DbFile
func (p *PageStore) getDbFileForPage(pageID primitives.PageID) (page.DbFile, error) {
	tableID := pageID.GetTableID()
	p.mutex.RLock()
	dbFile, exists := p.dbFiles[tableID]
	p.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no DbFile registered for table %d", tableID)
	}

	return dbFile, nil
}

// CommitTransaction finalizes all changes made by a transaction and makes them durable.
// This implements the commit phase of the 2-Phase Commit protocol:
//  1. Log COMMIT record to WAL
//  2. Update before-images for all dirty pages (for next transaction)
//  3. Flush all dirty pages to disk (FORCE policy)
//  4. Release all locks held by transaction
//
// After commit completes:
//   - All changes are durable (survive crash)
//   - Other transactions can see the changes
//   - Transaction resources are released
//
// Parameters:
//   - ctx: Transaction context containing dirty pages and lock information
func (p *PageStore) CommitTransaction(ctx TxContext) error {
	return p.finalizeTransaction(ctx, CommitOperation)
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism:
//  1. Log ABORT record to WAL
//  2. Restore before-images for all dirty pages (UNDO)
//  3. Remove uncommitted pages from cache
//  4. Release all locks held by transaction
//
// After abort completes:
//   - All changes are discarded (not visible to any transaction)
//   - Database state is as if transaction never executed
//   - Transaction resources are released
//
// Parameters:
//   - ctx: Transaction context containing dirty pages and lock information
func (p *PageStore) AbortTransaction(ctx TxContext) error {
	return p.finalizeTransaction(ctx, AbortOperation)
}

// finalizeTransaction is the unified handler for COMMIT and ABORT operations.
// It implements the common transaction finalization protocol:
//  1. Validate transaction context
//  2. Check for dirty pages (no-op if read-only transaction)
//  3. Log operation to WAL
//  4. Execute operation-specific logic (commit or abort)
//  5. Release all locks
//
// This centralizes lock management and WAL logging for transaction termination.
//
// Parameters:
//   - ctx: Transaction context (must not be nil)
//   - operation: CommitOperation or AbortOperation
//
// Returns an error if any step fails. The transaction may be in an inconsistent
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

// handleCommit executes the commit phase for dirty pages:
//  1. Update before-images (for next transaction's rollback)
//  2. Flush all dirty pages to disk (FORCE policy)
//
// This ensures durability - after commit returns, changes survive crashes.
//
// Parameters:
//   - dirtyPageIDs: List of pages modified by the committing transaction
//
// Returns an error if any page flush fails. This is a serious error that may
// leave the database in an inconsistent state requiring recovery.
func (p *PageStore) handleCommit(dirtyPageIDs []primitives.PageID) error {
	p.mutex.Lock()
	for _, pid := range dirtyPageIDs {
		if page, exists := p.cache.Get(pid); exists {
			page.SetBeforeImage()
			p.cache.Put(pid, page)
		}
	}
	p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		dbFile, err := p.getDbFileForPage(pid)
		if err != nil {
			return fmt.Errorf("commit failed: unable to get dbFile for page %v: %v", pid, err)
		}
		if err := p.flushPage(dbFile, pid); err != nil {
			return fmt.Errorf("commit failed: unable to flush page %v: %v", pid, err)
		}
	}
	return nil
}

// handleAbort executes the rollback phase for dirty pages:
//  1. Restore before-images for all modified pages (UNDO)
//  2. Remove pages without before-images (newly allocated)
//
// This ensures atomicity - after abort returns, no trace of the transaction remains.
//
// Parameters:
//   - dirtyPageIDs: List of pages modified by the aborting transaction
//
// Returns an error only in exceptional cases (should not normally fail).
//
// Thread-safe: Acquires lock for entire operation to ensure atomic rollback.
func (p *PageStore) handleAbort(dirtyPageIDs []primitives.PageID) error {
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

// GetWal returns the WAL instance used by this PageStore.
func (p *PageStore) GetWal() *wal.WAL {
	return p.wal
}
