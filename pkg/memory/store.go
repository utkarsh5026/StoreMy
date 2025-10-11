package memory

import (
	"fmt"
	"storemy/pkg/concurrency/lock"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"sync"
)

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
	}
	return "UNKNOWN"
}

// StatsRecorder interface for recording table modifications (to avoid circular dependency)
type StatsRecorder interface {
	RecordModification(tableID int)
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
	mutex        sync.RWMutex
	lockManager  *lock.LockManager
	cache        PageCache
	wal          *log.WAL
	statsManager StatsRecorder
	dbFiles      map[int]page.DbFile // tableID -> DbFile mapping
}

// NewPageStore creates and initializes a new PageStore instance
func NewPageStore(wal *log.WAL) *PageStore {
	return &PageStore{
		cache:        NewLRUPageCache(MaxPageCount),
		lockManager:  lock.NewLockManager(),
		wal:          wal,
		statsManager: nil, // Can be set later via SetStatsManager
		dbFiles:      make(map[int]page.DbFile),
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

// SetStatsManager sets the statistics manager for tracking table modifications
// This allows the PageStore to automatically record modifications for statistics updates
func (p *PageStore) SetStatsManager(sm StatsRecorder) {
	p.statsManager = sm
}

// GetPage retrieves a page with specified permissions for a transaction.
// This is the main entry point for all page access in the database, enforcing:
//   - Lock acquisition through LockManager (shared for READ, exclusive for READ_WRITE)
//   - Page loading from disk if not in cache
//   - LRU eviction when cache is full (respecting NO-STEAL policy)
//   - Transaction tracking of accessed pages for commit/abort
//
// Lock Protocol:
//   - ReadOnly: Acquires shared lock (allows concurrent reads)
//   - ReadWrite: Acquires exclusive lock (blocks all other access)
//   - Deadlock detection via dependency graph in LockManager
//
// Parameters:
//   - ctx: Transaction context (must not be nil)
//   - dbFile: Heap file containing the requested page
//   - pid: Page identifier (contains table ID and page number)
//   - perm: Access permissions (ReadOnly or ReadWrite)
//
// Returns the requested page or an error if:
//   - Transaction context is nil
//   - Lock acquisition fails (deadlock or timeout)
//   - Cache is full and eviction fails (all pages dirty or locked)
//   - Disk read fails for page not in cache
//
// Thread-safe: Acquires appropriate locks via LockManager and mutex.
func (p *PageStore) GetPage(ctx *transaction.TransactionContext, dbFile page.DbFile, pid primitives.PageID, perm transaction.Permissions) (page.Page, error) {
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

// markPagesAsDirty updates the dirty status for all pages modified by an operation.
// This helper:
//  1. Marks each page as dirty with transaction ID
//  2. Updates cached copy
//  3. Records page in transaction's write set
//
// This ensures proper tracking for commit/abort processing and lock management.
//
// Parameters:
//   - ctx: Transaction context that modified the pages
//   - pages: All pages that were modified
//
// Note: Caller must hold p.mutex lock if needed (currently acquires internally).
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
