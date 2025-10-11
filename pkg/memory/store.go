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
// providing ACID compliance through transaction tracking and page lifecycle management.
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

// GetPage retrieves a page with specified permissions for a transaction
// This is the main entry point for all page access in the database
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

// FlushAllPages writes all dirty pages to persistent storage.
// This operation ensures data durability by synchronizing the in-memory cache
// with the underlying database files. Pages are unmarked as dirty after successful writes.
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
// This is an internal helper method used by FlushAllPages and other flush operations.
// The page is unmarked as dirty after a successful write operation.
//
// NOTE: flushPage requires the dbFile to be passed externally since PageStore no longer
// maintains table metadata. The caller (typically CatalogManager) must provide the appropriate DbFile.
// Returns an error if the page write fails, or nil if the page doesn't exist or isn't dirty.
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

// ensureTransactionBegun ensures that a transaction's BEGIN record has been logged to the WAL.
// This is a helper method for tests to directly ensure a transaction has begun.
func (p *PageStore) ensureTransactionBegun(ctx *transaction.TransactionContext) error {
	return ctx.EnsureBegunInWAL(p.wal)
}
