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

// Permissions represents the access level for database operations
type Permissions int

const (
	// ReadOnly permission allows only read operations
	ReadOnly Permissions = iota
	// ReadWrite permission allows both read and write operations
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
	numPages     int
	pageCache    map[tuple.PageID]page.Page                      // In-memory cache of database pages
	tableManager *TableManager                                   // Manager for database tables and files
	mutex        sync.RWMutex                                    // Protects concurrent access to the page cache
	transactions map[*transaction.TransactionID]*TransactionInfo // Active transaction metadata
	accessOrder  []tuple.PageID                                  // For LRU eviction
	lockManager  *lock.LockManager                               // Manages locks for pages
}

// NewPageStore creates and initializes a new PageStore instance with the given TableManager.
// The PageStore will use the TableManager to access database files and manage table operations.
func NewPageStore(tm *TableManager) *PageStore {
	return &PageStore{
		numPages:     50, // Default buffer pool size
		pageCache:    make(map[tuple.PageID]page.Page),
		transactions: make(map[*transaction.TransactionID]*TransactionInfo),
		accessOrder:  make([]tuple.PageID, 0),
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

	if page, exists := p.pageCache[pid]; exists {
		p.updateAccessOrder(pid)
		return page, nil
	}

	if len(p.pageCache) >= p.numPages {
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

	p.pageCache[pid] = page
	p.accessOrder = append(p.accessOrder, pid)
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

func (p *PageStore) updateAccessOrder(pid tuple.PageID) {
	// Move accessed page to end (most recently used)
	for i, pageID := range p.accessOrder {
		if pageID == pid {
			p.accessOrder = append(p.accessOrder[:i], p.accessOrder[i+1:]...)
			p.accessOrder = append(p.accessOrder, pid)
			break
		}
	}
}

// evictPage implements NO-STEAL policy
// We never evict dirty pages to simplify recovery
func (p *PageStore) evictPage() error {
	// Find a clean (not dirty) page that is not locked
	for i := 0; i < len(p.accessOrder); i++ {
		pid := p.accessOrder[i]
		page := p.pageCache[pid]

		// Check if page is dirty (NO-STEAL policy)
		if page.IsDirty() != nil {
			continue
		}

		// // Check if page is locked
		// if p.lockManager.IsPageLocked(pid) {
		// 	continue
		// }

		delete(p.pageCache, pid)
		p.accessOrder = append(p.accessOrder[:i], p.accessOrder[i+1:]...)
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
		p.pageCache[page.GetID()] = page

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

	p.pageCache[modifiedPage.GetID()] = modifiedPage
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
	pids := make([]tuple.PageID, 0, len(p.pageCache))
	for pid := range p.pageCache {
		pids = append(pids, pid)
	}
	p.mutex.RUnlock()

	for _, pid := range pids {
		if err := p.flushPage(pid); err != nil {
			return err
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

	// PHASE 1: Collect dirty pages under lock
	p.mutex.Lock()
	txInfo, exists := p.transactions[tid]
	if !exists {
		p.mutex.Unlock()
		p.lockManager.UnlockAllPages(tid)
		return nil
	}

	// Copy dirty page IDs so we can release the lock
	dirtyPageIDs := make([]tuple.PageID, 0, len(txInfo.dirtyPages))
	for pid := range txInfo.dirtyPages {
		dirtyPageIDs = append(dirtyPageIDs, pid)
	}
	p.mutex.Unlock() // ✅ Release lock before I/O operations

	// PHASE 2: Flush dirty pages to disk (WITHOUT holding the main lock)
	// This allows other transactions to continue while we do I/O
	for _, pid := range dirtyPageIDs {
		if err := p.flushPage(pid); err != nil {
			return fmt.Errorf("commit failed: unable to flush page %v: %v (transaction must be aborted)", pid, err)
		}
	}

	// PHASE 3: Update page states under lock
	p.mutex.Lock()
	for _, pid := range dirtyPageIDs {
		page, exists := p.pageCache[pid]
		if !exists {
			continue
		}

		// Set new baseline for future aborts
		page.SetBeforeImage()

		// Mark page as clean
		page.MarkDirty(false, nil)

		// Update cache
		p.pageCache[pid] = page
	}

	// Remove transaction from tracking
	delete(p.transactions, tid)
	p.mutex.Unlock() // ✅ Release lock before releasing page locks

	// PHASE 4: Release all locks (allows waiting transactions to proceed)
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

	// PHASE 1: Get transaction metadata
	p.mutex.Lock()
	txInfo, exists := p.transactions[tid]
	if !exists {
		p.mutex.Unlock() // ✅ Unlock before releasing locks
		p.lockManager.UnlockAllPages(tid)
		return nil
	}

	// PHASE 2: Restore all dirty pages to their before-image state
	for pid := range txInfo.dirtyPages {
		page, pageExists := p.pageCache[pid]
		if !pageExists {
			continue
		}

		beforeImage := page.GetBeforeImage()
		if beforeImage == nil {
			fmt.Printf("Warning: no before-image for page %v during abort of transaction %v\n",
				pid, tid)
			delete(p.pageCache, pid)
			p.removeFromAccessOrder(pid)
			continue
		}

		// Replace dirty page with clean before-image
		p.pageCache[pid] = beforeImage
		p.updateAccessOrder(pid)
	}

	// PHASE 3: Clean up transaction metadata
	delete(p.transactions, tid)
	p.mutex.Unlock() // ✅ Unlock BEFORE releasing page locks

	// PHASE 4: Release all locks
	p.lockManager.UnlockAllPages(tid)

	return nil
}

// Helper method to remove a page from the access order list
func (p *PageStore) removeFromAccessOrder(pid tuple.PageID) {
	for i, pageID := range p.accessOrder {
		if pageID.Equals(pid) {
			p.accessOrder = append(p.accessOrder[:i], p.accessOrder[i+1:]...)
			return
		}
	}
}

// flushPage writes a specific page to disk if it has been modified (is dirty).
// This is an internal helper method used by FlushAllPages and other flush operations.
// The page is unmarked as dirty after a successful write operation.
//
// Returns an error if the page write fails, or nil if the page doesn't exist or isn't dirty.
func (p *PageStore) flushPage(pid tuple.PageID) error {
	// Read the page under lock
	p.mutex.RLock()
	page, exists := p.pageCache[pid]
	p.mutex.RUnlock()

	if !exists {
		return nil
	}

	// Check if dirty (no lock needed for read-only check)
	if page.IsDirty() == nil {
		return nil
	}

	// Get database file
	dbFile, err := p.tableManager.GetDbFile(pid.GetTableID())
	if err != nil {
		return fmt.Errorf("table for page %v not found: %v", pid, err)
	}

	// Write to disk (I/O operation - no locks held)
	if err := dbFile.WritePage(page); err != nil {
		return fmt.Errorf("failed to write page to disk: %v", err)
	}

	// Note: We don't mark the page as clean here anymore
	// The caller (CommitTransaction) will do that under the proper lock

	return nil
}
