package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/indexmanager"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// OperationType defines the type of tuple operation
type OperationType uint8

const (
	InsertOperation OperationType = iota
	DeleteOperation
	UpdateOperation
)

func (o OperationType) String() string {
	switch o {
	case InsertOperation:
		return "INSERT"
	case DeleteOperation:
		return "DELETE"
	case UpdateOperation:
		return "UPDATE"
	}
	return "UNKNOWN"
}

// StatsRecorder interface for recording table modifications (to avoid circular dependency)
type StatsRecorder interface {
	RecordModification(tableID primitives.TableID)
}

// TupleManager handles tuple-level operations (insert, delete, update) on tables.
// It sits above the buffer pool layer and provides high-level tuple manipulation
// while delegating page-level concerns to the buffer pool.
//
// Responsibilities:
//   - Tuple insertion (finding free space, handling page allocation)
//   - Tuple deletion (locating and removing tuples)
//   - Tuple updates (implementing delete+insert semantics)
//   - WAL logging for all tuple operations
//   - Transaction coordination for tuple operations
//   - Index maintenance on all DML operations
//   - Statistics tracking for query optimization

type TupleManager struct {
	pageProvider *memory.PageStore
	wal          *wal.WAL
	statsManager StatsRecorder
	indexManager *indexmanager.IndexManager
}

// NewTupleManager creates a new TupleManager instance
func NewTupleManager(store *memory.PageStore) *TupleManager {
	return &TupleManager{
		pageProvider: store,
		wal:          store.GetWal(),
		statsManager: nil,
		indexManager: nil,
	}
}

// SetStatsManager sets the statistics manager for tracking table modifications
func (tm *TupleManager) SetStatsManager(sm StatsRecorder) {
	tm.statsManager = sm
}

// SetIndexManager sets the index manager for maintaining indexes on tuple operations
func (tm *TupleManager) SetIndexManager(im *indexmanager.IndexManager) {
	tm.indexManager = im
}

// InsertTuple adds a new tuple to the specified table within the given transaction context.
// This operation:
//   - Logs the insert to WAL before modification (Write-Ahead Logging)
//   - Acquires exclusive lock on target page(s)
//   - Marks modified pages as dirty in transaction's write set
//   - Records modification for statistics updates
//
// The tuple may span multiple pages if the table has large tuples or requires page splits.
// All modified pages are tracked for commit/abort processing.
func (tm *TupleManager) InsertTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) error {
	tableID := dbFile.GetID()
	return tm.performDataOperation(InsertOperation, ctx, dbFile, tableID, t)
}

// DeleteTuple removes a tuple from its table within the given transaction context.
// This operation:
//   - Validates tuple has a RecordID (must have been previously inserted)
//   - Logs the delete to WAL with before-image
//   - Acquires exclusive lock on the containing page
//   - Marks tuple as deleted (may compact page or leave tombstone)
//   - Records modification for statistics updates
//
// MVCC Consideration: The tuple may not be immediately removed from the page.
// Older transactions may still need to see the deleted version for snapshot isolation.
//
// Parameters:
//   - ctx: Transaction context for ACID compliance
//   - dbFile: Heap file for the target table
//   - t: Tuple to delete (must have valid RecordID)
//
// Returns an error if:
//   - Transaction context is nil
//   - Tuple or RecordID is nil
//   - WAL logging fails
//   - Page access fails
//   - Tuple not found at specified RecordID
//
// Thread-safe: Uses page locks and proper WAL ordering.
func (tm *TupleManager) DeleteTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) error {
	if t == nil {
		return fmt.Errorf("tuple cannot be nil")
	}

	if t.RecordID == nil {
		return fmt.Errorf("tuple must have a valid record ID")
	}

	tableID := dbFile.GetID()
	return tm.performDataOperation(DeleteOperation, ctx, dbFile, tableID, t)
}

// UpdateTuple replaces an existing tuple with a new version within the given transaction.
// This implements the UPDATE operation as a DELETE followed by INSERT:
//  1. Delete old tuple (preserving before-image)
//  2. Insert new tuple (may go to different page)
//  3. Rollback insert if it fails (re-insert old tuple)
//
// This approach simplifies MVCC and recovery but may cause fragmentation over time.
// Future optimization: In-place updates when tuple size doesn't change.
//
// Parameters:
//   - ctx: Transaction context
//   - dbFile: Heap file for the table
//   - oldTuple: Current version to replace (must have RecordID)
//   - newTuple: New version to insert
//
// Returns an error if:
//   - oldTuple is nil or has no RecordID
//   - Delete operation fails
//   - Insert operation fails (attempts rollback by reinserting oldTuple)
//
// Thread-safe: Uses page locks via Delete and Insert operations.
func (tm *TupleManager) UpdateTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, oldTuple *tuple.Tuple, newTuple *tuple.Tuple) error {
	if oldTuple == nil {
		return fmt.Errorf("old tuple cannot be nil")
	}

	if oldTuple.RecordID == nil {
		return fmt.Errorf("old tuple has no RecordID")
	}

	if err := tm.DeleteTuple(ctx, dbFile, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %v", err)
	}

	if err := tm.InsertTuple(ctx, dbFile, newTuple); err != nil {
		tm.InsertTuple(ctx, dbFile, oldTuple)
		return fmt.Errorf("failed to insert updated tuple: %v", err)
	}

	return nil
}

// performDataOperation is the unified handler for INSERT and DELETE operations.
// It enforces the Write-Ahead Logging protocol and transaction lifecycle:
//  1. Ensure transaction has logged BEGIN record
//  2. Execute operation-specific logic (insert or delete)
//  3. Log operation to WAL before page modification
//  4. Mark modified pages as dirty
//  5. Update all indexes for the table (automatic index maintenance)
//  6. Record modification for statistics
func (tm *TupleManager) performDataOperation(operation OperationType, ctx *transaction.TransactionContext, dbFile page.DbFile, tableID primitives.TableID, t *tuple.Tuple) error {
	if ctx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	if err := ctx.EnsureBegunInWAL(tm.wal); err != nil {
		return err
	}

	if operation == DeleteOperation && tm.indexManager != nil {
		if err := tm.indexManager.OnDelete(ctx, tableID, t); err != nil {
			return fmt.Errorf("failed to update indexes on delete: %v", err)
		}
	}

	var modifiedPages []page.Page
	var err error
	switch operation {
	case InsertOperation:
		modifiedPages, err = tm.handleInsert(ctx, t, dbFile)

	case DeleteOperation:
		modifiedPages, err = tm.handleDelete(ctx, dbFile, t)

	default:
		return fmt.Errorf("unsupported operation: %s", operation.String())
	}

	if err != nil {
		return err
	}

	tm.markPagesAsDirty(ctx, modifiedPages)

	if operation == InsertOperation && tm.indexManager != nil {
		if err := tm.indexManager.OnInsert(ctx, tableID, t); err != nil {
			return fmt.Errorf("failed to update indexes on insert: %v", err)
		}
	}

	if tm.statsManager != nil {
		tm.statsManager.RecordModification(tableID)
	}

	return nil
}

// handleInsert executes the insert operation and logs it to WAL.
// This helper:
//   - Finds a page with available space (or allocates a new one)
//   - Acquires exclusive lock on the page through GetPage
//   - Logs page state to WAL before modification
//   - Inserts tuple into the page
//   - Returns all modified pages for dirty tracking
//
// Parameters:
//   - ctx: Transaction context
//   - t: Tuple to insert
//   - dbFile: Target heap file
//
// Returns:
//   - modifiedPages: All pages changed by the insert (for dirty tracking)
//   - error: If insert or WAL logging fails
func (tm *TupleManager) handleInsert(ctx *transaction.TransactionContext, t *tuple.Tuple, dbFile page.DbFile) ([]page.Page, error) {
	heapFile, ok := dbFile.(*heap.HeapFile)
	if !ok {
		return nil, fmt.Errorf("dbFile must be a HeapFile for tuple insertion")
	}

	if heapFile.GetTupleDesc() != nil && !t.TupleDesc.Equals(heapFile.GetTupleDesc()) {
		return nil, fmt.Errorf("tuple schema does not match file schema")
	}

	modifiedPages, inserted, err := tm.tryInsertIntoExistingPages(ctx, t, heapFile)
	if err != nil {
		return nil, err
	}
	if inserted {
		return modifiedPages, nil
	}

	return tm.insertIntoNewPage(ctx, t, heapFile)
}

// insertIntoNewPage allocates a new page at the end of the heap file and inserts the tuple.
// This is called when no existing page has sufficient free space for the insertion.
//
// The function follows this sequence:
//  1. Atomically allocate next page number (prevents concurrent allocation races)
//  2. Create new HeapPage with fresh page buffer
//  3. Add tuple to the new page
//  4. Write page to disk through heap file
//  5. Log operation to WAL for durability
//  6. Mark page as dirty in transaction context
func (tm *TupleManager) insertIntoNewPage(ctx *transaction.TransactionContext, t *tuple.Tuple, heapFile *heap.HeapFile) ([]page.Page, error) {
	newPageNo, err := heapFile.AllocateNewPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new page: %v", err)
	}

	newPageID := page.NewPageDescriptor(heapFile.GetID(), newPageNo)
	newPage, err := heap.NewHeapPage(newPageID, make([]byte, page.PageSize), heapFile.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("failed to create new page: %v", err)
	}

	if err := newPage.AddTuple(t); err != nil {
		return nil, fmt.Errorf("failed to add tuple to new page: %v", err)
	}

	if err := heapFile.WritePage(newPage); err != nil {
		return nil, fmt.Errorf("failed to write new page: %v", err)
	}

	if err := tm.logOperation(memory.InsertOperation, ctx.ID, newPageID, newPage.GetPageData()); err != nil {
		return nil, err
	}

	newPage.MarkDirty(true, ctx.ID)
	return []page.Page{newPage}, nil
}

// tryInsertIntoExistingPages attempts to insert a tuple into any existing page with free space.
// This scans all pages in the heap file sequentially, looking for the first page with at least
// one empty slot that can accommodate the tuple.
//
// The function follows this sequence for each page:
//  1. Construct page ID for current page number
//  2. Acquire exclusive lock via GetPage (ReadWrite mode)
//  3. Check if page has empty slots (bitmap-based check)
//  4. Log operation to WAL with before-image
//  5. Attempt tuple insertion
//  6. Mark page dirty if insertion succeeds
//  7. Return immediately on first successful insertion
//
// Concurrency Behavior:
//   - Page-level locks prevent concurrent modifications to the same page
//   - Multiple transactions can insert into different pages simultaneously
//   - Lock acquisition may fail (deadlock, timeout) - these pages are skipped
//   - First-fit strategy: returns first suitable page, not most optimal
//
// Performance Considerations:
//   - O(n) scan of all pages in worst case (no free space found)
//   - No free space tracking - future optimization opportunity
//   - Lock contention possible on popular pages
func (tm *TupleManager) tryInsertIntoExistingPages(ctx *transaction.TransactionContext, t *tuple.Tuple, heapFile *heap.HeapFile) ([]page.Page, bool, error) {
	numPages, err := heapFile.NumPages()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get number of pages: %v", err)
	}

	for i := range numPages {
		pageID := page.NewPageDescriptor(heapFile.GetID(), i)
		pg, err := tm.pageProvider.GetPage(ctx, heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			continue
		}

		heapPage, ok := pg.(*heap.HeapPage)
		if !ok {
			continue
		}

		if heapPage.GetNumEmptySlots() > 0 {
			if err := tm.logOperation(memory.InsertOperation, ctx.ID, pageID, heapPage.GetPageData()); err != nil {
				return nil, false, err
			}

			if err := heapPage.AddTuple(t); err == nil {
				heapPage.MarkDirty(true, ctx.ID)
				return []page.Page{heapPage}, true, nil
			}
		}
	}

	return nil, false, nil // No suitable page found
}

// handleDelete executes the delete operation and logs it to WAL.
// This helper:
//   - Acquires exclusive lock on page containing tuple
//   - Logs before-image to WAL for UNDO capability
//   - Performs actual tuple deletion on HeapPage
//   - Marks page as dirty
//
// Parameters:
//   - ctx: Transaction context
//   - dbFile: Heap file containing the tuple
//   - t: Tuple to delete (must have RecordID)
//
// Returns:
//   - modifiedPages: Single-element array with the page containing deleted tuple
//   - error: If page access, WAL logging, or deletion fails
func (tm *TupleManager) handleDelete(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) ([]page.Page, error) {
	pageID := t.RecordID.PageID
	hpid, ok := pageID.(*page.PageDescriptor)
	if !ok {
		return nil, fmt.Errorf("wrong poage id format")
	}
	pg, err := tm.pageProvider.GetPage(ctx, dbFile, hpid, transaction.ReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to get page for delete: %v", err)
	}

	if err := tm.logOperation(memory.DeleteOperation, ctx.ID, pageID, pg.GetPageData()); err != nil {
		return nil, err
	}

	if heapPage, ok := pg.(*heap.HeapPage); ok {
		if err := heapPage.DeleteTuple(t); err != nil {
			return nil, fmt.Errorf("failed to delete tuple: %v", err)
		}
		heapPage.MarkDirty(true, ctx.ID)
		return []page.Page{pg}, nil
	}

	return nil, fmt.Errorf("expecting the pageType to be of heapage")
}

// logOperation writes an operation record to the Write-Ahead Log.
// This enforces the WAL protocol: log records must be written before page modifications.
//
// Record types:
//   - INSERT: Records new tuple data with page ID
//   - DELETE: Records before-image with page ID for UNDO
//
// Parameters:
//   - operation: Type of operation to log
//   - tid: Transaction ID performing the operation
//   - pageID: Page affected
//   - data: Page or tuple data
//
// Returns an error if WAL write fails. This is a critical error that should
// cause the operation to be aborted.
func (tm *TupleManager) logOperation(operation memory.OperationType, tid *primitives.TransactionID, pageID primitives.PageID, data []byte) error {
	var err error
	switch operation {
	case memory.InsertOperation:
		_, err = tm.wal.LogInsert(tid, pageID, data)
	case memory.DeleteOperation:
		_, err = tm.wal.LogDelete(tid, pageID, data)
	default:
		return fmt.Errorf("unknown operation: %s", operation.String())
	}

	if err != nil {
		return fmt.Errorf("failed to log %s to WAL: %v", operation, err)
	}
	return nil
}

// markPagesAsDirty updates the dirty status for all pages modified by an operation.
// This helper:
//  1. Marks each page as dirty with transaction ID
//  2. Records page in transaction's write set
//
// This ensures proper tracking for commit/abort processing and lock management.
//
// Parameters:
//   - ctx: Transaction context that modified the pages
//   - pages: All pages that were modified
func (tm *TupleManager) markPagesAsDirty(ctx *transaction.TransactionContext, pages []page.Page) {
	for _, pg := range pages {
		pg.MarkDirty(true, ctx.ID)
		ctx.MarkPageDirty(pg.GetID())
	}
}
