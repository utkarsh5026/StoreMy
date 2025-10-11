package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// InsertTuple adds a new tuple to the specified table within the given transaction context.
// This operation:
//   - Logs the insert to WAL before modification (Write-Ahead Logging)
//   - Acquires exclusive lock on target page(s)
//   - Marks modified pages as dirty in transaction's write set
//   - Records modification for statistics updates
//
// The tuple may span multiple pages if the table has large tuples or requires page splits.
// All modified pages are tracked for commit/abort processing.
//
// Parameters:
//   - ctx: Transaction context for ACID compliance
//   - dbFile: Heap file for the target table
//   - t: Tuple to insert (RecordID will be assigned during insertion)
//
// Returns an error if:
//   - Transaction context is nil
//   - WAL logging fails
//   - No free space exists in any page (table full)
//   - Lock acquisition fails
//
// Thread-safe: Uses page locks and proper WAL ordering.
func (p *PageStore) InsertTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) error {
	tableID := dbFile.GetID()
	return p.performDataOperation(InsertOperation, ctx, dbFile, tableID, t)
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
func (p *PageStore) DeleteTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) error {
	if t == nil {
		return fmt.Errorf("tuple cannot be nil")
	}

	if t.RecordID == nil {
		return fmt.Errorf("tuple must have a valid record ID")
	}

	tableID := dbFile.GetID()
	return p.performDataOperation(DeleteOperation, ctx, dbFile, tableID, t)
}

// performDataOperation is the unified handler for INSERT and DELETE operations.
// It enforces the Write-Ahead Logging protocol and transaction lifecycle:
//  1. Ensure transaction has logged BEGIN record
//  2. Execute operation-specific logic (insert or delete)
//  3. Log operation to WAL before page modification
//  4. Mark modified pages as dirty
//  5. Record modification for statistics
//
// This centralizes the common transaction management code shared by all DML operations.
//
// Parameters:
//   - operation: Type of operation (InsertOperation or DeleteOperation)
//   - ctx: Transaction context
//   - dbFile: Target heap file
//   - tableID: Table identifier for statistics
//   - t: Tuple being inserted or deleted
//
// Returns an error if any step fails. The transaction should be aborted on error.
//
// Note: Updates are implemented as DELETE + INSERT at a higher level.
func (p *PageStore) performDataOperation(operation OperationType, ctx *transaction.TransactionContext, dbFile page.DbFile, tableID int, t *tuple.Tuple) error {
	if ctx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	if err := ctx.EnsureBegunInWAL(p.wal); err != nil {
		return err
	}

	var modifiedPages []page.Page
	var err error
	switch operation {
	case InsertOperation:
		modifiedPages, err = p.handleInsert(ctx, t, dbFile)

	case DeleteOperation:
		modifiedPages, err = p.handleDelete(ctx, dbFile, t)

	default:
		return fmt.Errorf("unsupported operation: %s", operation.String())
	}

	if err != nil {
		return err
	}

	p.markPagesAsDirty(ctx, modifiedPages)

	if p.statsManager != nil {
		p.statsManager.RecordModification(tableID)
	}

	return nil
}

// handleInsert executes the insert operation and logs it to WAL.
// This helper:
//   - Delegates to heap file's AddTuple (which handles page selection/splitting)
//   - Logs each modified page to WAL (multiple pages if tuple is large)
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
func (p *PageStore) handleDelete(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) ([]page.Page, error) {
	pageID := t.RecordID.PageID
	pg, err := p.GetPage(ctx, dbFile, pageID, transaction.ReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to get page for delete: %v", err)
	}

	if err := p.logOperation(DeleteOperation, ctx.ID, pageID, pg.GetPageData()); err != nil {
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
func (p *PageStore) UpdateTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, oldTuple *tuple.Tuple, newTuple *tuple.Tuple) error {
	if oldTuple == nil {
		return fmt.Errorf("old tuple cannot be nil")
	}

	if oldTuple.RecordID == nil {
		return fmt.Errorf("old tuple has no RecordID")
	}

	if err := p.DeleteTuple(ctx, dbFile, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %v", err)
	}

	if err := p.InsertTuple(ctx, dbFile, newTuple); err != nil {
		p.InsertTuple(ctx, dbFile, oldTuple)
		return fmt.Errorf("failed to insert updated tuple: %v", err)
	}

	return nil
}
