package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
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

// TupleOperation defines the interface for batch tuple operations.
// Each operation is scoped to a single transaction context and can operate on multiple tuples.
type TupleOperation interface {
	Execute() error
	Validate() error
}

// IndexMaintainer defines the interface for maintaining indexes during tuple operations.
// This interface breaks the import cycle between pkg/memory/wrappers/table and pkg/indexmanager.
type IndexMaintainer interface {
	OnInsert(ctx *transaction.TransactionContext, tableID primitives.FileID, t *tuple.Tuple) error
	OnDelete(ctx *transaction.TransactionContext, tableID primitives.FileID, t *tuple.Tuple) error
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

type TupleManager struct {
	pageProvider    *memory.PageStore
	wal             *wal.WAL
	indexMaintainer IndexMaintainer
}

// NewTupleManager creates a new TupleManager instance
func NewTupleManager(store *memory.PageStore) *TupleManager {
	return &TupleManager{
		pageProvider:    store,
		wal:             store.GetWal(),
		indexMaintainer: nil,
	}
}

// SetIndexMaintainer sets the index maintainer for maintaining indexes on tuple operations
func (tm *TupleManager) SetIndexMaintainer(im IndexMaintainer) {
	tm.indexMaintainer = im
}

// InsertTuple adds a new tuple to the specified table within the given transaction context.
// This operation:
//   - Logs the insert to WAL before modification (Write-Ahead Logging)
//   - Acquires exclusive lock on target page(s)
//   - Marks modified pages as dirty in transaction's write set
//
// The tuple may span multiple pages if the table has large tuples or requires page splits.
// All modified pages are tracked for commit/abort processing.
func (tm *TupleManager) InsertTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, t *tuple.Tuple) error {
	return tm.NewInsertOp(ctx, dbFile.(*heap.HeapFile), []*tuple.Tuple{t}).Execute()
}

// DeleteTuple removes a tuple from its table within the given transaction context.
// This operation:
//   - Validates tuple has a RecordID (must have been previously inserted)
//   - Logs the delete to WAL with before-image
//   - Acquires exclusive lock on the containing page
//   - Marks tuple as deleted (may compact page or leave tombstone)
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
	return tm.NewDeleteOp(ctx, dbFile.(*heap.HeapFile), []*tuple.Tuple{t}).Execute()
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
func (tm *TupleManager) UpdateTuple(ctx *transaction.TransactionContext, dbFile page.DbFile, oldTuple, newTuple *tuple.Tuple) error {
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
		_ = tm.InsertTuple(ctx, dbFile, oldTuple)
		return fmt.Errorf("failed to insert updated tuple: %v", err)
	}

	return nil
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
func (tm *TupleManager) markPagesAsDirty(ctx *transaction.TransactionContext, pages []*heap.HeapPage) {
	for _, pg := range pages {
		pg.MarkDirty(true, ctx.ID)
		ctx.MarkPageDirty(pg.GetID())
	}
}
