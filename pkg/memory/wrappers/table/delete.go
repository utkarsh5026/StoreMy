package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// DeleteOp handles batch deletion of tuples within a single transaction.
type DeleteOp struct {
	tm     *TupleManager
	ctx    *transaction.TransactionContext
	dbFile *heap.HeapFile
	tuples []*tuple.Tuple

	executed bool
}

// NewDeleteOp creates a new batch delete operation for the given transaction.
// The operation is scoped to a single transaction and can delete multiple tuples efficiently.
//
// Parameters:
//   - ctx: Transaction context for this operation
//   - dbFile: Target heap file containing tuples to delete
//   - tuples: Slice of tuples to delete (must have valid RecordIDs)
//
// Returns:
//   - *DeleteOp: A new delete operation ready to be validated and executed
func (tm *TupleManager) NewDeleteOp(ctx *transaction.TransactionContext, dbFile *heap.HeapFile, tuples []*tuple.Tuple) *DeleteOp {
	return &DeleteOp{
		tm:     tm,
		ctx:    ctx,
		dbFile: dbFile,
		tuples: tuples,
	}
}

// Validate checks if the DeleteOp is valid before execution.
func (op *DeleteOp) Validate() error {
	if err := validateBasic("delete", op.ctx, op.dbFile, op.tuples, op.executed); err != nil {
		return err
	}

	return validateTuplesHaveRecordIDs(op.tuples, "tuple")
}

// Execute performs the batch delete operation.
// This operation:
//  1. Validates the operation
//  2. Ensures transaction has logged BEGIN record
//  3. Updates indexes before deletions (for proper cleanup)
//  4. Deletes all tuples (fail-fast on first error)
//  5. Records modification for statistics
//
// On failure, successfully deleted tuples remain deleted (transaction rollback will restore them).
// The operation becomes marked as executed regardless of success/failure.
func (op *DeleteOp) Execute() error {
	if err := op.Validate(); err != nil {
		return err
	}

	op.executed = true
	if err := op.ctx.EnsureBegunInWAL(op.tm.wal); err != nil {
		return err
	}

	tableID := op.dbFile.GetID()

	if op.tm.indexMaintainer != nil {
		for i, t := range op.tuples {
			if err := op.tm.indexMaintainer.OnDelete(op.ctx, tableID, t); err != nil {
				return fmt.Errorf("failed to update indexes on delete at index %d: %v", i, err)
			}
		}
	}

	for i, t := range op.tuples {
		modifiedPages, err := op.handleDelete(t)
		if err != nil {
			return fmt.Errorf("failed to delete tuple at index %d: %v", i, err)
		}

		op.tm.markPagesAsDirty(op.ctx, modifiedPages)
	}

	return nil
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
func (op *DeleteOp) handleDelete(t *tuple.Tuple) ([]*heap.HeapPage, error) {
	pageID := t.RecordID.PageID
	hpid, ok := pageID.(*page.PageDescriptor)
	if !ok {
		return nil, fmt.Errorf("wrong page id format")
	}

	pg, err := op.tm.pageProvider.GetPage(op.ctx, op.dbFile, hpid, transaction.ReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to get page for delete: %v", err)
	}

	if err := op.tm.logOperation(memory.DeleteOperation, op.ctx.ID, pageID, pg.GetPageData()); err != nil {
		return nil, err
	}

	if heapPage, ok := pg.(*heap.HeapPage); ok {
		if err := heapPage.DeleteTuple(t); err != nil {
			return nil, fmt.Errorf("failed to delete tuple: %v", err)
		}
		heapPage.MarkDirty(true, op.ctx.ID)
		return []*heap.HeapPage{heapPage}, nil
	}

	return nil, fmt.Errorf("expecting the pageType to be of heapage")
}
