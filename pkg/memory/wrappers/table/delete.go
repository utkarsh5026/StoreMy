package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// DeleteOp handles batch deletion of tuples within a single transaction.
// This operation allows deleting multiple tuples efficiently by:
//   - Batching WAL writes
//   - Processing tuples on the same page together
//   - Updating indexes once after all deletions
//   - Reducing lock acquisition overhead
type DeleteOp struct {
	tm     *TupleManager
	ctx    *transaction.TransactionContext
	dbFile page.DbFile
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
func (tm *TupleManager) NewDeleteOp(ctx *transaction.TransactionContext, dbFile page.DbFile, tuples []*tuple.Tuple) *DeleteOp {
	return &DeleteOp{
		tm:     tm,
		ctx:    ctx,
		dbFile: dbFile,
		tuples: tuples,
	}
}

// Validate checks if the DeleteOp is valid before execution.
// This validates:
//   - Operation has not already been executed
//   - Transaction context is not nil
//   - DbFile is not nil
//   - At least one tuple to delete
//   - All tuples have valid RecordIDs
func (op *DeleteOp) Validate() error {
	if err := validateExecuted(op.executed, "delete"); err != nil {
		return err
	}

	if err := validateTransactionContext(op.ctx); err != nil {
		return err
	}

	if err := validateDbFile(op.dbFile); err != nil {
		return err
	}

	if err := validateTuplesNotEmpty(op.tuples, "delete"); err != nil {
		return err
	}

	if err := validateTuplesHaveRecordIDs(op.tuples, "tuple"); err != nil {
		return err
	}

	return nil
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

	if op.tm.indexManager != nil {
		for i, t := range op.tuples {
			if err := op.tm.indexManager.OnDelete(op.ctx, tableID, t); err != nil {
				return fmt.Errorf("failed to update indexes on delete at index %d: %v", i, err)
			}
		}
	}

	for i, t := range op.tuples {
		modifiedPages, err := op.tm.handleDelete(op.ctx, op.dbFile, t)
		if err != nil {
			return fmt.Errorf("failed to delete tuple at index %d: %v", i, err)
		}

		op.tm.markPagesAsDirty(op.ctx, modifiedPages)
	}

	return nil
}
