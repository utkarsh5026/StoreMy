package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// UpdateOp handles batch updates of tuples within a single transaction.
// Updates are implemented as delete+insert pairs.
// This operation allows updating multiple tuples efficiently by:
//   - Batching deletions and insertions
//   - Reusing pages for new tuple versions
//   - Updating indexes once after all updates
type UpdateOp struct {
	tm        *TupleManager
	ctx       *transaction.TransactionContext
	dbFile    page.DbFile
	oldTuples []*tuple.Tuple
	newTuples []*tuple.Tuple

	// Track execution state
	executed     bool
	updatedCount int // How many updates completed (for partial rollback tracking)
}

// NewUpdateOp creates a new batch update operation for the given transaction.
// The operation is scoped to a single transaction and can update multiple tuples efficiently.
//
// Parameters:
//   - ctx: Transaction context for this operation
//   - dbFile: Target heap file containing tuples to update
//   - oldTuples: Slice of current tuple versions (must have valid RecordIDs)
//   - newTuples: Slice of new tuple versions (must match length of oldTuples)
//
// Returns:
//   - *UpdateOp: A new update operation ready to be validated and executed
func (tm *TupleManager) NewUpdateOp(ctx *transaction.TransactionContext, dbFile page.DbFile, oldTuples, newTuples []*tuple.Tuple) *UpdateOp {
	return &UpdateOp{
		tm:        tm,
		ctx:       ctx,
		dbFile:    dbFile,
		oldTuples: oldTuples,
		newTuples: newTuples,
	}
}

// Validate checks if the UpdateOp is valid before execution.
// This validates:
//   - Operation has not already been executed
//   - Transaction context is not nil
//   - DbFile is not nil
//   - At least one tuple pair to update
//   - oldTuples and newTuples have matching lengths
//   - All old tuples have valid RecordIDs
func (op *UpdateOp) Validate() error {
	if op.executed {
		return fmt.Errorf("update operation already executed")
	}

	if op.ctx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	if op.dbFile == nil {
		return fmt.Errorf("dbFile cannot be nil")
	}

	if len(op.oldTuples) == 0 {
		return fmt.Errorf("no tuples to update")
	}

	if len(op.oldTuples) != len(op.newTuples) {
		return fmt.Errorf("oldTuples and newTuples must have the same length")
	}

	// Validate all old tuples have RecordIDs
	for i, t := range op.oldTuples {
		if t == nil {
			return fmt.Errorf("old tuple at index %d is nil", i)
		}
		if t.RecordID == nil {
			return fmt.Errorf("old tuple at index %d has no RecordID", i)
		}
	}

	// Validate all new tuples are not nil
	for i, t := range op.newTuples {
		if t == nil {
			return fmt.Errorf("new tuple at index %d is nil", i)
		}
	}

	return nil
}

// Execute performs the batch update operation.
// This operation:
//  1. Validates the operation
//  2. Ensures transaction has logged BEGIN record
//  3. For each tuple pair: deletes old version, inserts new version
//  4. Updates indexes once after all updates
//  5. Records modification for statistics
//
// Updates are implemented as delete+insert pairs.
// On failure at tuple i, tuples 0..i-1 are updated, tuple i and beyond are not modified.
// The operation becomes marked as executed regardless of success/failure.
func (op *UpdateOp) Execute() error {
	if err := op.Validate(); err != nil {
		return err
	}

	op.executed = true

	if err := op.ctx.EnsureBegunInWAL(op.tm.wal); err != nil {
		return err
	}

	tableID := op.dbFile.GetID()
	heapFile, ok := op.dbFile.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("dbFile must be a HeapFile")
	}

	// Perform updates as delete+insert pairs
	for i := range op.oldTuples {
		oldTuple := op.oldTuples[i]
		newTuple := op.newTuples[i]

		if op.tm.indexManager != nil {
			if err := op.tm.indexManager.OnDelete(op.ctx, tableID, oldTuple); err != nil {
				return fmt.Errorf("failed to update indexes on delete at index %d: %v", i, err)
			}
		}

		modifiedPages, err := op.tm.handleDelete(op.ctx, op.dbFile, oldTuple)
		if err != nil {
			return fmt.Errorf("failed to delete old tuple at index %d: %v", i, err)
		}
		op.tm.markPagesAsDirty(op.ctx, modifiedPages)

		// Insert new tuple
		modifiedPages, err = op.tm.handleInsert(op.ctx, newTuple, heapFile)
		if err != nil {
			// Try to reinsert old tuple for rollback
			op.tm.handleInsert(op.ctx, oldTuple, heapFile)
			return fmt.Errorf("failed to insert new tuple at index %d: %v", i, err)
		}
		op.tm.markPagesAsDirty(op.ctx, modifiedPages)

		// Insert index entry for new tuple
		if op.tm.indexManager != nil {
			if err := op.tm.indexManager.OnInsert(op.ctx, tableID, newTuple); err != nil {
				return fmt.Errorf("failed to update indexes on insert at index %d: %v", i, err)
			}
		}

		op.updatedCount++
	}

	return nil
}
