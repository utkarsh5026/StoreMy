package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
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
	dbFile    *heap.HeapFile
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
func (tm *TupleManager) NewUpdateOp(ctx *transaction.TransactionContext, dbFile *heap.HeapFile, oldTuples, newTuples []*tuple.Tuple) *UpdateOp {
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
	if err := validateBasic("update", op.ctx, op.dbFile, op.oldTuples, op.executed); err != nil {
		return err
	}

	if len(op.oldTuples) != len(op.newTuples) {
		return fmt.Errorf("oldTuples and newTuples must have the same length")
	}

	if err := validateTuplesHaveRecordIDs(op.oldTuples, "old tuple"); err != nil {
		return err
	}

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
//  3. Creates and executes a DeleteOp for all old tuples
//  4. Creates and executes an InsertOp for all new tuples
//  5. Records modification for statistics
//
// Updates are implemented as delete+insert operations using DeleteOp and InsertOp.
// If deletion succeeds but insertion fails, the old tuples remain deleted (transaction rollback will restore them).
// The operation becomes marked as executed regardless of success/failure.
func (op *UpdateOp) Execute() error {
	if err := op.Validate(); err != nil {
		return err
	}

	op.executed = true

	if err := op.ctx.EnsureBegunInWAL(op.tm.wal); err != nil {
		return err
	}

	deleteOp := op.tm.NewDeleteOp(op.ctx, op.dbFile, op.oldTuples)
	if err := deleteOp.Execute(); err != nil {
		return fmt.Errorf("failed to delete old tuples: %v", err)
	}

	insertOp := op.tm.NewInsertOp(op.ctx, op.dbFile, op.newTuples)
	if err := insertOp.Execute(); err != nil {
		return fmt.Errorf("failed to insert new tuples: %v", err)
	}

	op.updatedCount = len(op.oldTuples)
	return nil
}
