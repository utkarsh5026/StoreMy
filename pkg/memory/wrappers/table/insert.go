package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// InsertOp handles batch insertion of tuples within a single transaction.
// This operation allows inserting multiple tuples efficiently by:
//   - Batching WAL writes
//   - Reusing pages across multiple insertions
//   - Updating indexes once after all insertions
//   - Amortizing lock acquisition overhead
type InsertOp struct {
	tm     *TupleManager
	ctx    *transaction.TransactionContext
	dbFile page.DbFile
	tuples []*tuple.Tuple

	// Track execution state
	executed       bool
	insertedTuples []*tuple.Tuple // Successfully inserted tuples (for partial rollback)
}

// NewInsertOp creates a new batch insert operation for the given transaction.
// The operation is scoped to a single transaction and can insert multiple tuples efficiently.
//
// Parameters:
//   - ctx: Transaction context for this operation
//   - dbFile: Target heap file for insertions
//   - tuples: Slice of tuples to insert
//
// Returns:
//   - *InsertOp: A new insert operation ready to be validated and executed
func (tm *TupleManager) NewInsertOp(ctx *transaction.TransactionContext, dbFile page.DbFile, tuples []*tuple.Tuple) *InsertOp {
	return &InsertOp{
		tm:             tm,
		ctx:            ctx,
		dbFile:         dbFile,
		tuples:         tuples,
		insertedTuples: make([]*tuple.Tuple, 0, len(tuples)),
	}
}

// Validate checks if the InsertOp is valid before execution.
// This validates:
//   - Operation has not already been executed
//   - Transaction context is not nil
//   - DbFile is not nil
//   - At least one tuple to insert
//   - All tuples have matching schemas
func (op *InsertOp) Validate() error {
	if op.executed {
		return fmt.Errorf("insert operation already executed")
	}

	if op.ctx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	if op.dbFile == nil {
		return fmt.Errorf("dbFile cannot be nil")
	}

	if len(op.tuples) == 0 {
		return fmt.Errorf("no tuples to insert")
	}

	heapFile, ok := op.dbFile.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("dbFile must be a HeapFile for tuple insertion")
	}

	fileSchema := heapFile.GetTupleDesc()
	if fileSchema != nil {
		for i, t := range op.tuples {
			if t == nil {
				return fmt.Errorf("tuple at index %d is nil", i)
			}
			if !t.TupleDesc.Equals(fileSchema) {
				return fmt.Errorf("tuple at index %d has incompatible schema", i)
			}
		}
	}

	return nil
}

// Execute performs the batch insert operation.
// This operation:
//  1. Validates the operation
//  2. Ensures transaction has logged BEGIN record
//  3. Inserts all tuples (fail-fast on first error)
//  4. Updates all indexes once after all insertions
//  5. Records modification for statistics
//
// On failure, successfully inserted tuples remain inserted (transaction rollback will undo them).
// The operation becomes marked as executed regardless of success/failure.
func (op *InsertOp) Execute() error {
	if err := op.Validate(); err != nil {
		return err
	}

	op.executed = true
	if err := op.ctx.EnsureBegunInWAL(op.tm.wal); err != nil {
		return err
	}

	tableID := op.dbFile.GetID()
	heapFile := op.dbFile.(*heap.HeapFile)

	for i, t := range op.tuples {
		modifiedPages, err := op.tm.handleInsert(op.ctx, t, heapFile)
		if err != nil {
			return fmt.Errorf("failed to insert tuple at index %d: %v", i, err)
		}

		op.tm.markPagesAsDirty(op.ctx, modifiedPages)
		op.insertedTuples = append(op.insertedTuples, t)
	}

	if op.tm.indexManager != nil {
		for _, t := range op.insertedTuples {
			if err := op.tm.indexManager.OnInsert(op.ctx, tableID, t); err != nil {
				return fmt.Errorf("failed to update indexes on insert: %v", err)
			}
		}
	}

	return nil
}
