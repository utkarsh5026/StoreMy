package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
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
	dbFile *heap.HeapFile
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
func (tm *TupleManager) NewInsertOp(ctx *transaction.TransactionContext, dbFile *heap.HeapFile, tuples []*tuple.Tuple) *InsertOp {
	return &InsertOp{
		tm:             tm,
		ctx:            ctx,
		dbFile:         dbFile,
		tuples:         tuples,
		insertedTuples: make([]*tuple.Tuple, 0, len(tuples)),
	}
}

// Validate checks if the InsertOp is valid before execution.
func (op *InsertOp) Validate() error {
	if err := validateBasic("insert", op.ctx, op.dbFile, op.tuples, op.executed); err != nil {
		return err
	}

	fileSchema := op.dbFile.GetTupleDesc()
	if fileSchema == nil {
		return nil
	}

	for i, t := range op.tuples {
		if t == nil {
			return fmt.Errorf("tuple at index %d is nil", i)
		}
		if !t.TupleDesc.Equals(fileSchema) {
			return fmt.Errorf("tuple at index %d has incompatible schema", i)
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

	for i, t := range op.tuples {
		modifiedPages, err := op.handleInsert(t)
		if err != nil {
			return fmt.Errorf("failed to insert tuple at index %d: %v", i, err)
		}

		op.tm.markPagesAsDirty(op.ctx, modifiedPages)
		op.insertedTuples = append(op.insertedTuples, t)
	}

	if op.tm.indexMaintainer != nil {
		for _, t := range op.insertedTuples {
			if err := op.tm.indexMaintainer.OnInsert(op.ctx, tableID, t); err != nil {
				return fmt.Errorf("failed to update indexes on insert: %v", err)
			}
		}
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
func (op *InsertOp) handleInsert(t *tuple.Tuple) ([]*heap.HeapPage, error) {
	modifiedPages, inserted, err := op.tryInsertIntoExistingPages(t)
	if err != nil {
		return nil, err
	}
	if inserted {
		return modifiedPages, nil
	}

	return op.insertIntoNewPage(t)
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
func (op *InsertOp) insertIntoNewPage(t *tuple.Tuple) ([]*heap.HeapPage, error) {
	p, err := op.createNewPage()
	if err != nil {
		return nil, err
	}

	if err := p.AddTuple(t); err != nil {
		return nil, fmt.Errorf("failed to add tuple to new page: %v", err)
	}

	if err := op.dbFile.WritePage(p); err != nil {
		return nil, fmt.Errorf("failed to write new page: %v", err)
	}

	if err := op.logInsert(p); err != nil {
		return nil, err
	}

	p.MarkDirty(true, op.ctx.ID)
	return []*heap.HeapPage{p}, nil
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
func (op *InsertOp) tryInsertIntoExistingPages(t *tuple.Tuple) ([]*heap.HeapPage, bool, error) {
	f := op.dbFile
	numPages, err := op.dbFile.NumPages()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get number of pages: %v", err)
	}

	for i := range numPages {
		pageID := page.NewPageDescriptor(f.GetID(), i)
		pg, err := op.tm.pageProvider.GetPage(op.ctx, op.dbFile, pageID, transaction.ReadWrite)
		if err != nil {
			continue
		}

		heapPage, ok := pg.(*heap.HeapPage)
		if !ok {
			continue
		}

		if heapPage.GetNumEmptySlots() > 0 {
			if err := op.logInsert(heapPage); err != nil {
				return nil, false, err
			}

			if err := heapPage.AddTuple(t); err == nil {
				heapPage.MarkDirty(true, op.ctx.ID)
				return []*heap.HeapPage{heapPage}, true, nil
			}
		}
	}

	return nil, false, nil
}

func (op *InsertOp) createNewPage() (*heap.HeapPage, error) {
	f := op.dbFile
	newPageNo, err := f.AllocateNewPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new page: %v", err)
	}

	newPageID := page.NewPageDescriptor(f.GetID(), newPageNo)
	newPage, err := heap.NewEmptyHeapPage(newPageID, f.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("failed to create new page: %v", err)
	}
	return newPage, err
}

func (op *InsertOp) logInsert(page *heap.HeapPage) error {
	return op.tm.logOperation(memory.InsertOperation, op.ctx.ID, page.GetID(), page.GetPageData())
}
