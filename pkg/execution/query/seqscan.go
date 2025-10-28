package query

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// SequentialScan implements a sequential scan operator that iterates through all tuples in a table.
// It provides an iterator interface for reading tuples from a database file sequentially.
//
// SequentialScan is a fundamental access method in database systems, serving as the foundation
// for table scans in query execution. It reads all tuples from a table in storage order,
// making it suitable for operations that need to examine every tuple in a table.
type SequentialScan struct {
	base            *iterator.BaseIterator
	tableID         primitives.FileID
	currentPage     int64
	tupleDesc       *tuple.TupleDescription
	tx              *transaction.TransactionContext
	dbFile          *heap.HeapFile
	store           *memory.PageStore
	tupIter         *tuple.Iterator
	prefetchEnabled bool
	prefetchDone    chan struct{} // Signals when prefetch is complete
}

// NewSeqScan creates a new SequentialScan operator for the specified table within a transaction context.
// It initializes the scan operator with the necessary metadata and prepares it for iteration.
func NewSeqScan(tx *transaction.TransactionContext, tableID primitives.FileID, file *heap.HeapFile, store *memory.PageStore) (*SequentialScan, error) {
	if store == nil {
		return nil, fmt.Errorf("page store cannot be nil")
	}

	ss := &SequentialScan{
		tx:              tx,
		tableID:         tableID,
		tupleDesc:       file.GetTupleDesc(),
		store:           store,
		currentPage:     -1, // Start at -1 so first increment brings us to page 0
		dbFile:          file,
		prefetchEnabled: true,
		prefetchDone:    make(chan struct{}),
	}

	// Close the initial channel since no prefetch is in progress
	close(ss.prefetchDone)

	ss.base = iterator.NewBaseIterator(ss.readNext)
	return ss, nil
}

// Open initializes the SequentialScan operator for iteration by creating and opening
// the underlying file iterator.
func (ss *SequentialScan) Open() error {
	ss.base.MarkOpened()
	return nil
}

// Close releases resources associated with the SequentialScan operator by closing
// the file iterator and performing cleanup.
// Note: This does NOT close the underlying DbFile, as it's managed by the catalog
// and may be shared across multiple iterators.
func (ss *SequentialScan) Close() error {
	if ss.prefetchDone != nil {
		<-ss.prefetchDone
	}
	ss.dbFile = nil
	return ss.base.Close()
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this scan.
// The schema describes the structure, field names, and types of tuples in the target table.
func (ss *SequentialScan) GetTupleDesc() *tuple.TupleDescription {
	return ss.tupleDesc
}

// HasNext checks if there are more tuples available in the sequential scan.
func (ss *SequentialScan) HasNext() (bool, error) {
	return ss.base.HasNext()
}

// Next retrieves the next tuple from the sequential scan.
func (ss *SequentialScan) Next() (*tuple.Tuple, error) {
	return ss.base.Next()
}

// prefetchNextPage asynchronously prefetches the next page to improve I/O performance.
// It runs in a background goroutine and signals completion via the prefetchDone channel.
func (ss *SequentialScan) prefetchNextPage(nextPageNum primitives.PageNumber) {
	if !ss.prefetchEnabled {
		return
	}

	numPages, err := ss.dbFile.NumPages()
	if err != nil || nextPageNum >= numPages {
		return
	}

	ss.prefetchDone = make(chan struct{})

	go func() {
		defer close(ss.prefetchDone)

		nextPID := page.NewPageDescriptor(ss.tableID, nextPageNum)
		_, _ = ss.store.GetPage(ss.tx, ss.dbFile, nextPID, transaction.ReadOnly)
		// Ignore errors in prefetch - the main read will handle them
	}()
}

// readNext is the internal method that implements the sequential scanning logic.
// It reads the next tuple from the underlying file iterator, handling the
// low-level details of tuple retrieval from storage.
//
// The implementation includes page prefetching: when moving to a new page,
// it asynchronously prefetches the next page in the background to reduce I/O wait time.
func (ss *SequentialScan) readNext() (*tuple.Tuple, error) {
	if ss.dbFile == nil {
		return nil, fmt.Errorf("database file not initialized")
	}

	numPages, err := ss.dbFile.NumPages()
	if err != nil {
		return nil, fmt.Errorf("failed to get number of pages: %v", err)
	}

	if ss.tupIter != nil {
		hasNext, err := ss.tupIter.HasNext()
		if err != nil {
			return nil, fmt.Errorf("failed to check iterator: %v", err)
		}
		if hasNext {
			return ss.tupIter.Next()
		}
	}

	for {
		ss.currentPage++
		if ss.currentPage >= int64(numPages) {
			return nil, nil
		}

		<-ss.prefetchDone

		pid := page.NewPageDescriptor(ss.tableID, primitives.PageNumber(ss.currentPage))
		page, err := ss.store.GetPage(ss.tx, ss.dbFile, pid, transaction.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %d: %v", pid, err)
		}

		hPage := page.(*heap.HeapPage)
		tuples := hPage.GetTuples()

		ss.prefetchNextPage(primitives.PageNumber(ss.currentPage + 1))

		if len(tuples) == 0 {
			continue
		}

		ss.tupIter = tuple.NewIterator(tuples)
		if err := ss.tupIter.Open(); err != nil {
			return nil, fmt.Errorf("failed to open tuple iterator: %v", err)
		}
		hasNext, err := ss.tupIter.HasNext()
		if err != nil {
			return nil, fmt.Errorf("failed to check iterator: %v", err)
		}
		if hasNext {
			return ss.tupIter.Next()
		}
	}
}

// Rewind resets the SequentialScan operator to the beginning of the table.
// This allows the scan to be re-executed from the start, which is useful
// for operations that need to scan the table multiple times.
func (ss *SequentialScan) Rewind() error {
	if ss.dbFile == nil {
		return fmt.Errorf("database file not initialized")
	}

	<-ss.prefetchDone

	ss.currentPage = -1
	ss.tupIter = nil
	ss.base.ClearCache()

	ss.prefetchDone = make(chan struct{})
	close(ss.prefetchDone)
	return nil
}
