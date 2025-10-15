package query

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
)

// SequentialScan implements a sequential scan operator that iterates through all tuples in a table.
// It provides an iterator interface for reading tuples from a database file sequentially.
//
// SequentialScan is a fundamental access method in database systems, serving as the foundation
// for table scans in query execution. It reads all tuples from a table in storage order,
// making it suitable for operations that need to examine every tuple in a table.
type SequentialScan struct {
	base                 *BaseIterator
	tableID, currentPage int
	tupleDesc            *tuple.TupleDescription
	tx                   *transaction.TransactionContext
	dbFile               *heap.HeapFile
	store                *memory.PageStore
	tupIter              *tuple.Iterator
}

// NewSeqScan creates a new SequentialScan operator for the specified table within a transaction context.
// It initializes the scan operator with the necessary metadata and prepares it for iteration.
func NewSeqScan(tx *transaction.TransactionContext, tableID int, file *heap.HeapFile, store *memory.PageStore) (*SequentialScan, error) {
	if store == nil {
		return nil, fmt.Errorf("page store cannot be nil")
	}

	ss := &SequentialScan{
		tx:          tx,
		tableID:     tableID,
		tupleDesc:   file.GetTupleDesc(),
		store:       store,
		currentPage: -1, // Start at -1 so first increment brings us to page 0
		dbFile:      file,
	}

	ss.base = NewBaseIterator(ss.readNext)
	return ss, nil
}

// Open initializes the SequentialScan operator for iteration by creating and opening
// the underlying file iterator.
//
// The method obtains the database file for the target table and creates a file
// iterator that will be used to read tuples sequentially from storage.
//
// Returns an error if the database file cannot be accessed or the iterator fails to open.
func (ss *SequentialScan) Open() error {
	ss.base.MarkOpened()
	return nil
}

// Close releases resources associated with the SequentialScan operator by closing
// the file iterator and performing cleanup.
// Note: This does NOT close the underlying DbFile, as it's managed by the catalog
// and may be shared across multiple iterators.
func (ss *SequentialScan) Close() error {
	// Don't close ss.dbFile - it's owned by the catalog, not this iterator
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

// readNext is the internal method that implements the sequential scanning logic.
// It reads the next tuple from the underlying file iterator, handling the
// low-level details of tuple retrieval from storage.
func (ss *SequentialScan) readNext() (*tuple.Tuple, error) {
	if ss.dbFile == nil {
		return nil, fmt.Errorf("database file not initialized")
	}

	numPages, err := ss.dbFile.NumPages()
	if err != nil {
		return nil, fmt.Errorf("failed to get number of pages: %v", err)
	}

	// Check if current iterator has more tuples
	if ss.tupIter != nil {
		hasNext, err := ss.tupIter.HasNext()
		if err != nil {
			return nil, fmt.Errorf("failed to check iterator: %v", err)
		}
		if hasNext {
			return ss.tupIter.Next()
		}
	}

	// Need to fetch next page with tuples
	for {
		ss.currentPage++
		if ss.currentPage >= numPages {
			return nil, nil // End of table
		}

		pid := heap.NewHeapPageID(ss.tableID, ss.currentPage)
		page, err := ss.store.GetPage(ss.tx, ss.dbFile, pid, transaction.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %d: %v", pid, err)
		}

		hPage := page.(*heap.HeapPage)
		tuples := hPage.GetTuples()

		// Skip empty pages
		if len(tuples) == 0 {
			continue
		}

		ss.tupIter = tuple.NewIterator(tuples)

		// Return first tuple from this page
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

	ss.currentPage = -1
	ss.tupIter = nil
	ss.base.ClearCache()
	return nil
}
