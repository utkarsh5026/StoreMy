package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// SequentialScan implements a sequential scan operator that iterates through all tuples in a table.
// It provides an iterator interface for reading tuples from a database file sequentially.
//
// SequentialScan is a fundamental access method in database systems, serving as the foundation
// for table scans in query execution. It reads all tuples from a table in storage order,
// making it suitable for operations that need to examine every tuple in a table.
type SequentialScan struct {
	base         *BaseIterator
	tid          *primitives.TransactionID
	tableID      int
	fileIter     iterator.DbFileIterator
	tupleDesc    *tuple.TupleDescription
	tableManager *memory.TableManager
}

// NewSeqScan creates a new SequentialScan operator for the specified table within a transaction context.
// It initializes the scan operator with the necessary metadata and prepares it for iteration.
func NewSeqScan(tid *primitives.TransactionID, tableID int, tm *memory.TableManager) (*SequentialScan, error) {
	if tm == nil {
		return nil, fmt.Errorf("tm cannot be nil")
	}

	info, err := tm.GetTableInfo(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tuple desc for table %d: %v", tableID, err)
	}

	ss := &SequentialScan{
		tid:          tid,
		tableID:      tableID,
		tupleDesc:    info.Schema.TupleDesc,
		tableManager: tm,
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
	info, err := ss.tableManager.GetTableInfo(ss.tableID)
	if err != nil {
		return fmt.Errorf("failed to get db file for table %d: %v", ss.tableID, err)
	}

	ss.fileIter = info.File.Iterator(ss.tid)
	if ss.fileIter == nil {
		return fmt.Errorf("failed to create file iterator")
	}

	if err := ss.fileIter.Open(); err != nil {
		return fmt.Errorf("failed to open file iterator: %v", err)
	}

	ss.base.MarkOpened()
	return nil
}

// Close releases resources associated with the SequentialScan operator by closing
// the file iterator and performing cleanup.
func (ss *SequentialScan) Close() error {
	if ss.fileIter != nil {
		ss.fileIter.Close()
		ss.fileIter = nil
	}
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
	if ss.fileIter == nil {
		return nil, fmt.Errorf("file iterator not initialized")
	}

	hasNext, err := ss.fileIter.HasNext()
	if err != nil {
		return nil, err
	}

	if !hasNext {
		return nil, nil
	}

	return ss.fileIter.Next()
}

// Rewind resets the SequentialScan operator to the beginning of the table.
// This allows the scan to be re-executed from the start, which is useful
// for operations that need to scan the table multiple times.
func (ss *SequentialScan) Rewind() error {
	if ss.fileIter == nil {
		return fmt.Errorf("file iterator not initialized")
	}

	if err := ss.fileIter.Rewind(); err != nil {
		return err
	}

	ss.base.ClearCache()
	return nil
}
