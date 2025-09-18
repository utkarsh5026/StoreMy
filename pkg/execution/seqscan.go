package execution

import (
	"fmt"
	"storemy/pkg/storage"
	"storemy/pkg/tables"
	"storemy/pkg/tuple"
)

// SequentialScan implements a sequential scan operator that iterates through all tuples in a table.
// It provides an iterator interface for reading tuples from a database file sequentially.
type SequentialScan struct {
	base         *BaseIterator
	tid          *storage.TransactionID
	tableID      int
	fileIter     storage.DbFileIterator
	tupleDesc    *tuple.TupleDescription
	tableManager *tables.TableManager
}

// NewSeqScan creates a new sequential scan operator for the specified table.
// It initializes the operator but does not open the underlying file iterator.
//
// Parameters:
//   - tid: Transaction ID for the scan operation
//   - tableID: Unique identifier of the table to scan
//   - tm: Table manager instance for accessing table metadata and files
//
// Returns:
//   - *SequentialScan: New sequential scan operator instance
//   - error: Error if table manager is nil or tuple description cannot be retrieved
func NewSeqScan(tid *storage.TransactionID, tableID int, tm *tables.TableManager) (*SequentialScan, error) {
	if tm == nil {
		return nil, fmt.Errorf("tm cannot be nil")
	}

	tupleDesc, err := tm.GetTupleDesc(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tuple desc for table %d: %v", tableID, err)
	}

	ss := &SequentialScan{
		tid:          tid,
		tableID:      tableID,
		tupleDesc:    tupleDesc,
		tableManager: tm,
	}

	ss.base = NewBaseIterator(ss.readNext)
	return ss, nil
}

// Open initializes the sequential scan by opening the underlying database file iterator.
// This method must be called before attempting to read tuples from the scan.
//
// Returns:
//   - error: Error if the database file cannot be accessed or the iterator cannot be opened
func (ss *SequentialScan) Open() error {
	dbFile, err := ss.tableManager.GetDbFile(ss.tableID)
	if err != nil {
		return fmt.Errorf("failed to get db file for table %d: %v", ss.tableID, err)
	}

	ss.fileIter = dbFile.Iterator(ss.tid)
	if ss.fileIter == nil {
		return fmt.Errorf("failed to create file iterator")
	}

	if err := ss.fileIter.Open(); err != nil {
		return fmt.Errorf("failed to open file iterator: %v", err)
	}

	ss.base.MarkOpened()
	return nil
}

// readNext is an internal method that reads the next tuple from the file iterator.
// This method is used by the BaseIterator to implement the iterator pattern.
//
// Returns:
//   - *tuple.Tuple: Next tuple from the scan, or nil if no more tuples are available
//   - error: Error if the file iterator is not initialized or reading fails
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

// GetTupleDesc returns the tuple description for the table being scanned.
// The tuple description contains metadata about the columns and their types.
//
// Returns:
//   - *tuple.TupleDescription: Tuple description for the scanned table
func (ss *SequentialScan) GetTupleDesc() *tuple.TupleDescription {
	return ss.tupleDesc
}

// Close releases resources associated with the sequential scan.
// It closes the underlying file iterator and marks the base iterator as closed.
//
// Returns:
//   - error: Error if closing the base iterator fails
func (ss *SequentialScan) Close() error {
	if ss.fileIter != nil {
		ss.fileIter.Close()
		ss.fileIter = nil
	}
	return ss.base.Close()
}

// HasNext checks if there are more tuples available in the sequential scan.
// The scan must be opened before calling this method.
//
// Returns:
//   - bool: True if more tuples are available, false otherwise
//   - error: Error if the scan is not opened or checking fails
func (ss *SequentialScan) HasNext() (bool, error) { return ss.base.HasNext() }

// Next returns the next tuple from the sequential scan and advances the iterator.
// The scan must be opened before calling this method.
//
// Returns:
//   - *tuple.Tuple: Next tuple from the scan
//   - error: Error if the scan is not opened, no more tuples are available, or reading fails
func (ss *SequentialScan) Next() (*tuple.Tuple, error) { return ss.base.Next() }
