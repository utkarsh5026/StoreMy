package query

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/tables"
	"storemy/pkg/tuple"
)

// SequentialScan implements a sequential scan operator that iterates through all tuples in a table.
// It provides an iterator interface for reading tuples from a database file sequentially.
type SequentialScan struct {
	base         *BaseIterator
	tid          *transaction.TransactionID
	tableID      int
	fileIter     iterator.DbFileIterator
	tupleDesc    *tuple.TupleDescription
	tableManager *tables.TableManager
}

func NewSeqScan(tid *transaction.TransactionID, tableID int, tm *tables.TableManager) (*SequentialScan, error) {
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

func (ss *SequentialScan) GetTupleDesc() *tuple.TupleDescription {
	return ss.tupleDesc
}

func (ss *SequentialScan) Close() error {
	if ss.fileIter != nil {
		ss.fileIter.Close()
		ss.fileIter = nil
	}
	return ss.base.Close()
}

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

func (ss *SequentialScan) HasNext() (bool, error) { return ss.base.HasNext() }

func (ss *SequentialScan) Next() (*tuple.Tuple, error) { return ss.base.Next() }
