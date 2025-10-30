package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// Common validation helpers to reduce duplication across InsertOp, UpdateOp, and DeleteOp

// validateExecuted checks if an operation has already been executed.
func validateExecuted(executed bool, opType string) error {
	if executed {
		return fmt.Errorf("%s operation already executed", opType)
	}
	return nil
}

// validateTransactionContext checks if the transaction context is not nil.
func validateTransactionContext(ctx *transaction.TransactionContext) error {
	if ctx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}
	return nil
}

// validateDbFile checks if the dbFile is not nil.
func validateDbFile(dbFile page.DbFile) error {
	if dbFile == nil {
		return fmt.Errorf("dbFile cannot be nil")
	}
	return nil
}

// validateTuplesNotEmpty checks if a tuple slice has at least one element.
func validateTuplesNotEmpty(tuples []*tuple.Tuple, opType string) error {
	if len(tuples) == 0 {
		return fmt.Errorf("no tuples to %s", opType)
	}
	return nil
}

// validateTuplesNotNil checks if any tuple in the slice is nil.
func validateTuplesNotNil(tuples []*tuple.Tuple, tupleName string) error {
	for i, t := range tuples {
		if t == nil {
			return fmt.Errorf("%s at index %d is nil", tupleName, i)
		}
	}
	return nil
}

// validateTuplesHaveRecordIDs checks if all tuples have valid RecordIDs.
func validateTuplesHaveRecordIDs(tuples []*tuple.Tuple, tupleName string) error {
	for i, t := range tuples {
		if t == nil {
			return fmt.Errorf("%s at index %d is nil", tupleName, i)
		}
		if t.RecordID == nil {
			return fmt.Errorf("%s at index %d has no RecordID", tupleName, i)
		}
	}
	return nil
}

// validateHeapFile checks if the dbFile is a HeapFile and returns it.
func validateHeapFile(dbFile page.DbFile) (*heap.HeapFile, error) {
	heapFile, ok := dbFile.(*heap.HeapFile)
	if !ok {
		return nil, fmt.Errorf("dbFile must be a HeapFile for tuple insertion")
	}
	return heapFile, nil
}

// validateSchemaMatch checks if all tuples match the file's schema.
func validateSchemaMatch(tuples []*tuple.Tuple, heapFile *heap.HeapFile) error {
	fileSchema := heapFile.GetTupleDesc()
	if fileSchema == nil {
		return nil
	}

	for i, t := range tuples {
		if t == nil {
			return fmt.Errorf("tuple at index %d is nil", i)
		}
		if !t.TupleDesc.Equals(fileSchema) {
			return fmt.Errorf("tuple at index %d has incompatible schema", i)
		}
	}

	return nil
}

// validateTupleSlicesMatchLength checks if two tuple slices have the same length.
func validateTupleSlicesMatchLength(tuples1, tuples2 []*tuple.Tuple, name1, name2 string) error {
	if len(tuples1) != len(tuples2) {
		return fmt.Errorf("%s and %s must have the same length", name1, name2)
	}
	return nil
}
