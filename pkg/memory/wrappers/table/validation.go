package table

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/utils"
)

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

func validateBasic(opType string, tx *transaction.TransactionContext, dbFile page.DbFile, tuples []*tuple.Tuple, executed bool) error {
	if executed {
		return fmt.Errorf("%s operation already executed", opType)
	}

	if tx == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	// CRITICAL: Must use IsNilInterface instead of dbFile == nil
	// In Go, an interface wrapping a nil pointer is NOT equal to nil.
	// When a nil *heap.HeapFile is passed as page.DbFile interface,
	// dbFile == nil returns false (interface has type info), but the
	// underlying value is nil, causing panics when methods are called.
	if utils.IsNilInterface(dbFile) {
		return fmt.Errorf("dbFile cannot be nil")
	}

	if len(tuples) == 0 {
		return fmt.Errorf("no tuples to %s", opType)
	}

	return nil
}
