package catalog

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"time"
)

type TableStatistics = systemtable.TableStatistics

// UpdateTableStatistics collects and updates statistics for a given table
func (sc *SystemCatalog) UpdateTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	stats, err := sc.collectTableStatistics(tx.ID, tableID)
	if err != nil {
		return fmt.Errorf("failed to collect statistics for table %d: %w", tableID, err)
	}

	existingStats, err := sc.GetTableStatistics(tx.ID, tableID)
	if err == nil && existingStats != nil {
		return sc.updateExistingStatistics(tx, tableID, stats)
	}

	return sc.insertNewStatistics(tx, stats)
}

// collectTableStatistics scans a table and computes its statistics
func (sc *SystemCatalog) collectTableStatistics(tid *primitives.TransactionID, tableID int) (*TableStatistics, error) {
	file, err := sc.tableManager.GetDbFile(tableID)
	if err != nil {
		return nil, err
	}

	pageCount, err := getHeapPageCount(file)
	if err != nil {
		return nil, err
	}

	stats := &TableStatistics{
		TableID:     tableID,
		Cardinality: 0,
		PageCount:   pageCount,
		LastUpdated: time.Now(),
	}

	totalSize := 0
	distinctMap := make(map[string]bool)

	err = sc.iterateTable(sc.statisticsTableID, tid, func(t *tuple.Tuple) error {
		stats.Cardinality++
		td := t.TupleDesc
		totalSize += int(td.GetSize())

		if td.NumFields() == 0 {
			return nil
		}

		field, err := t.GetField(0)
		if err == nil && field != nil {
			distinctMap[field.String()] = true
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if stats.Cardinality > 0 {
		stats.AvgTupleSize = totalSize / stats.Cardinality
	}
	stats.DistinctValues = len(distinctMap)

	return stats, nil
}

// GetTableStatistics retrieves statistics for a given table
func (sc *SystemCatalog) GetTableStatistics(tid *primitives.TransactionID, tableID int) (*TableStatistics, error) {
	var result *TableStatistics

	err := sc.iterateTable(sc.statisticsTableID, tid, func(statsTuple *tuple.Tuple) error {
		storedTableID, err := systemtable.Stats.GetTableID(statsTuple)
		if err != nil {
			return err
		}

		if storedTableID != tableID {
			return nil
		}

		result, err = systemtable.Stats.Parse(statsTuple)
		if err != nil {
			return err
		}
		return fmt.Errorf("found")
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("statistics for table %d not found", tableID)
}

// insertNewStatistics creates a new statistics entry
func (sc *SystemCatalog) insertNewStatistics(tx *transaction.TransactionContext, stats *TableStatistics) error {
	tup := systemtable.Stats.CreateTuple(stats)
	if err := sc.store.InsertTuple(tx, sc.statisticsTableID, tup); err != nil {
		return fmt.Errorf("failed to insert statistics: %w", err)
	}
	return nil
}

// updateExistingStatistics updates an existing statistics entry
func (sc *SystemCatalog) updateExistingStatistics(tx *transaction.TransactionContext, tableID int, stats *TableStatistics) error {
	if err := sc.deleteTableStatistics(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete old statistics: %w", err)
	}
	return sc.insertNewStatistics(tx, stats)
}

// deleteTableStatistics removes statistics for a table
func (sc *SystemCatalog) deleteTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	var tuplesToDelete []*tuple.Tuple
	err := sc.iterateTable(sc.statisticsTableID, tx.ID, func(t *tuple.Tuple) error {
		storedTableID, err := systemtable.Stats.GetTableID(t)
		if err != nil {
			return err
		}

		if storedTableID == tableID {
			tuplesToDelete = append(tuplesToDelete, t)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to collect statistics tuples: %w", err)
	}

	for _, tup := range tuplesToDelete {
		if err := sc.store.DeleteTuple(tx, tup); err != nil {
			return fmt.Errorf("failed to delete statistics tuple: %w", err)
		}
	}

	return nil
}

func getHeapPageCount(file page.DbFile) (int, error) {
	pageCount := 0
	if hf, ok := file.(*heap.HeapFile); ok {
		pc, err := hf.NumPages()
		if err != nil {
			return -1, fmt.Errorf("failed to get page count: %w", err)
		}
		pageCount = pc
	}
	return pageCount, nil
}
