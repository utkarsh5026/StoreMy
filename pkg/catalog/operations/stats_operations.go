package operations

import (
	"errors"
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"time"
)

type TableStatistics = systemtable.TableStatistics
type FileGetter = func(tableID int) (page.DbFile, error)

type StatsOperations struct {
	reader       catalogio.CatalogReader
	writer       catalogio.CatalogWriter
	statsTableID int // ID of CATALOG_STATISTICS table
	fileGetter   FileGetter
}

// NewStatsOperations creates a new StatsOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - statsTableID: ID of the CATALOG_STATISTICS system table
//   - fileGetter: Function to retrieve DbFile by table ID
func NewStatsOperations(access catalogio.CatalogAccess, statsTableID int, fileGetter FileGetter) *StatsOperations {
	return &StatsOperations{
		reader:       access,
		writer:       access,
		statsTableID: statsTableID,
		fileGetter:   fileGetter,
	}
}

// UpdateTableStatistics collects and updates statistics for a given table
// Also updates the cache with fresh statistics
func (so *StatsOperations) UpdateTableStatistics(tx TxContext, tableID int) error {
	stats, err := so.collectTableStatistics(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to collect statistics for table %d: %w", tableID, err)
	}

	existingStats, err := so.GetTableStatistics(tx, tableID)
	if err == nil && existingStats != nil {
		if err := so.updateExistingStatistics(tx, tableID, stats); err != nil {
			return err
		}
	} else {
		if err := so.insertNewStatistics(tx, stats); err != nil {
			return err
		}
	}
	return nil
}

// collectTableStatistics scans a table and computes its statistics
func (so *StatsOperations) collectTableStatistics(tx TxContext, tableID int) (*systemtable.TableStatistics, error) {
	file, err := so.fileGetter(tableID)
	if err != nil {
		return nil, err
	}

	pageCount, err := getHeapPageCount(file)
	if err != nil {
		return nil, err
	}

	stats := &systemtable.TableStatistics{
		TableID:     tableID,
		Cardinality: 0,
		PageCount:   pageCount,
		LastUpdated: time.Now(),
	}

	totalSize := 0
	distinctMap := make(map[string]bool)

	err = so.reader.IterateTable(tableID, tx, func(t *tuple.Tuple) error {
		stats.Cardinality++
		td := t.TupleDesc
		totalSize += int(td.GetSize())

		if td.NumFields() == 0 {
			return nil
		}

		// Track distinct values for the first field as a sample
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
func (so *StatsOperations) GetTableStatistics(tx TxContext, tableID int) (*TableStatistics, error) {
	var result *TableStatistics

	err := so.iterateTable(tx, func(t *TableStatistics) error {
		if t.TableID != tableID {
			return nil
		}
		result = t
		return ErrSuccess
	})

	if err != nil && errors.Is(err, ErrSuccess) {
		return result, nil
	}

	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("statistics for table %d not found", tableID)
}

// insertNewStatistics creates a new statistics entry
func (so *StatsOperations) insertNewStatistics(tx TxContext, stats *TableStatistics) error {
	tup := systemtable.Stats.CreateTuple(stats)
	if err := so.writer.InsertRow(so.statsTableID, tx, tup); err != nil {
		return fmt.Errorf("failed to insert statistics: %w", err)
	}
	return nil
}

// updateExistingStatistics updates an existing statistics entry
func (so *StatsOperations) updateExistingStatistics(tx *transaction.TransactionContext, tableID int, stats *TableStatistics) error {
	if err := so.deleteTableStats(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete old statistics: %w", err)
	}
	return so.insertNewStatistics(tx, stats)
}

func (so *StatsOperations) deleteTableStats(tx TxContext, tableID int) error {
	var tuplesToDelete []*tuple.Tuple

	so.iterateTable(tx, func(t *TableStatistics) error {
		if tableID != t.TableID {
			return nil
		}

		tuplesToDelete = append(tuplesToDelete, systemtable.Stats.CreateTuple(t))
		return nil
	})

	for _, tup := range tuplesToDelete {
		if err := so.writer.DeleteRow(so.statsTableID, tx, tup); err != nil {
			return err
		}
	}
	return nil
}

func (so *StatsOperations) iterateTable(tx TxContext, processFunc func(t *TableStatistics) error) error {
	return so.reader.IterateTable(so.statsTableID, tx, func(statsTuple *tuple.Tuple) error {
		stats, err := systemtable.Stats.Parse(statsTuple)
		if err != nil {
			return fmt.Errorf("failed to parse statistics tuple: %v", err)
		}

		if err = processFunc(stats); err != nil {
			return fmt.Errorf("failed to process statistics tuple: %v", err)
		}
		return nil
	})
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
