package systable

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"time"
)

// TableStatistics holds statistics about a table for query optimization
type TableStatistics struct {
	TableID        primitives.FileID     // Table identifier
	Cardinality    uint64                // Number of tuples in the table
	PageCount      primitives.PageNumber // Number of pages used by the table
	AvgTupleSize   uint64                // Average tuple size in bytes
	LastUpdated    time.Time             // When statistics were last updated
	DistinctValues uint64                // Approximate number of distinct values (for primary key)
	NullCount      uint64                // Number of null values
	MinValue       string                // Min value for primary key (if applicable)
	MaxValue       string                // Max value for primary key (if applicable)
}

type StatsTable struct {
	*BaseOperations[TableStatistics]
	reader     catalogio.CatalogReader // Reads tuples from catalog tables
	fileGetter FileGetter              // Retrieves DbFile for a table ID
	cache      StatsCacheSetter        // Optional statistics cache
	colsTable  *ColumnsTable
}

type FileGetter = func(tableID primitives.FileID) (page.DbFile, error)

// StatsCacheSetter defines the interface for caching table statistics.
// Implementations should handle concurrent access safely.
type StatsCacheSetter interface {
	SetCachedStatistics(tableID primitives.FileID, stats *TableStatistics) error
}

// NewStatsOperations creates a new StatsTable instance.
func NewStatsOperations(access catalogio.CatalogAccess, id primitives.FileID, f FileGetter, cache StatsCacheSetter, colsTable *ColumnsTable) *StatsTable {
	return &StatsTable{
		reader:         access,
		fileGetter:     f,
		cache:          cache,
		colsTable:      colsTable,
		BaseOperations: NewBaseOperations(access, id, CatalogStatisticsTableDescriptor),
	}
}

// UpdateTableStatistics collects fresh statistics for a table and persists them.
// If statistics already exist, they are updated; otherwise, new statistics are inserted.
func (so *StatsTable) UpdateTableStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	stats, err := so.collectStats(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to collect statistics for table %d: %w", tableID, err)
	}

	existingStats, err := so.GetTableStatistics(tx, tableID)
	if err == nil && existingStats != (TableStatistics{}) {
		if err := so.update(tx, tableID, stats); err != nil {
			return err
		}
	} else {
		if err := so.insert(tx, stats); err != nil {
			return err
		}
	}

	if so.cache != nil {
		_ = so.cache.SetCachedStatistics(tableID, &stats)
	}
	return nil
}

// getPrimaryKeyIndex finds the primary key column index for a table by querying CATALOG_COLUMNS.
// Returns -1 if no primary key is found.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table to find primary key for
//
// Returns:
//   - int: Column index of the primary key, or -1 if not found
//   - error: nil on success, or error if catalog read fails
func (so *StatsTable) getPrimaryKeyIndex(tx TxContext, tableID primitives.FileID) (primitives.ColumnID, error) {
	var primaryKeyIndex primitives.ColumnID = 0

	err := so.reader.IterateTable(so.colsTable.TableID(), tx, func(t *tuple.Tuple) error {
		col, err := ColumnsTableDescriptor.ParseTuple(t)
		if err != nil {
			return nil
		}

		if col.TableID != tableID {
			return nil
		}

		if col.IsPrimary {
			primaryKeyIndex = col.Position
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to query primary key: %w", err)
	}

	return primaryKeyIndex, nil
}

// collectStats performs a full table scan to compute statistics.
//
// Collected metrics:
//   - Cardinality: Total number of tuples
//   - PageCount: Number of pages in the heap file
//   - AvgTupleSize: Average tuple size in bytes
//   - DistinctValues: Number of distinct values in the primary key column (for selectivity estimation)
//   - LastUpdated: Timestamp of collection
//
// Parameters:
//   - tx: Transaction context for reading the table
//   - tableID: ID of the table to scan
//
// Returns:
//   - *TableStatistics: Collected statistics
//   - error: nil on success, or error if scan fails
func (so *StatsTable) collectStats(tx TxContext, tableID primitives.FileID) (TableStatistics, error) {
	file, err := so.fileGetter(tableID)
	var zero TableStatistics
	if err != nil {
		return zero, err
	}

	pageCount, err := getHeapPageCount(file)
	if err != nil {
		return zero, err
	}

	primaryKeyIndex, err := so.getPrimaryKeyIndex(tx, tableID)
	if err != nil {
		return zero, fmt.Errorf("failed to get primary key index: %w", err)
	}

	stats := &TableStatistics{
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

		fieldCount := td.NumFields()

		if fieldCount == 0 {
			return nil
		}

		// Track distinct values in the primary key column for selectivity estimation
		// If no primary key exists, use the first column as fallback
		columnIndex := max(primaryKeyIndex, 0)

		if columnIndex < primitives.ColumnID(fieldCount) {
			field, err := t.GetField(columnIndex)
			if err == nil && field != nil {
				distinctMap[field.String()] = true
			}
		}

		return nil
	})

	if err != nil {
		return zero, err
	}

	if stats.Cardinality > 0 {
		stats.AvgTupleSize = uint64(totalSize) / stats.Cardinality
	}
	stats.DistinctValues = uint64(len(distinctMap))

	return *stats, nil
}

// GetTableStatistics retrieves persisted statistics for a table.
// Does not trigger statistics collection - returns existing data only.
//
// Parameters:
//   - tx: Transaction context for the read
//   - tableID: ID of the table
//
// Returns:
//   - *TableStatistics: Statistics record if found
//   - error: error if statistics not found or read fails
func (so *StatsTable) GetTableStatistics(tx TxContext, tableID primitives.FileID) (TableStatistics, error) {
	result, err := so.FindOne(tx, func(t TableStatistics) bool {
		return t.TableID == tableID
	})

	if err != nil {
		return TableStatistics{}, fmt.Errorf("statistics for table %d not found", tableID)
	}

	return result, nil
}

// insert creates a new statistics entry in the CATALOG_STATISTICS table.
//
// Parameters:
//   - tx: Transaction context
//   - stats: Statistics to insert
//
// Returns:
//   - error: nil on success, or error if insertion fails
func (so *StatsTable) insert(tx TxContext, stats TableStatistics) error {
	if err := so.Insert(tx, stats); err != nil {
		return fmt.Errorf("failed to insert statistics: %w", err)
	}
	return nil
}

// update replaces existing statistics for a table.
//
// Parameters:
//   - tx: Transaction context
//   - tableID: ID of the table to update
//   - stats: New statistics values
//
// Returns:
//   - error: nil on success, or error if update fails
func (so *StatsTable) update(tx TxContext, tableID primitives.FileID, stats TableStatistics) error {
	err := so.Upsert(tx, func(t TableStatistics) bool {
		return t.TableID == tableID
	}, stats)

	if err != nil {
		return fmt.Errorf("failed to update statistics: %w", err)
	}
	return nil
}

// getHeapPageCount retrieves the number of pages in a heap file.
// This is used to track storage footprint for cost estimation.
//
// Parameters:
//   - file: DbFile to inspect (must be a HeapFile)
//
// Returns:
//   - int: Number of pages, or 0 for non-heap files
//   - error: nil on success, or error if page count retrieval fails
func getHeapPageCount(file page.DbFile) (primitives.PageNumber, error) {
	hf, ok := file.(*heap.HeapFile)
	if !ok {
		return 0, nil
	}

	pageCount, err := hf.NumPages()
	if err != nil {
		return 0, fmt.Errorf("failed to get page count: %w", err)
	}

	return pageCount, nil
}
