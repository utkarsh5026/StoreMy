package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"time"
)

type tableStats = systemtable.TableStatistics
type FileGetter = func(tableID primitives.TableID) (page.DbFile, error)

// StatsCacheSetter defines the interface for caching table statistics.
// Implementations should handle concurrent access safely.
type StatsCacheSetter interface {
	SetCachedStatistics(tableID primitives.TableID, stats *tableStats) error
}

// StatsOperations manages table statistics in the CATALOG_STATISTICS system table.
// It provides functionality to collect, update, and retrieve table statistics including:
//   - Cardinality (number of tuples)
//   - Page count
//   - Average tuple size
//   - Distinct values (sampled from primary key column)
//
// Statistics are persisted to disk and optionally cached for performance.
// All operations respect transaction boundaries through the provided TxContext.
type StatsOperations struct {
	*BaseOperations[*tableStats]
	reader         catalogio.CatalogReader // Reads tuples from catalog tables
	fileGetter     FileGetter              // Retrieves DbFile for a table ID
	cache          StatsCacheSetter        // Optional statistics cache
	columnsTableID primitives.TableID      // ID of CATALOG_COLUMNS table for schema lookup
}

// NewStatsOperations creates a new StatsOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - statsTableID: ID of the CATALOG_STATISTICS system table
//   - columnsTableID: ID of the CATALOG_COLUMNS system table (for primary key lookup)
//   - fileGetter: Function to retrieve DbFile by table ID
//   - cache: Optional cache for storing statistics (can be nil)
//
// Returns:
//   - *StatsOperations: Configured operations instance
func NewStatsOperations(access catalogio.CatalogAccess, statsTableID, columnsTableID primitives.TableID, f FileGetter, cache StatsCacheSetter) *StatsOperations {
	base := NewBaseOperations(
		access,
		statsTableID,
		systemtable.Stats.Parse,
		systemtable.Stats.CreateTuple,
	)

	return &StatsOperations{
		BaseOperations: base,
		reader:         access,
		fileGetter:     f,
		cache:          cache,
		columnsTableID: columnsTableID,
	}
}

// UpdateTableStatistics collects fresh statistics for a table and persists them.
// If statistics already exist, they are updated; otherwise, new statistics are inserted.
// The cache is automatically updated with fresh statistics if a cache is configured.
//
// This operation performs a full table scan to collect accurate statistics.
//
// Parameters:
//   - tx: Transaction context for the operation
//   - tableID: ID of the table to update statistics for
//
// Returns:
//   - error: nil on success, or error if collection/persistence fails
func (so *StatsOperations) UpdateTableStatistics(tx TxContext, tableID primitives.TableID) error {
	stats, err := so.collectStats(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to collect statistics for table %d: %w", tableID, err)
	}

	existingStats, err := so.GetTableStatistics(tx, tableID)
	if err == nil && existingStats != nil {
		if err := so.update(tx, tableID, stats); err != nil {
			return err
		}
	} else {
		if err := so.insert(tx, stats); err != nil {
			return err
		}
	}

	if so.cache != nil {
		_ = so.cache.SetCachedStatistics(tableID, stats)
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
func (so *StatsOperations) getPrimaryKeyIndex(tx TxContext, tableID primitives.TableID) (primitives.ColumnID, error) {
	var primaryKeyIndex primitives.ColumnID = 0

	err := so.reader.IterateTable(so.columnsTableID, tx, func(t *tuple.Tuple) error {
		col, err := systemtable.Columns.Parse(t)
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
//   - *tableStats: Collected statistics
//   - error: nil on success, or error if scan fails
func (so *StatsOperations) collectStats(tx TxContext, tableID primitives.TableID) (*tableStats, error) {
	file, err := so.fileGetter(tableID)
	if err != nil {
		return nil, err
	}

	pageCount, err := getHeapPageCount(file)
	if err != nil {
		return nil, err
	}

	primaryKeyIndex, err := so.getPrimaryKeyIndex(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key index: %w", err)
	}

	stats := &tableStats{
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
		return nil, err
	}

	if stats.Cardinality > 0 {
		stats.AvgTupleSize = uint64(totalSize) / stats.Cardinality
	}
	stats.DistinctValues = uint64(len(distinctMap))

	return stats, nil
}

// GetTableStatistics retrieves persisted statistics for a table.
// Does not trigger statistics collection - returns existing data only.
//
// Parameters:
//   - tx: Transaction context for the read
//   - tableID: ID of the table
//
// Returns:
//   - *tableStats: Statistics record if found
//   - error: error if statistics not found or read fails
func (so *StatsOperations) GetTableStatistics(tx TxContext, tableID primitives.TableID) (*tableStats, error) {
	result, err := so.FindOne(tx, func(t *tableStats) bool {
		return t.TableID == tableID
	})

	if err != nil {
		return nil, fmt.Errorf("statistics for table %d not found", tableID)
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
func (so *StatsOperations) insert(tx TxContext, stats *tableStats) error {
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
func (so *StatsOperations) update(tx TxContext, tableID primitives.TableID, stats *tableStats) error {
	err := so.Upsert(tx, func(t *tableStats) bool {
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
