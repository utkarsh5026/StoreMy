package catalogmanager

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
)

// UpdateTableStatistics collects and updates statistics for a given table.
//
// This scans the entire table and computes:
//   - Cardinality (row count)
//   - Page count
//   - Average tuple size
//
// The statistics are stored in CATALOG_STATISTICS and also cached in memory.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//
// Returns error if statistics collection or update fails.
func (cm *CatalogManager) UpdateTableStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	return cm.StatsTable.UpdateTableStatistics(tx, tableID)
}

// GetTableStatistics retrieves statistics for a given table.
//
// This reads directly from disk via CATALOG_STATISTICS.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//
// Returns:
//   - *TableStatistics: Table statistics
//   - error: Error if statistics cannot be retrieved
func (cm *CatalogManager) GetTableStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) (*TableStatistics, error) {
	stats, err := cm.StatsTable.GetTableStatistics(tx, tableID)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

// RefreshStatistics updates statistics for a table and returns the updated stats.
//
// This is a convenience method that combines UpdateTableStatistics + GetTableStatistics.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//
// Returns:
//   - *TableStatistics: Updated table statistics
//   - error: Error if update or retrieval fails
func (cm *CatalogManager) RefreshStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) (*TableStatistics, error) {
	if err := cm.UpdateTableStatistics(tx, tableID); err != nil {
		return nil, err
	}
	return cm.GetTableStatistics(tx, tableID)
}

// CollectColumnStatistics collects statistics for a specific column in a table.
//
// This scans the column and computes:
//   - Distinct count (NDV)
//   - Null count
//   - Min/max values
//   - Average width (for variable-length types)
//   - Histogram (optional, if histogramBuckets > 0)
//   - Most common values (optional, if mcvCount > 0)
//
// The statistics are stored in CATALOG_COLUMN_STATISTICS.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//   - columnName: Name of the column
//   - columnIndex: Position of the column (0-based)
//   - histogramBuckets: Number of histogram buckets (0 to skip histogram)
//   - mcvCount: Number of most common values to track (0 to skip MCVs)
//
// Returns:
//   - *ColumnStatistics: Collected column statistics
//   - error: Error if collection fails
func (cm *CatalogManager) CollectColumnStatistics(
	tx *transaction.TransactionContext,
	tableID primitives.FileID,
	columnName string,
	columnIndex primitives.ColumnID,
	histogramBuckets int,
	mcvCount int,
) (*ColumnStatistics, error) {
	info, err := cm.ColumnStatsTable.CollectColumnStatistics(tx, columnName, tableID, columnIndex, histogramBuckets, mcvCount)
	if err != nil {
		return nil, err
	}
	return toColumnStatistics(info), nil
}

// UpdateColumnStatistics updates statistics for all columns in a table.
//
// This calls CollectColumnStatistics for each column in the table.
// Histogram and MCV collection are delegated to the ColumnStatsTable implementation.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//
// Returns error if any column statistics update fails.
func (cm *CatalogManager) UpdateColumnStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	return cm.ColumnStatsTable.UpdateColumnStatistics(tx, tableID)
}

// GetColumnStatistics retrieves statistics for a specific column from CATALOG_COLUMN_STATISTICS.
//
// Note: Histogram and MCV data are not stored in the system table and will be nil.
// Use CollectColumnStatistics to rebuild these on demand if needed.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//   - columnName: Name of the column
//
// Returns:
//   - *ColumnStatistics: Column statistics (with nil histogram and MCVs), or nil if not found
//   - error: Error if statistics retrieval fails unexpectedly
func (cm *CatalogManager) GetColumnStatistics(
	tx *transaction.TransactionContext,
	tableID primitives.FileID,
	columnName string,
) (*ColumnStatistics, error) {
	if cm.ColumnStatsTable == nil {
		return nil, nil
	}

	row, err := cm.ColumnStatsTable.GetColumnStatistics(tx, tableID, columnName)
	if err != nil {
		// Not found is acceptable - return nil without error
		return nil, nil
	}

	// Convert row to ColumnStatistics (Histogram and MCVs will be nil)
	return &ColumnStatistics{
		TableID:        row.TableID,
		ColumnName:     row.ColumnName,
		ColumnIndex:    row.ColumnIndex,
		DistinctCount:  row.DistinctCount,
		NullCount:      row.NullCount,
		MinValue:       stringToField(row.MinValue),
		MaxValue:       stringToField(row.MaxValue),
		AvgWidth:       row.AvgWidth,
		Histogram:      nil, // Not stored in system table
		MostCommonVals: nil, // Not stored in system table
		MCVFreqs:       nil, // Not stored in system table
		LastUpdated:    row.LastUpdated,
	}, nil
}

// CollectIndexStatistics collects statistics for a specific index.
//
// This scans the index and computes:
//   - Number of entries
//   - Index height (for B-Tree)
//   - Number of pages
//   - Average key size
//
// The statistics are stored in CATALOG_INDEX_STATISTICS.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - indexID: ID of the index
//   - tableID: ID of the table this index belongs to
//   - indexName: Name of the index
//   - indexType: Type of index (B-Tree, Hash, etc.)
//   - columnName: Name of the indexed column
//
// Returns:
//   - *IndexStatistics: Collected index statistics
//   - error: Error if collection fails
func (cm *CatalogManager) CollectIndexStatistics(
	tx *transaction.TransactionContext,
	indexID primitives.FileID,
	tableID primitives.FileID,
	indexName string,
	indexType index.IndexType,
	columnName string,
) (*IndexStatistics, error) {
	stats, err := cm.IndexesStatsTable.CollectIndexStatistics(tx, indexID, tableID, indexName, indexType, columnName)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

// UpdateIndexStatistics updates statistics for all indexes on a table.
//
// This calls CollectIndexStatistics for each index on the table.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//
// Returns error if any index statistics update fails.
func (cm *CatalogManager) UpdateIndexStatistics(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	return cm.IndexesStatsTable.UpdateIndexStatistics(tx, tableID)
}

// GetIndexStatistics retrieves statistics for a specific index from CATALOG_INDEX_STATISTICS.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexID: ID of the index
//
// Returns:
//   - *IndexStatistics: Index statistics
//   - error: Error if statistics cannot be retrieved
func (cm *CatalogManager) GetIndexStatistics(
	tx *transaction.TransactionContext,
	indexID primitives.FileID,
) (*IndexStatistics, error) {
	stats, err := cm.IndexesStatsTable.GetIndexStatistics(tx, indexID)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}
