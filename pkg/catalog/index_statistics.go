package catalog

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// IndexStatistics represents statistics about an index for query optimization
type IndexStatistics struct {
	IndexID          int             // Index identifier
	TableID          int             // Table this index belongs to
	IndexName        string          // Index name
	IndexType        index.IndexType // "btree", "hash", etc.
	ColumnName       string          // Indexed column name
	NumEntries       int64           // Number of entries in the index
	NumPages         int             // Number of pages used by index
	Height           int             // Tree height (for B-tree indexes)
	DistinctKeys     int64           // Number of distinct keys
	ClusteringFactor float64         // 0.0-1.0: how ordered is table by this index
	AvgKeySize       int             // Average key size in bytes
	LastUpdated      time.Time       // Last update timestamp
}

// CollectIndexStatistics collects statistics for a specific index
func (sc *SystemCatalog) CollectIndexStatistics(
	tx *transaction.TransactionContext,
	indexID, tableID int,
	indexName string,
	indexType index.IndexType,
	columnName string,
) (*IndexStatistics, error) {
	stats := &IndexStatistics{
		IndexID:     indexID,
		TableID:     tableID,
		IndexName:   indexName,
		IndexType:   indexType,
		ColumnName:  columnName,
		LastUpdated: time.Now(),
	}

	tableStats, err := sc.GetTableStatistics(tx, tableID)
	if err == nil && tableStats != nil {
		stats.NumEntries = int64(tableStats.Cardinality)
		stats.NumPages = tableStats.PageCount / 10 // Rough estimate
	}

	// Column statistics integration - placeholder for now
	// TODO: When column statistics are fully integrated, use:
	// colStats, err := sc.GetColumnStatistics(tx, tableID, columnName)
	// if err == nil && colStats != nil {
	//     stats.DistinctKeys = colStats.DistinctCount
	//     stats.AvgKeySize = colStats.AvgKeySize
	// }

	// For now, set reasonable defaults
	stats.DistinctKeys = stats.NumEntries / 10 // Rough estimate
	stats.AvgKeySize = 8                       // Default key size

	// Set type-specific defaults
	switch indexType {
	case index.BTreeIndex:
		if stats.NumEntries > 0 {
			stats.Height = int(logBase(float64(stats.NumEntries), 100)) + 1
		} else {
			stats.Height = 1
		}
	case index.HashIndex:
		stats.Height = 1 // Hash indexes are single-level
	default:
		stats.Height = 1
	}

	// Calculate clustering factor
	clusteringFactor, err := sc.calculateClusteringFactor(tx, tableID, indexID, columnName)
	if err != nil {
		// Non-fatal: use default clustering factor
		stats.ClusteringFactor = 0.5
	} else {
		stats.ClusteringFactor = clusteringFactor
	}

	return stats, nil
}

// calculateClusteringFactor calculates how well the table is ordered by the index
// Returns a value between 0.0 (random) and 1.0 (perfectly clustered)
//
// Clustering factor is calculated as:
// CF = 1 - (number of page switches / number of entries)
//
// A high clustering factor means scanning the index results in sequential
// table page access (good for range scans), while a low factor means random access.
func (sc *SystemCatalog) calculateClusteringFactor(
	tx *transaction.TransactionContext,
	tableID int,
	indexID int,
	columnName string,
) (float64, error) {
	// This is a simplified implementation
	// In practice, scan the index in order and track how many times
	// the table page changes as you follow the index entries

	// For now, return a default value
	// TODO: Implement proper clustering factor calculation
	return 0.5, nil
}

// UpdateIndexStatistics updates statistics for all indexes on a table
func (sc *SystemCatalog) UpdateIndexStatistics(tx *transaction.TransactionContext, tableID int) error {
	indexes, err := sc.GetIndexesForTable(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes: %w", err)
	}

	for _, idx := range indexes {
		stats, err := sc.CollectIndexStatistics(
			tx,
			idx.IndexID,
			idx.TableID,
			idx.IndexName,
			idx.IndexType,
			idx.ColumnName,
		)
		if err != nil {
			// Log error but continue with other indexes
			continue
		}

		// Store the statistics
		if err := sc.storeIndexStatistics(tx, stats); err != nil {
			return fmt.Errorf("failed to store index statistics: %w", err)
		}
	}

	return nil
}

// storeIndexStatistics stores index statistics in CATALOG_INDEX_STATISTICS
// Handles both insert (new statistics) and update (existing statistics) cases
func (sc *SystemCatalog) storeIndexStatistics(tx *transaction.TransactionContext, stats *IndexStatistics) error {
	existingStats, err := sc.GetIndexStatistics(tx, stats.IndexID)
	if err == nil && existingStats != nil {
		if err := sc.deleteIndexStatistics(tx, stats.IndexID); err != nil {
			return fmt.Errorf("failed to delete old index statistics: %w", err)
		}
	}

	// Convert IndexStatistics to IndexStatisticsRow for storage
	statsRow := &systemtable.IndexStatisticsRow{
		IndexID:          stats.IndexID,
		TableID:          stats.TableID,
		IndexName:        stats.IndexName,
		IndexType:        stats.IndexType,
		ColumnName:       stats.ColumnName,
		NumEntries:       stats.NumEntries,
		NumPages:         stats.NumPages,
		BTreeHeight:      stats.Height,
		DistinctKeys:     stats.DistinctKeys,
		ClusteringFactor: stats.ClusteringFactor,
		AvgKeySize:       stats.AvgKeySize,
		LastUpdated:      stats.LastUpdated,
	}

	// Create tuple from statistics row
	tup := systemtable.IndexStats.CreateTuple(statsRow)

	// Get the index statistics table file
	file, err := sc.cache.GetDbFile(sc.SystemTabs.IndexStatisticsTableID)
	if err != nil {
		return fmt.Errorf("failed to get index statistics table: %w", err)
	}

	// Insert the tuple
	if err := sc.tupMgr.InsertTuple(tx, file, tup); err != nil {
		return fmt.Errorf("failed to insert index statistics: %w", err)
	}

	return nil
}

// deleteIndexStatistics removes index statistics for a specific index
func (sc *SystemCatalog) deleteIndexStatistics(
	tx *transaction.TransactionContext,
	indexID int,
) error {
	return sc.DeleteTableFromSysTable(tx, indexID, sc.SystemTabs.IndexStatisticsTableID)
}

// GetIndexStatistics retrieves statistics for a specific index from CATALOG_INDEX_STATISTICS
func (sc *SystemCatalog) GetIndexStatistics(
	tx *transaction.TransactionContext,
	indexID int,
) (*IndexStatistics, error) {
	var result *IndexStatistics

	err := sc.iterateTable(sc.SystemTabs.IndexStatisticsTableID, tx, func(statsTuple *tuple.Tuple) error {
		storedIndexID, err := systemtable.IndexStats.GetIndexID(statsTuple)
		if err != nil {
			return err
		}

		if storedIndexID != indexID {
			return nil
		}

		statsRow, err := systemtable.IndexStats.Parse(statsTuple)
		if err != nil {
			return err
		}

		result = &IndexStatistics{
			IndexID:          statsRow.IndexID,
			TableID:          statsRow.TableID,
			IndexName:        statsRow.IndexName,
			IndexType:        statsRow.IndexType,
			ColumnName:       statsRow.ColumnName,
			NumEntries:       statsRow.NumEntries,
			NumPages:         statsRow.NumPages,
			Height:           statsRow.BTreeHeight,
			DistinctKeys:     statsRow.DistinctKeys,
			ClusteringFactor: statsRow.ClusteringFactor,
			AvgKeySize:       statsRow.AvgKeySize,
			LastUpdated:      statsRow.LastUpdated,
		}

		return fmt.Errorf("found")
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("index statistics not found for index %d", indexID)
}

// IndexInfo represents basic index metadata (already exists in catalog)
type IndexInfo struct {
	IndexID    int
	TableID    int
	IndexName  string
	IndexType  index.IndexType
	ColumnName string
	FilePath   string
}

// Helper function to calculate logarithm with arbitrary base
func logBase(x, base float64) float64 {
	if x <= 0 || base <= 1 {
		return 0
	}
	return log2(x) / log2(base)
}

func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Simple log2 approximation
	result := 0.0
	for x >= 2 {
		x /= 2
		result++
	}
	return result
}

// GetIndexesForTable retrieves all indexes for a table
func (sc *SystemCatalog) GetIndexesForTable(tx *transaction.TransactionContext, tableID int) ([]*IndexInfo, error) {
	var indexes []*IndexInfo

	err := sc.iterateTable(sc.SystemTabs.IndexesTableID, tx, func(t *tuple.Tuple) error {
		// Parse index tuple to get table ID
		// This is simplified - actual implementation would use systemtable.Indexes
		field, err := t.GetField(1) // Assuming field 1 is table_id
		if err != nil {
			return err
		}

		if intField, ok := field.(*types.IntField); ok {
			if int(intField.Value) == tableID {
				// Found an index for this table
				// TODO: Parse full index info
				indexes = append(indexes, &IndexInfo{
					TableID: tableID,
				})
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return indexes, nil
}
