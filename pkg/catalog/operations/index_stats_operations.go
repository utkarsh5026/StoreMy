package operations

import (
	"errors"
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

type IndexStatistics = systemtable.IndexStatisticsRow

// IndexStatsOperations handles all index statistics-related catalog operations.
// It depends only on the CatalogAccess interface, making it testable
// and decoupled from the concrete SystemCatalog implementation.
type IndexStatsOperations struct {
	reader            catalogio.CatalogReader
	writer            catalogio.CatalogWriter
	indexStatsTableID int // ID of CATALOG_INDEX_STATISTICS table
	indexTableID      int // ID of CATALOG_INDEXES table
	fileGetter        FileGetter
	statsOps          *StatsOperations // For accessing table statistics
}

// NewIndexStatsOperations creates a new IndexStatsOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - indexStatsTableID: ID of the CATALOG_INDEX_STATISTICS system table
//   - indexTableID: ID of the CATALOG_INDEXES system table
//   - fileGetter: Function to retrieve DbFile by table ID
//   - statsOps: StatsOperations instance for accessing table statistics (optional)
func NewIndexStatsOperations(access catalogio.CatalogAccess, indexStatsTableID, indexTableID int, fileGetter FileGetter, statsOps *StatsOperations) *IndexStatsOperations {
	return &IndexStatsOperations{
		reader:            access,
		writer:            access,
		indexStatsTableID: indexStatsTableID,
		indexTableID:      indexTableID,
		fileGetter:        fileGetter,
		statsOps:          statsOps,
	}
}

// CollectIndexStatistics collects statistics for a specific index
func (iso *IndexStatsOperations) CollectIndexStatistics(
	tx TxContext,
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

	// Get table statistics for base metrics
	if iso.statsOps != nil {
		tableStats, err := iso.statsOps.GetTableStatistics(tx, tableID)
		if err == nil && tableStats != nil {
			stats.NumEntries = int64(tableStats.Cardinality)
			stats.NumPages = tableStats.PageCount / 10 // Rough estimate
		}
	}

	// Column statistics integration - placeholder for now
	// TODO: When column statistics are fully integrated, use:
	// colStats, err := iso.getColumnStatistics(tx, tableID, columnName)
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
			stats.BTreeHeight = int(logBase(float64(stats.NumEntries), 100)) + 1
		} else {
			stats.BTreeHeight = 1
		}
	case index.HashIndex:
		stats.BTreeHeight = 1 // Hash indexes are single-level
	default:
		stats.BTreeHeight = 1
	}

	// Calculate clustering factor
	clusteringFactor, err := iso.calculateClusteringFactor(tx, tableID, indexID, columnName)
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
func (iso *IndexStatsOperations) calculateClusteringFactor(
	tx TxContext,
	tableID int,
	indexID int,
	columnName string,
) (float64, error) {
	// Get the table file to access heap pages
	tableFile, err := iso.fileGetter(tableID)
	if err != nil || tableFile == nil {
		// Can't calculate without table file, return default
		return 0.5, nil
	}

	// Scan the table and track index order vs physical order
	var entries []struct {
		keyValue  types.Field
		pageID    int
		tupleNum  int
	}

	// Iterate through all tuples in the table
	err = iso.reader.IterateTable(tableID, tx, func(t *tuple.Tuple) error {
		// Extract the indexed column value
		td := t.TupleDesc
		fieldIndex := -1
		for i := 0; i < td.NumFields(); i++ {
			name, _ := td.GetFieldName(i)
			if name == columnName {
				fieldIndex = i
				break
			}
		}

		if fieldIndex == -1 {
			// Column not found, skip this tuple
			return nil
		}

		keyValue, err := t.GetField(fieldIndex)
		if err != nil {
			return nil
		}
		rid := t.RecordID

		if rid != nil {
			entries = append(entries, struct {
				keyValue  types.Field
				pageID    int
				tupleNum  int
			}{
				keyValue: keyValue,
				pageID:   rid.PageID.PageNo(),
				tupleNum: rid.TupleNum,
			})
		}

		return nil
	})

	if err != nil {
		return 0.5, fmt.Errorf("failed to iterate table: %w", err)
	}

	if len(entries) <= 1 {
		// Empty or single entry table is perfectly clustered
		return 1.0, nil
	}

	// Sort entries by key value to simulate index scan order
	sortEntriesByKey(entries)

	// Count page switches as we follow the index order
	pageSwitches := 0
	lastPageID := entries[0].pageID

	for i := 1; i < len(entries); i++ {
		if entries[i].pageID != lastPageID {
			pageSwitches++
			lastPageID = entries[i].pageID
		}
	}

	// Calculate clustering factor
	// Perfect clustering = 0 switches (or minimal switches)
	// Random clustering = many switches
	clusteringFactor := 1.0 - (float64(pageSwitches) / float64(len(entries)-1))

	// Ensure result is in valid range [0, 1]
	if clusteringFactor < 0.0 {
		clusteringFactor = 0.0
	}
	if clusteringFactor > 1.0 {
		clusteringFactor = 1.0
	}

	return clusteringFactor, nil
}

// sortEntriesByKey sorts entries by their key value
func sortEntriesByKey(entries []struct {
	keyValue  types.Field
	pageID    int
	tupleNum  int
}) {
	// Simple bubble sort (can be replaced with a more efficient sort if needed)
	n := len(entries)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if compareFields(entries[j].keyValue, entries[j+1].keyValue) > 0 {
				entries[j], entries[j+1] = entries[j+1], entries[j]
			}
		}
	}
}

// compareFields compares two fields and returns:
// -1 if a < b, 0 if a == b, 1 if a > b
func compareFields(a, b types.Field) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Use the Field's built-in Compare method
	// Check equality first
	eq, err := a.Compare(primitives.Equals, b)
	if err == nil && eq {
		return 0
	}

	// Check less than
	lt, err := a.Compare(primitives.LessThan, b)
	if err == nil && lt {
		return -1
	}

	// Must be greater than
	return 1
}

// UpdateIndexStatistics updates statistics for all indexes on a table
func (iso *IndexStatsOperations) UpdateIndexStatistics(tx TxContext, tableID int) error {
	indexes, err := iso.getIndexesForTable(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes: %w", err)
	}

	for _, idx := range indexes {
		stats, err := iso.CollectIndexStatistics(
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
		if err := iso.StoreIndexStatistics(tx, stats); err != nil {
			return fmt.Errorf("failed to store index statistics: %w", err)
		}
	}

	return nil
}

// StoreIndexStatistics stores index statistics in CATALOG_INDEX_STATISTICS
// Handles both insert (new statistics) and update (existing statistics) cases
func (iso *IndexStatsOperations) StoreIndexStatistics(tx TxContext, stats *IndexStatistics) error {
	existingStats, err := iso.GetIndexStatistics(tx, stats.IndexID)
	if err == nil && existingStats != nil {
		if err := iso.deleteIndexStatistics(tx, stats.IndexID); err != nil {
			return fmt.Errorf("failed to delete old index statistics: %w", err)
		}
	}

	// Create tuple from statistics row
	tup := systemtable.IndexStats.CreateTuple(stats)

	// Insert the tuple
	if err := iso.writer.InsertRow(iso.indexStatsTableID, tx, tup); err != nil {
		return fmt.Errorf("failed to insert index statistics: %w", err)
	}

	return nil
}

// deleteIndexStatistics removes index statistics for a specific index
func (iso *IndexStatsOperations) deleteIndexStatistics(
	tx TxContext,
	indexID int,
) error {
	var tuplesToDelete []*tuple.Tuple

	err := iso.iterateIndexStats(tx, func(stats *IndexStatistics) error {
		if stats.IndexID != indexID {
			return nil
		}
		tuplesToDelete = append(tuplesToDelete, systemtable.IndexStats.CreateTuple(stats))
		return nil
	})

	if err != nil {
		return err
	}

	for _, tup := range tuplesToDelete {
		if err := iso.writer.DeleteRow(iso.indexStatsTableID, tx, tup); err != nil {
			return err
		}
	}
	return nil
}

// GetIndexStatistics retrieves statistics for a specific index from CATALOG_INDEX_STATISTICS
func (iso *IndexStatsOperations) GetIndexStatistics(
	tx TxContext,
	indexID int,
) (*IndexStatistics, error) {
	var result *IndexStatistics

	err := iso.iterateIndexStats(tx, func(stats *IndexStatistics) error {
		if stats.IndexID != indexID {
			return nil
		}
		result = stats
		return ErrSuccess
	})

	if err != nil && errors.Is(err, ErrSuccess) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("index statistics not found for index %d", indexID)
}

// iterateIndexStats is a helper function to iterate over index statistics
func (iso *IndexStatsOperations) iterateIndexStats(tx TxContext, processFunc func(*IndexStatistics) error) error {
	return iso.reader.IterateTable(iso.indexStatsTableID, tx, func(statsTuple *tuple.Tuple) error {
		statsRow, err := systemtable.IndexStats.Parse(statsTuple)
		if err != nil {
			return fmt.Errorf("failed to parse index statistics tuple: %w", err)
		}

		if err = processFunc(statsRow); err != nil {
			if errors.Is(err, ErrSuccess) {
				return err
			}
			return fmt.Errorf("failed to process index statistics tuple: %w", err)
		}
		return nil
	})
}

// getIndexesForTable retrieves all indexes for a table
func (iso *IndexStatsOperations) getIndexesForTable(tx TxContext, tableID int) ([]*systemtable.IndexMetadata, error) {
	var indexes []*systemtable.IndexMetadata

	err := iso.reader.IterateTable(iso.indexTableID, tx, func(t *tuple.Tuple) error {
		im, err := systemtable.Indexes.Parse(t)
		if err != nil {
			return fmt.Errorf("error in parsing the index table: %w", err)
		}
		if im.TableID == tableID {
			indexes = append(indexes, im)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return indexes, nil
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
