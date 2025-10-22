package operations

import (
	"fmt"
	"log"
	"math"
	"sort"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"time"
)

type (
	IndexStatistics = systemtable.IndexStatisticsRow
	indexMetadata   = systemtable.IndexMetadata
)

const (
	defaultDistinctKeysDivisor = 10  // Rough estimate: assume 10% uniqueness
	defaultAvgKeySize          = 8   // Default key size in bytes
	defaultClusteringFactor    = 0.5 // Default clustering factor (0.0 = random, 1.0 = perfectly clustered)
	defaultIndexPagesEstimate  = 10  // Rough estimate divisor for index pages from table pages
	btreeFanout                = 100 // Typical B-Tree node fanout/capacity
	minClusteringFactor        = 0.0 // Minimum clustering factor value
	maxClusteringFactor        = 1.0 // Maximum clustering factor value
)

// indexEntry represents a single entry in the index scan order
// Used for calculating clustering factor by tracking physical location
type indexEntry struct {
	keyValue types.Field // The indexed column value
	pageID   int         // Physical page ID where tuple is stored
	tupleNum int         // Tuple number within the page
}

// IndexStatsOperations handles all index statistics-related catalog operations.
// It depends only on the CatalogAccess interface, making it testable
// and decoupled from the concrete SystemCatalog implementation.
type IndexStatsOperations struct {
	*BaseOperations[*IndexStatistics]
	indexOps     *BaseOperations[*indexMetadata] // For querying CATALOG_INDEXES table
	indexTableID int                             // ID of CATALOG_INDEXES table
	fileGetter   FileGetter
	statsOps     *StatsOperations // For accessing table statistics
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
	base := NewBaseOperations(
		access,
		indexStatsTableID,
		systemtable.IndexStats.Parse,
		systemtable.IndexStats.CreateTuple,
	)

	indexOps := NewBaseOperations(
		access,
		indexTableID,
		systemtable.Indexes.Parse,
		func(im *indexMetadata) *Tuple {
			return systemtable.Indexes.CreateTuple(*im)
		},
	)

	return &IndexStatsOperations{
		BaseOperations: base,
		indexOps:       indexOps,
		indexTableID:   indexTableID,
		fileGetter:     fileGetter,
		statsOps:       statsOps,
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

	if iso.statsOps != nil {
		tableStats, err := iso.statsOps.GetTableStatistics(tx, tableID)
		if err == nil && tableStats != nil {
			stats.NumEntries = int64(tableStats.Cardinality)
			stats.NumPages = tableStats.PageCount / defaultIndexPagesEstimate
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
	stats.DistinctKeys = stats.NumEntries / defaultDistinctKeysDivisor
	stats.AvgKeySize = defaultAvgKeySize

	// Set type-specific defaults
	switch indexType {
	case index.BTreeIndex:
		if stats.NumEntries > 0 {
			// Height = log_fanout(entries) + 1 (for root level)
			stats.BTreeHeight = int(math.Log(float64(stats.NumEntries))/math.Log(btreeFanout)) + 1
		} else {
			stats.BTreeHeight = 1
		}
	case index.HashIndex:
		stats.BTreeHeight = 1
	default:
		stats.BTreeHeight = 1
	}

	clusteringFactor, err := iso.calculateClusteringFactor(tx, tableID, columnName)
	if err != nil {
		stats.ClusteringFactor = defaultClusteringFactor
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
	columnName string,
) (float64, error) {
	tableFile, err := iso.fileGetter(tableID)
	if err != nil || tableFile == nil {
		return 0.5, nil
	}

	var entries []indexEntry

	err = iso.Reader().IterateTable(tableID, tx, func(t *Tuple) error {
		fieldIndex, err := t.TupleDesc.FindFieldIndex(columnName)
		if err != nil {
			return nil
		}

		keyValue, err := t.GetField(fieldIndex)
		if err != nil {
			return nil
		}
		rid := t.RecordID

		if rid != nil {
			entries = append(entries, indexEntry{
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
	sort.Slice(entries, func(i, j int) bool {
		return compareFields(entries[i].keyValue, entries[j].keyValue) < 0
	})

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

	// Ensure result is in valid range [0, 1] using built-in math functions
	clusteringFactor = math.Max(minClusteringFactor, math.Min(maxClusteringFactor, clusteringFactor))

	return clusteringFactor, nil
}

// compareFields compares two fields and returns:
// -1 if a < b, 0 if a == b, 1 if a > b
func compareFields(a, b types.Field) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	eq, err := a.Compare(primitives.Equals, b)
	if err == nil && eq {
		return 0
	}

	lt, err := a.Compare(primitives.LessThan, b)
	if err == nil && lt {
		return -1
	}

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
			log.Println("couldn't collect stats")
			continue
		}

		if err := iso.StoreIndexStatistics(tx, stats); err != nil {
			return fmt.Errorf("failed to store index statistics: %w", err)
		}
	}
	return nil
}

// StoreIndexStatistics stores index statistics in CATALOG_INDEX_STATISTICS
// Handles both insert (new statistics) and update (existing statistics) cases
func (iso *IndexStatsOperations) StoreIndexStatistics(tx TxContext, stats *IndexStatistics) error {
	err := iso.Upsert(tx, func(s *IndexStatistics) bool {
		return s.IndexID == stats.IndexID
	}, stats)

	if err != nil {
		return fmt.Errorf("failed to store index statistics: %w", err)
	}
	return nil
}

// GetIndexStatistics retrieves statistics for a specific index from CATALOG_INDEX_STATISTICS
func (iso *IndexStatsOperations) GetIndexStatistics(tx TxContext, indexID int) (*IndexStatistics, error) {
	result, err := iso.FindOne(tx, func(stats *IndexStatistics) bool {
		return stats.IndexID == indexID
	})

	if err != nil {
		return nil, fmt.Errorf("index statistics not found for index %d", indexID)
	}

	return result, nil
}

// getIndexesForTable retrieves all indexes for a table using BaseOperations pattern
func (iso *IndexStatsOperations) getIndexesForTable(tx TxContext, tableID int) ([]*indexMetadata, error) {
	return iso.indexOps.FindAll(tx, func(im *indexMetadata) bool {
		return im.TableID == tableID
	})
}
