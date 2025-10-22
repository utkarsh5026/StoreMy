package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"time"
)

type tableStats = systemtable.TableStatistics
type FileGetter = func(tableID int) (page.DbFile, error)
type StatsCacheSetter interface {
	SetCachedStatistics(tableID int, stats *tableStats) error
}

type StatsOperations struct {
	*BaseOperations[*tableStats]
	reader     catalogio.CatalogReader
	fileGetter FileGetter
	cache      StatsCacheSetter
}

// NewStatsOperations creates a new StatsOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - statsTableID: ID of the CATALOG_STATISTICS system table
//   - fileGetter: Function to retrieve DbFile by table ID
//   - cache: Optional cache for storing statistics (can be nil)
func NewStatsOperations(access catalogio.CatalogAccess, statsTableID int, fileGetter FileGetter, cache StatsCacheSetter) *StatsOperations {
	base := NewBaseOperations(
		access,
		statsTableID,
		systemtable.Stats.Parse,
		systemtable.Stats.CreateTuple,
	)

	return &StatsOperations{
		BaseOperations: base,
		reader:         access,
		fileGetter:     fileGetter,
		cache:          cache,
	}
}

// UpdateTableStatistics collects and updates statistics for a given table
// Also updates the cache with fresh statistics
func (so *StatsOperations) UpdateTableStatistics(tx TxContext, tableID int) error {
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

// collectStats scans a table and computes its statistics
// Collects: Cardinality, PageCount, AvgTupleSize, and DistinctValues (sampled from first field)
func (so *StatsOperations) collectStats(tx TxContext, tableID int) (*tableStats, error) {
	file, err := so.fileGetter(tableID)
	if err != nil {
		return nil, err
	}

	pageCount, err := getHeapPageCount(file)
	if err != nil {
		return nil, err
	}

	stats := &tableStats{
		TableID:     tableID,
		Cardinality: 0,
		PageCount:   pageCount,
		LastUpdated: time.Now(),
	}

	totalSize := 0
	distinctMap := make(map[int]bool)

	err = so.reader.IterateTable(tableID, tx, func(t *tuple.Tuple) error {
		stats.Cardinality++
		td := t.TupleDesc
		totalSize += int(td.GetSize())

		if td.NumFields() == 0 {
			return nil
		}

		id, err := systemtable.Stats.GetTableID(t)
		if err == nil {
			distinctMap[id] = true
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
func (so *StatsOperations) GetTableStatistics(tx TxContext, tableID int) (*tableStats, error) {
	result, err := so.FindOne(tx, func(t *tableStats) bool {
		return t.TableID == tableID
	})

	if err != nil {
		return nil, fmt.Errorf("statistics for table %d not found", tableID)
	}

	return result, nil
}

// insert creates a new statistics entry
func (so *StatsOperations) insert(tx TxContext, stats *tableStats) error {
	if err := so.Insert(tx, stats); err != nil {
		return fmt.Errorf("failed to insert statistics: %w", err)
	}
	return nil
}

// update updates an existing statistics entry
func (so *StatsOperations) update(tx TxContext, tableID int, stats *tableStats) error {
	err := so.Upsert(tx, func(t *tableStats) bool {
		return t.TableID == tableID
	}, stats)

	if err != nil {
		return fmt.Errorf("failed to update statistics: %w", err)
	}
	return nil
}

// getHeapPageCount retrieves the number of pages in a heap file
// Returns 0 for non-heap files, or the actual page count for heap files
func getHeapPageCount(file page.DbFile) (int, error) {
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
