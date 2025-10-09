package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

const (
	StatisticsTable           = "CATALOG_STATISTICS"
	CatalogStatisticsFileName = "catalog_statistics.dat"
)

// TableStatistics holds statistics about a table for query optimization
type TableStatistics struct {
	TableID        int       // Table identifier
	Cardinality    int       // Number of tuples in the table
	PageCount      int       // Number of pages used by the table
	AvgTupleSize   int       // Average tuple size in bytes
	LastUpdated    time.Time // When statistics were last updated
	DistinctValues int       // Approximate number of distinct values (for primary key)
	NullCount      int       // Number of null values
	MinValue       string    // Min value for primary key (if applicable)
	MaxValue       string    // Max value for primary key (if applicable)
}

// GetStatisticsSchema returns the schema for CATALOG_STATISTICS table
// Schema: (table_id INT, cardinality INT, page_count INT, avg_tuple_size INT,
//
//	last_updated INT, distinct_values INT, null_count INT,
//	min_value STRING, max_value STRING)
func GetStatisticsSchema() *tuple.TupleDescription {
	fieldTypes := []types.Type{
		types.IntType,    // table_id
		types.IntType,    // cardinality
		types.IntType,    // page_count
		types.IntType,    // avg_tuple_size
		types.IntType,    // last_updated (Unix timestamp as int32)
		types.IntType,    // distinct_values
		types.IntType,    // null_count
		types.StringType, // min_value
		types.StringType, // max_value
	}

	fieldNames := []string{
		"table_id",
		"cardinality",
		"page_count",
		"avg_tuple_size",
		"last_updated",
		"distinct_values",
		"null_count",
		"min_value",
		"max_value",
	}

	desc, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return desc
}

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
		return &TableStatistics{
			TableID:     tableID,
			Cardinality: 0,
			PageCount:   0,
			LastUpdated: time.Now(),
		}, nil
	}

	pageCount := 0
	if hf, ok := file.(interface{ NumPages() (int, error) }); ok {
		pc, err := hf.NumPages()
		if err != nil {
			return nil, fmt.Errorf("failed to get page count: %w", err)
		}
		pageCount = pc
	}

	stats := &TableStatistics{
		TableID:     tableID,
		Cardinality: 0,
		PageCount:   pageCount,
		LastUpdated: time.Now(),
	}

	totalSize := 0
	distinctMap := make(map[string]bool)

	iter := file.Iterator(tid)
	if err := iter.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iter.Close()

	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			break
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			break
		}
		if tup == nil {
			break
		}

		stats.Cardinality++
		totalSize += int(tup.TupleDesc.GetSize())

		if tup.TupleDesc.NumFields() > 0 {
			field, err := tup.GetField(0)
			if err == nil && field != nil {
				distinctMap[field.String()] = true
			}
		}
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

	// Convert tableID to int32 for comparison since it's stored as int32
	tableIDInt32 := int(int32(tableID))

	err := sc.iterateTable(sc.statisticsTableID, tid, func(statsTuple *tuple.Tuple) error {
		storedTableID := getIntField(statsTuple, 0)
		if storedTableID == tableIDInt32 {
			result = &TableStatistics{
				TableID:        storedTableID,
				Cardinality:    getIntField(statsTuple, 1),
				PageCount:      getIntField(statsTuple, 2),
				AvgTupleSize:   getIntField(statsTuple, 3),
				LastUpdated:    time.Unix(int64(getIntField(statsTuple, 4)), 0),
				DistinctValues: getIntField(statsTuple, 5),
				NullCount:      getIntField(statsTuple, 6),
				MinValue:       getStringField(statsTuple, 7),
				MaxValue:       getStringField(statsTuple, 8),
			}
			return fmt.Errorf("found") // Break iteration
		}
		return nil
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
	tup := createStatisticsTuple(stats)
	if err := sc.store.InsertTuple(tx, sc.statisticsTableID, tup); err != nil {
		return fmt.Errorf("failed to insert statistics: %w", err)
	}
	return nil
}

// updateExistingStatistics updates an existing statistics entry
func (sc *SystemCatalog) updateExistingStatistics(tx *transaction.TransactionContext, tableID int, stats *TableStatistics) error {
	// Delete old statistics
	if err := sc.deleteTableStatistics(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete old statistics: %w", err)
	}

	// Insert new statistics
	return sc.insertNewStatistics(tx, stats)
}

// deleteTableStatistics removes statistics for a table
func (sc *SystemCatalog) deleteTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	file, err := sc.tableManager.GetDbFile(sc.statisticsTableID)
	if err != nil {
		return fmt.Errorf("failed to get statistics table: %w", err)
	}

	tableIDInt32 := int(int32(tableID))

	iter := file.Iterator(tx.ID)
	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iter.Close()

	// Collect tuples to delete (can't delete while iterating)
	var tuplesToDelete []*tuple.Tuple
	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		storedTableID := getIntField(tup, 0)
		if storedTableID == tableIDInt32 {
			tuplesToDelete = append(tuplesToDelete, tup)
		}
	}

	for _, tup := range tuplesToDelete {
		if err := sc.store.DeleteTuple(tx, tup); err != nil {
			return fmt.Errorf("failed to delete statistics tuple: %w", err)
		}
	}

	return nil
}

// createStatisticsTuple creates a tuple for the statistics table
func createStatisticsTuple(stats *TableStatistics) *tuple.Tuple {
	t := tuple.NewTuple(GetStatisticsSchema())
	t.SetField(0, types.NewIntField(int32(stats.TableID)))
	t.SetField(1, types.NewIntField(int32(stats.Cardinality)))
	t.SetField(2, types.NewIntField(int32(stats.PageCount)))
	t.SetField(3, types.NewIntField(int32(stats.AvgTupleSize)))
	t.SetField(4, types.NewIntField(int32(stats.LastUpdated.Unix())))
	t.SetField(5, types.NewIntField(int32(stats.DistinctValues)))
	t.SetField(6, types.NewIntField(int32(stats.NullCount)))
	t.SetField(7, types.NewStringField(stats.MinValue, types.StringMaxSize))
	t.SetField(8, types.NewStringField(stats.MaxValue, types.StringMaxSize))
	return t
}
