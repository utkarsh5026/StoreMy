package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/scanner"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// RegisterTable adds a new user table to the system catalog.
// It inserts metadata into CATALOG_TABLES and column definitions into CATALOG_COLUMNS.
//
// Note: Primary key indexes should be created separately by the DDL layer to ensure
// both catalog metadata and physical index files are created together.
//
// This is an internal helper used by CreateTable.
func (cm *CatalogManager) registerTable(tx *transaction.TransactionContext, sch *schema.Schema, filepath primitives.Filepath) error {
	tm := systable.TableMetadata{
		TableName:     sch.TableName,
		TableID:       sch.TableID,
		FilePath:      filepath,
		PrimaryKeyCol: sch.PrimaryKey,
	}
	if err := cm.TablesTable.Insert(tx, tm); err != nil {
		return err
	}

	if err := cm.ColumnTable.InsertColumns(tx, sch.Columns); err != nil {
		return err
	}

	return nil
}

// DeleteCatalogEntry removes all catalog metadata for a table.
// This includes entries in CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, and CATALOG_INDEXES.
//
// This is typically called as part of a DROP TABLE operation.
// Note: This only removes catalog entries - the heap file must be deleted separately.
func (cm *CatalogManager) DeleteCatalogEntry(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	if err := cm.TablesTable.DeleteTable(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_TABLES: %w", err)
	}

	if err := cm.ColumnTable.DeleteBy(tx, func(c schema.ColumnMetadata) bool {
		return c.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_COLUMNS: %w", err)
	}

	if err := cm.StatsTable.DeleteBy(tx, func(s systable.TableStatistics) bool {
		return s.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_STATISTICS: %w", err)
	}

	if err := cm.IndexTable.DeleteBy(tx, func(i systable.IndexMetadata) bool {
		return i.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_INDEXES: %w", err)
	}

	return nil
}

// DeleteTableFromSysTable removes all entries for a specific table from a given system table.
// Due to MVCC, there may be multiple versions of tuples for the same table - this deletes all.
func (cm *CatalogManager) DeleteTableFromSysTable(tx *transaction.TransactionContext, tableID, sysTableID primitives.FileID) error {
	tableInfo, err := cm.tableCache.GetTableInfo(sysTableID)
	if err != nil {
		return err
	}

	syst, err := cm.getSystemTableByID(sysTableID)
	if err != nil {
		return nil
	}

	var tuplesToDelete []*tuple.Tuple

	// Iterate through the system table to find all tuples matching the tableID
	err = cm.iterateTable(sysTableID, tx, func(t *tuple.Tuple) error {
		field, err := t.GetField(primitives.ColumnID(syst.TableIDIndex())) // #nosec G115
		if err != nil {
			return err
		}

		// All system tables use Uint64Field for table_id
		if uint64Field, ok := field.(*types.Uint64Field); ok {
			if uint64Field.Value == uint64(tableID) {
				tuplesToDelete = append(tuplesToDelete, t)
			}
		}

		return nil
	})

	// Check if iteration failed
	if err != nil {
		return fmt.Errorf("failed to iterate system table %d: %w", sysTableID, err)
	}

	// Delete all matching tuples
	for _, tup := range tuplesToDelete {
		if err := cm.tupMgr.DeleteTuple(tx, tableInfo.File, tup); err != nil {
			return err
		}
	}
	return nil
}

// LoadTableSchema reconstructs the complete schema for a table from CATALOG_COLUMNS.
// This includes column definitions, types, and constraints.
func (cm *CatalogManager) LoadTableSchema(tx *transaction.TransactionContext, tableID primitives.FileID) (*schema.Schema, error) {
	tm, err := cm.TablesTable.GetByID(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	columns, err := cm.ColumnTable.LoadColumnMetadata(tx, tableID)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %d", tableID)
	}

	sch, err := schema.NewSchema(tableID, tm.TableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return sch, nil
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is the core primitive for catalog queries, handling MVCC visibility and locking.
func (cm *CatalogManager) iterateTable(tableID primitives.FileID, tx *transaction.TransactionContext, processFunc func(*tuple.Tuple) error) error {
	file, err := cm.tableCache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	heapFile := file.(*heap.HeapFile)

	iter, err := scanner.NewSeqScan(tx, tableID, heapFile, cm.store)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}

	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}

// toColumnStatistics converts operations.ColStatsInfo to catalogmanager.ColumnStatistics.
// This is an internal helper for statistics collection methods.
func toColumnStatistics(info *systable.ColStatsInfo) *ColumnStatistics {
	if info == nil {
		return nil
	}

	return &ColumnStatistics{
		TableID:        info.TableID,
		ColumnName:     info.ColumnName,
		ColumnIndex:    info.ColumnIndex,
		DistinctCount:  info.DistinctCount,
		NullCount:      info.NullCount,
		MinValue:       stringToField(info.MinValue),
		MaxValue:       stringToField(info.MaxValue),
		AvgWidth:       info.AvgWidth,
		Histogram:      info.Histogram,
		MostCommonVals: info.MostCommonVals,
		MCVFreqs:       info.MCVFreqs,
		LastUpdated:    info.LastUpdated,
	}
}

// stringToField converts a string to a Field.
// Used when deserializing statistics from the catalog.
func stringToField(s string) types.Field {
	if s == "" {
		return nil
	}
	return types.NewStringField(s, len(s))
}
