package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/execution/query"
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
func (cm *CatalogManager) registerTable(tx TxContext, sch *schema.Schema, filepath primitives.Filepath) error {
	tm := &systemtable.TableMetadata{
		TableName:     sch.TableName,
		TableID:       sch.TableID,
		FilePath:      filepath,
		PrimaryKeyCol: sch.PrimaryKey,
	}
	if err := cm.tableOps.Insert(tx, tm); err != nil {
		return err
	}

	if err := cm.colOps.InsertColumns(tx, sch.Columns); err != nil {
		return err
	}

	return nil
}

// DeleteCatalogEntry removes all catalog metadata for a table.
// This includes entries in CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, and CATALOG_INDEXES.
//
// This is typically called as part of a DROP TABLE operation.
// Note: This only removes catalog entries - the heap file must be deleted separately.
func (cm *CatalogManager) DeleteCatalogEntry(tx TxContext, tableID primitives.FileID) error {
	sysTableIDs := []primitives.FileID{
		cm.SystemTabs.TablesTableID,
		cm.SystemTabs.ColumnsTableID,
		cm.SystemTabs.StatisticsTableID,
		cm.SystemTabs.IndexesTableID,
	}

	for _, id := range sysTableIDs {
		if err := cm.DeleteTableFromSysTable(tx, tableID, id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTableFromSysTable removes all entries for a specific table from a given system table.
// Due to MVCC, there may be multiple versions of tuples for the same table - this deletes all.
func (cm *CatalogManager) DeleteTableFromSysTable(tx TxContext, tableID, sysTableID primitives.FileID) error {
	tableInfo, err := cm.tableCache.GetTableInfo(sysTableID)
	if err != nil {
		return err
	}

	syst, err := cm.SystemTabs.GetSysTable(sysTableID)
	if err != nil {
		return nil
	}

	var tuplesToDelete []*tuple.Tuple

	cm.iterateTable(sysTableID, tx, func(t *tuple.Tuple) error {
		field, err := t.GetField(primitives.ColumnID(syst.TableIDIndex()))
		if err != nil {
			return err
		}

		if intField, ok := field.(*types.IntField); ok {
			if intField.Value == int64(tableID) {
				tuplesToDelete = append(tuplesToDelete, t)
			}
		}

		return nil
	})

	for _, tup := range tuplesToDelete {
		if err := cm.tupMgr.DeleteTuple(tx, tableInfo.File, tup); err != nil {
			return err
		}
	}
	return nil
}

// GetTableMetadataByID retrieves complete table metadata from CATALOG_TABLES by table ID.
// Returns TableMetadata or an error if the table is not found.
func (cm *CatalogManager) GetTableMetadataByID(tx TxContext, tableID primitives.FileID) (*systemtable.TableMetadata, error) {
	return cm.tableOps.GetTableMetadataByID(tx, tableID)
}

// GetTableMetadataByName retrieves complete table metadata from CATALOG_TABLES by table name.
// Table name matching is case-insensitive.
func (cm *CatalogManager) GetTableMetadataByName(tx TxContext, tableName string) (*systemtable.TableMetadata, error) {
	return cm.tableOps.GetTableMetadataByName(tx, tableName)
}

// GetAllTables retrieves metadata for all tables registered in the catalog.
// This includes both user tables and system catalog tables.
func (cm *CatalogManager) GetAllTables(tx TxContext) ([]*systemtable.TableMetadata, error) {
	return cm.tableOps.GetAllTables(tx)
}

// LoadTableSchema reconstructs the complete schema for a table from CATALOG_COLUMNS.
// This includes column definitions, types, and constraints.
func (cm *CatalogManager) LoadTableSchema(tx TxContext, tableID primitives.FileID) (*schema.Schema, error) {
	tm, err := cm.GetTableMetadataByID(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	columns, err := cm.colOps.LoadColumnMetadata(tx, tableID)
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
func (cm *CatalogManager) iterateTable(tableID primitives.FileID, tx TxContext, processFunc func(*tuple.Tuple) error) error {
	file, err := cm.tableCache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	heapFile := file.(*heap.HeapFile)

	iter, err := query.NewSeqScan(tx, tableID, heapFile, cm.store)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}

	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}

// IterateTable implements CatalogReader interface by delegating to CatalogIO.
// Scans all tuples in a table and applies a processing function to each.
func (cm *CatalogManager) IterateTable(tableID primitives.FileID, tx TxContext, processFunc func(Tuple) error) error {
	return cm.io.IterateTable(tableID, tx, processFunc)
}

// InsertRow implements CatalogWriter interface by delegating to CatalogIO.
// Inserts a tuple into a table within a transaction.
func (cm *CatalogManager) InsertRow(tableID primitives.FileID, tx TxContext, tup Tuple) error {
	return cm.io.InsertRow(tableID, tx, tup)
}

// DeleteRow implements CatalogWriter interface by delegating to CatalogIO.
// Deletes a tuple from a table within a transaction.
func (cm *CatalogManager) DeleteRow(tableID primitives.FileID, tx TxContext, tup Tuple) error {
	return cm.io.DeleteRow(tableID, tx, tup)
}

// toColumnStatistics converts operations.ColStatsInfo to catalogmanager.ColumnStatistics.
// This is an internal helper for statistics collection methods.
func toColumnStatistics(info *operations.ColStatsInfo) *ColumnStatistics {
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

// hashFilePath generates a unique ID from a file path using a simple hash function.
// Used for generating index IDs from file paths.
func hashFilePath(filePath string) int {
	hash := 0
	for i := 0; i < len(filePath); i++ {
		hash = hash*31 + int(filePath[i])
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}
