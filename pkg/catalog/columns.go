package catalog

import (
	"fmt"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// GetAutoIncrementColumn retrieves auto-increment column info for a table.
// Returns nil if the table has no auto-increment column.
//
// Note: Due to MVCC, there may be multiple versions of the same column tuple.
// This function returns the version with the highest next_auto_value (most recent).
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - tableID: ID of the table to check for auto-increment columns
//
// Returns AutoIncrementInfo if an auto-increment column exists, nil if none exists,
// or an error if the catalog cannot be read.
func (sc *SystemCatalog) GetAutoIncrementColumn(tx *transaction.TransactionContext, tableID int) (*operations.AutoIncrementInfo, error) {
	return sc.colOps.GetAutoIncrementColumn(tx, tableID)
}

// IncrementAutoIncrementValue updates the next auto-increment value for a column.
// This implements MVCC by deleting the old tuple and inserting a new one with the updated value.
func (sc *SystemCatalog) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID int, columnName string, newValue int) error {
	return sc.colOps.IncrementAutoIncrementValue(tx, tableID, columnName, newValue)
}

// LoadTableSchema reconstructs the schema for a table from CATALOG_COLUMNS.
// It scans all column metadata for the given tableID and builds a Schema object.
func (sc *SystemCatalog) LoadTableSchema(tx *transaction.TransactionContext, tableID int, tableName string) (*schema.Schema, error) {
	return sc.colOps.LoadTableSchema(tx, tableID, tableName)
}

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
// It filters rows by table_id and collects ColumnInfo for each matching column.
func (sc *SystemCatalog) loadColumnMetadata(tx *transaction.TransactionContext, tableID int) ([]schema.ColumnMetadata, error) {
	var columns []schema.ColumnMetadata

	err := sc.iterateTable(sc.SystemTabs.ColumnsTableID, tx, func(ct *tuple.Tuple) error {
		id, err := systemtable.Columns.GetTableID(ct)
		if err != nil {
			return err
		}

		if id != tableID {
			return nil
		}

		col, err := systemtable.Columns.Parse(ct)
		if err != nil {
			return fmt.Errorf("failed to parse column tuple: %v", err)
		}

		columns = append(columns, *col)
		return nil
	})

	return columns, err
}
