package catalog

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// AutoIncrementInfo represents auto-increment metadata for a column.
// Contains the column name, its position in the tuple, and the next value to use.
type AutoIncrementInfo struct {
	ColumnName  string
	ColumnIndex int
	NextValue   int
}

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
func (sc *SystemCatalog) GetAutoIncrementColumn(tx *transaction.TransactionContext, tableID int) (*AutoIncrementInfo, error) {
	var result *AutoIncrementInfo

	err := sc.iterateTable(sc.ColumnsTableID, tx, func(columnTuple *tuple.Tuple) error {
		colTableID, err := systemtable.Columns.GetTableID(columnTuple)
		if err != nil {
			return err
		}

		if colTableID != tableID {
			return nil
		}

		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return err
		}

		if !col.IsAutoInc {
			return nil
		}

		if result == nil || col.NextAutoValue > result.NextValue {
			result = &AutoIncrementInfo{
				ColumnName:  getStringField(columnTuple, 1),
				ColumnIndex: getIntField(columnTuple, 3),
				NextValue:   col.NextAutoValue,
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// IncrementAutoIncrementValue updates the next auto-increment value for a column.
// This implements MVCC by deleting the old tuple and inserting a new one with the updated value.
//
// This is called after successfully inserting a row with an auto-increment column to
// ensure the next INSERT gets a higher value.
//
// Parameters:
//   - tx: Transaction context for catalog updates
//   - tableID: ID of the table containing the auto-increment column
//   - columnName: Name of the auto-increment column to update
//   - newValue: New next_auto_value to set (typically current value + 1)
//
// Returns an error if the column cannot be found or the catalog update fails.
func (sc *SystemCatalog) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID int, columnName string, newValue int) error {
	file, err := sc.cache.GetDbFile(sc.ColumnsTableID)
	if err != nil {
		return fmt.Errorf("failed to get columns table: %w", err)
	}

	heapFFile := file.(*heap.HeapFile)
	iter, err := query.NewSeqScan(tx, sc.ColumnsTableID, heapFFile, sc.store)
	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iter.Close()

	// Use system table method to find latest version (MVCC-aware)
	match, err := systemtable.Columns.FindLatestAutoIncrementColumn(iter, tableID, columnName)
	if err != nil {
		return fmt.Errorf("failed to find auto-increment column: %w", err)
	}

	if match == nil {
		return fmt.Errorf("auto-increment column %s not found for table %d", columnName, tableID)
	}

	col := schema.ColumnMetadata{
		TableID:   tableID,
		Name:      columnName,
		FieldType: match.ColumnType,
		Position:  match.Position,
		IsPrimary: match.IsPrimary,
		IsAutoInc: match.IsAutoInc,
	}
	newTuple := systemtable.Columns.CreateTuple(col)
	newTuple.SetField(6, types.NewIntField(int64(newValue)))

	if err := sc.tupMgr.DeleteTuple(tx, file, match.Tuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := sc.tupMgr.InsertTuple(tx, file, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}

// LoadTableSchema reconstructs the schema for a table from CATALOG_COLUMNS.
// It scans all column metadata for the given tableID and builds a Schema object.
func (sc *SystemCatalog) LoadTableSchema(tx *transaction.TransactionContext, tableID int, tableName string) (*schema.Schema, error) {
	columns, err := sc.loadColumnMetadata(tx, tableID)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %d", tableID)
	}

	schemaObj, err := schema.NewSchema(tableID, tableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return schemaObj, nil
}

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
// It filters rows by table_id and collects ColumnInfo for each matching column.
func (sc *SystemCatalog) loadColumnMetadata(tx *transaction.TransactionContext, tableID int) ([]schema.ColumnMetadata, error) {
	var columns []schema.ColumnMetadata

	err := sc.iterateTable(sc.ColumnsTableID, tx, func(columnTuple *tuple.Tuple) error {
		id, err := systemtable.Columns.GetTableID(columnTuple)
		if err != nil {
			return err
		}

		if id != tableID {
			return nil
		}

		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return fmt.Errorf("failed to parse column tuple: %v", err)
		}

		columns = append(columns, *col)
		return nil
	})

	return columns, err
}
