package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
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

type ColumnOperations struct {
	reader         catalogio.CatalogReader
	writer         catalogio.CatalogWriter
	columnsTableID int
}

func NewColumnOperations(access catalogio.CatalogAccess, colTableID int) *ColumnOperations {
	return &ColumnOperations{
		reader:         access,
		writer:         access,
		columnsTableID: colTableID,
	}
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
func (co *ColumnOperations) GetAutoIncrementColumn(tx TxContext, tableID int) (*AutoIncrementInfo, error) {
	var result *AutoIncrementInfo

	err := co.reader.IterateTable(co.columnsTableID, tx, func(columnTuple *tuple.Tuple) error {
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
				ColumnName:  col.Name,
				ColumnIndex: col.Position,
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

// FindLatestAutoIncrementColumn finds the latest version (highest next_auto_value) of an auto-increment column.
// This is MVCC-aware and handles multiple versions of the same column that may exist due to concurrent
// transactions updating the auto-increment counter.
//
// Parameters:
//   - iter: TupleIterator over CATALOG_COLUMNS (may contain multiple versions)
//   - tableID: The table to search within
//   - columnName: The column name to find
//
// Returns the ColumnMatch with the highest next_auto_value, or nil if no matching column found.
// This ensures we always use the most recent auto-increment state even with concurrent INSERTs.
func (co *ColumnOperations) findLatestAutoIncrementColumn(tx TxContext, tableID int, columnName string) (*schema.ColumnMetadata, error) {
	var result *schema.ColumnMetadata
	maxValue := -1

	err := co.iterateColumnsTable(tx, func(c *schema.ColumnMetadata) error {
		if c.TableID != tableID {
			return nil
		}

		if c.Name != columnName {
			return nil
		}

		if c.NextAutoValue < maxValue {
			return nil
		}

		result = c
		maxValue = c.NextAutoValue
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (co *ColumnOperations) iterateColumnsTable(tx TxContext, operation func(c *schema.ColumnMetadata) error) error {
	return co.reader.IterateTable(co.columnsTableID, tx, func(columnTuple *tuple.Tuple) error {

		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return fmt.Errorf("column parsing error")
		}

		if err = operation(col); err != nil {
			return fmt.Errorf("failed to parse column tuple: %v", err)
		}

		return nil
	})
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
func (co *ColumnOperations) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID int, columnName string, newValue int) error {
	match, err := co.findLatestAutoIncrementColumn(tx, tableID, columnName)
	if err != nil {
		return fmt.Errorf("failed to find auto-increment column: %w", err)
	}

	if match == nil {
		return fmt.Errorf("auto-increment column %s not found for table %d", columnName, tableID)
	}

	oldTuple := systemtable.Columns.CreateTuple(*match)
	newTuple := systemtable.Columns.CreateTuple(*match)
	newTuple.SetField(6, types.NewIntField(int64(newValue)))

	if err := co.writer.DeleteRow(co.columnsTableID, tx, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := co.writer.InsertRow(co.columnsTableID, tx, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}

// LoadTableSchema reconstructs the schema for a table from CATALOG_COLUMNS.
// It scans all column metadata for the given tableID and builds a Schema object.
func (co *ColumnOperations) LoadTableSchema(tx *transaction.TransactionContext, tableID int, tableName string) (*schema.Schema, error) {
	columns, err := co.loadColumnMetadata(tx, tableID)
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
func (co *ColumnOperations) loadColumnMetadata(tx *transaction.TransactionContext, tableID int) ([]schema.ColumnMetadata, error) {
	var columns []schema.ColumnMetadata

	err := co.iterateColumnsTable(tx, func(c *schema.ColumnMetadata) error {
		if c.TableID != tableID {
			return nil
		}
		columns = append(columns, *c)
		return nil
	})

	return columns, err
}
