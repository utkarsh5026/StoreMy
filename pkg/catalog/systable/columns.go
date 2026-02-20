package systable

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
)

// ColumnsTable is a system catalog table that stores metadata about all columns
// in the database. Each row represents one column definition with its properties
// including type, position, and auto-increment state.
type ColumnsTable struct {
	*BaseOperations[schema.ColumnMetadata]
}

// NewColumnOperations creates a new ColumnsTable instance.
func NewColumnOperations(access catalogio.CatalogAccess, colTableID primitives.FileID) *ColumnsTable {
	return &ColumnsTable{
		BaseOperations: NewBaseOperations(access, colTableID, ColumnsTableDescriptor),
	}
}

// AutoIncrementInfo represents auto-increment metadata for a column.
// Contains the column name, its position in the tuple, and the next value to use.
type AutoIncrementInfo struct {
	ColumnName  string              // Name of the auto-increment column (e.g., "id")
	ColumnIndex primitives.ColumnID // Position of the column in the tuple (0-indexed)
	NextValue   uint64              // Next value to use for this column
}

// GetAutoIncrementColumn retrieves auto-increment column info for a table.
// Returns nil if the table has no auto-increment column.
func (c *ColumnsTable) GetAutoIncrementColumn(tx *transaction.TransactionContext, tableID primitives.FileID) (*AutoIncrementInfo, error) {
	var result *AutoIncrementInfo

	err := c.Iterate(tx, func(col schema.ColumnMetadata) error {
		if col.TableID != tableID {
			return nil
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

// findLatestAutoIncrementColumn finds the latest version (highest next_auto_value) of a specific auto-increment column.
// This is used to ensure that concurrent transactions updating the auto-increment value do not overwrite each other's progress.
func (co *ColumnsTable) findLatestAutoIncrementColumn(tx *transaction.TransactionContext, tableID primitives.FileID, colName string) (*schema.ColumnMetadata, error) {
	var result *schema.ColumnMetadata
	var maxValue uint64 = 0

	err := co.Iterate(tx, func(c schema.ColumnMetadata) error {
		if c.TableID != tableID || c.Name != colName || c.NextAutoValue < maxValue {
			return nil
		}

		result = &c
		maxValue = c.NextAutoValue
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// IncrementAutoIncrementValue updates the next auto-increment value for a column.
// This method uses optimistic concurrency control by first finding the latest auto-increment column metadata and then attempting to update it with a new value.
func (co *ColumnsTable) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID primitives.FileID, colName string, newValue uint64) error {
	match, err := co.findLatestAutoIncrementColumn(tx, tableID, colName)
	if err != nil || match == nil {
		return fmt.Errorf("failed to find auto-increment column: %w", err)
	}

	updated := *match
	updated.NextAutoValue = newValue

	err = co.Upsert(tx, func(c schema.ColumnMetadata) bool {
		return c.TableID == tableID && c.Name == colName && c.NextAutoValue == match.NextAutoValue
	}, updated)

	if err != nil {
		return fmt.Errorf("failed to update auto-increment value: %w", err)
	}
	return nil
}

// LoadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
func (c *ColumnsTable) LoadColumnMetadata(tx *transaction.TransactionContext, tableID primitives.FileID) ([]schema.ColumnMetadata, error) {
	columns, err := c.FindAll(tx, func(c schema.ColumnMetadata) bool {
		return c.TableID == tableID
	})

	if err != nil {
		return nil, err
	}

	return columns, nil
}

// InsertColumns inserts multiple column metadata records into CATALOG_COLUMNS.
func (c *ColumnsTable) InsertColumns(tx *transaction.TransactionContext, cols []schema.ColumnMetadata) error {
	for _, col := range cols {
		if err := c.Insert(tx, col); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}
	return nil
}
