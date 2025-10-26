package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
)

type colMetadata = schema.ColumnMetadata

// AutoIncrementInfo represents auto-increment metadata for a column.
// Contains the column name, its position in the tuple, and the next value to use.
type AutoIncrementInfo struct {
	ColumnName  string // Name of the auto-increment column (e.g., "id")
	ColumnIndex int    // Position of the column in the tuple (0-indexed)
	NextValue   int    // Next value to use for this column
}

// ColumnOperations provides operations for managing column metadata in the CATALOG_COLUMNS system table.
//
// This component is part of the catalog layer and handles:
//   - Retrieving column schemas for tables
//   - Managing auto-increment column metadata
//   - MVCC-aware updates to column state (especially auto-increment counters)
type ColumnOperations struct {
	*BaseOperations[*colMetadata]
}

// NewColumnOperations creates a new ColumnOperations instance.
//
// Parameters:
//   - access: CatalogAccess interface providing both read and write capabilities
//   - colTableID: The table ID of the CATALOG_COLUMNS system table (typically 2)
//
// Returns a configured ColumnOperations ready to perform catalog operations.
func NewColumnOperations(access catalogio.CatalogAccess, colTableID int) *ColumnOperations {
	base := NewBaseOperations(access, colTableID, systemtable.Columns.Parse,
		func(c *colMetadata) *Tuple {
			return systemtable.Columns.CreateTuple(*c)
		},
	)

	return &ColumnOperations{
		BaseOperations: base,
	}
}

// GetAutoIncrementColumn retrieves auto-increment column info for a table.
// Returns nil if the table has no auto-increment column.
//
// Parameters:
//   - tx: Transaction context for MVCC-aware catalog reads
//   - tableID: ID of the table to check for auto-increment columns
//
// Returns:
//   - AutoIncrementInfo if an auto-increment column exists
//   - nil if the table has no auto-increment column
//   - error if the catalog cannot be read or is corrupted
func (co *ColumnOperations) GetAutoIncrementColumn(tx TxContext, tableID int) (*AutoIncrementInfo, error) {
	var result *AutoIncrementInfo

	err := co.Iterate(tx, func(col *colMetadata) error {
		if col.TableID != tableID {
			return nil
		}

		if !col.IsAutoInc {
			return nil
		}

		// Keep the version with the highest next_auto_value (most recent)
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
//
// Parameters:
//   - tx: Transaction context for MVCC-aware catalog reads
//   - tableID: The table containing the column
//   - columnName: The name of the auto-increment column to find
//
// Returns:
//   - ColumnMetadata with the highest next_auto_value for the specified column
//   - nil if no matching column found
//   - error if catalog read fails
func (co *ColumnOperations) findLatestAutoIncrementColumn(tx TxContext, tableID int, columnName string) (*colMetadata, error) {
	var result *colMetadata
	maxValue := -1

	err := co.Iterate(tx, func(c *colMetadata) error {
		if c.TableID != tableID || c.Name != columnName || c.NextAutoValue < maxValue {
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

// IncrementAutoIncrementValue updates the next auto-increment value for a column.
//
// MVCC Implementation: This implements MVCC by following the delete-then-insert pattern:
//  1. Find the latest version of the column tuple (highest next_auto_value)
//  2. Delete that tuple (creates a deletion marker visible to this transaction)
//  3. Insert a new tuple with the incremented value
//
// This ensures that concurrent transactions see a consistent view of the auto-increment
// counter without requiring row-level locks on CATALOG_COLUMNS.
//
// Parameters:
//   - tx: Transaction context for catalog updates (must have write permissions)
//   - tableID: ID of the table containing the auto-increment column
//   - columnName: Name of the auto-increment column to update
//   - newValue: New next_auto_value to set (typically current value + 1)
//
// Returns an error if:
//   - The column cannot be found (may indicate catalog corruption)
//   - The delete operation fails (lock conflict or storage error)
//   - The insert operation fails (storage error or constraint violation)
func (co *ColumnOperations) IncrementAutoIncrementValue(tx TxContext, tableID int, columnName string, newValue int) error {
	match, err := co.findLatestAutoIncrementColumn(tx, tableID, columnName)
	if err != nil || match == nil {
		return fmt.Errorf("failed to find auto-increment column: %w", err)
	}

	updated := *match
	updated.NextAutoValue = newValue

	err = co.Upsert(tx, func(c *colMetadata) bool {
		return c.TableID == tableID && c.Name == columnName && c.NextAutoValue == match.NextAutoValue
	}, &updated)

	if err != nil {
		return fmt.Errorf("failed to update auto-increment value: %w", err)
	}
	return nil
}

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table to load columns for
//
// Returns:
//   - Slice of ColumnMetadata for all columns in the table
//   - error if catalog read or parsing fails
func (co *ColumnOperations) LoadColumnMetadata(tx TxContext, tableID int) ([]colMetadata, error) {
	columnPtrs, err := co.FindAll(tx, func(c *colMetadata) bool {
		return c.TableID == tableID
	})

	if err != nil {
		return nil, err
	}

	columns := make([]colMetadata, len(columnPtrs))
	for i, colPtr := range columnPtrs {
		columns[i] = *colPtr
	}

	return columns, nil
}

// InsertColumns inserts multiple column metadata records into CATALOG_COLUMNS.
//
// This is typically used during table creation to populate all column definitions
// in a single transaction. Each column is inserted sequentially; if any insertion
// fails, the function returns immediately with the error.
//
// Parameters:
//   - tx: Transaction context for catalog writes
//   - cols: Slice of ColumnMetadata to insert
//
// Returns an error if any column insertion fails, which may indicate:
//   - Storage errors (page allocation failures, I/O errors)
//   - Constraint violations (duplicate column entries)
//   - Transaction conflicts
func (co *ColumnOperations) InsertColumns(tx TxContext, cols []schema.ColumnMetadata) error {
	for _, col := range cols {
		if err := co.Insert(tx, &col); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}
	return nil
}
