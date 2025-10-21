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
//
// This struct is returned by GetAutoIncrementColumn and is used by INSERT operations
// to determine which column needs automatic value generation and what value to use.
//
// Example:
//
//	info := &AutoIncrementInfo{
//	    ColumnName:  "id",
//	    ColumnIndex: 0,
//	    NextValue:   42,
//	}
//	// Use info.NextValue for the next INSERT, then increment via IncrementAutoIncrementValue
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
//
// The operations are transaction-aware and work with the storage engine through
// the CatalogAccess interface, ensuring proper locking and concurrency control.
//
// Thread-safety: All operations require a valid TransactionContext and rely on
// the underlying storage engine's page-level locking for concurrency control.
type ColumnOperations struct {
	reader         catalogio.CatalogReader // Interface for reading catalog data
	writer         catalogio.CatalogWriter // Interface for writing catalog data
	columnsTableID int                     // Table ID of CATALOG_COLUMNS system table
}

// NewColumnOperations creates a new ColumnOperations instance.
//
// Parameters:
//   - access: CatalogAccess interface providing both read and write capabilities
//   - colTableID: The table ID of the CATALOG_COLUMNS system table (typically 2)
//
// Returns a configured ColumnOperations ready to perform catalog operations.
//
// Example:
//
//	colOps := NewColumnOperations(catalogAccess, systemtable.ColumnsTableID)
//	schema, err := colOps.LoadTableSchema(tx, tableID, "users")
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
// MVCC Behavior: Due to MVCC, there may be multiple versions of the same column tuple
// visible to the transaction (from concurrent auto-increment updates). This function
// returns the version with the highest next_auto_value to ensure monotonically
// increasing auto-increment values across concurrent transactions.
//
// This is critical for INSERT operations that need to determine:
//  1. Which column (if any) needs automatic value generation
//  2. What value to assign to that column
//  3. The column's position for tuple construction
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
// MVCC Awareness: This is critical for handling concurrent transactions that may be
// simultaneously incrementing the same auto-increment counter. Each increment creates
// a new tuple version in CATALOG_COLUMNS, and this function ensures we always get
// the most recent state by selecting the version with the highest next_auto_value.
//
// This is used internally by IncrementAutoIncrementValue to locate the exact tuple
// that needs to be deleted before inserting the updated version.
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
//
// Implementation note: This performs a full scan of CATALOG_COLUMNS filtered by
// tableID and columnName. The max value tracking ensures MVCC correctness.
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

		// Track the version with the highest next_auto_value
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

// iterateColumnsTable is a helper that iterates over all tuples in CATALOG_COLUMNS
// and applies an operation to each parsed ColumnMetadata.
//
// This abstracts the common pattern of:
//  1. Iterating raw tuples from the catalog
//  2. Parsing each tuple into ColumnMetadata
//  3. Applying a custom operation to the metadata
//
// Parameters:
//   - tx: Transaction context for reading the catalog
//   - operation: Function to apply to each ColumnMetadata. Return error to stop iteration.
//
// Returns an error if iteration fails or if the operation returns an error.
//
// This is used by GetAutoIncrementColumn, findLatestAutoIncrementColumn, and loadColumnMetadata
// to avoid code duplication in the common iteration + parse pattern.
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
	newTuple.SetField(6, types.NewIntField(int64(newValue))) // Field 6 is next_auto_value

	if err := co.writer.DeleteRow(co.columnsTableID, tx, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := co.writer.InsertRow(co.columnsTableID, tx, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}

// LoadTableSchema reconstructs the complete schema for a table from CATALOG_COLUMNS.
//
// This is a critical operation performed during table open/access. It scans all
// column metadata for the given tableID and builds a Schema object that contains:
//   - Column names, types, and positions
//   - Constraints (nullable, auto-increment)
//   - Auto-increment state
//
// The returned Schema is used throughout query execution for:
//   - Validating INSERT/UPDATE operations
//   - Type checking in WHERE clauses
//   - Projection operations (SELECT column lists)
//   - Join planning and execution
//
// MVCC Note: May see multiple versions of column tuples if concurrent schema
// modifications are happening. This typically doesn't affect correctness as
// schema changes (ALTER TABLE) should be serialized at a higher level.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table whose schema to load
//   - tableName: Name of the table (used for error messages and schema metadata)
//
// Returns:
//   - Fully constructed Schema object with all column metadata
//   - error if the table has no columns or catalog read fails
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
//
// This is a helper function for LoadTableSchema that handles the low-level details
// of scanning CATALOG_COLUMNS and collecting matching column metadata.
//
// It performs a filtered scan:
//  1. Iterate all tuples in CATALOG_COLUMNS
//  2. Parse each tuple into ColumnMetadata
//  3. Keep only columns where TableID matches the target table
//  4. Collect all matching columns into a slice
//
// The returned slice is unordered from the storage layer perspective, but typically
// columns are stored in position order. The Schema constructor is responsible for
// any final ordering requirements.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table to load columns for
//
// Returns:
//   - Slice of ColumnMetadata for all columns in the table
//   - error if catalog read or parsing fails
//
// This function does not validate that the columns make sense together (e.g.,
// multiple auto-increment columns, duplicate positions). That validation happens
// in schema.NewSchema().
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
