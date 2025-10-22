package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/tuple"
	"strings"
)

type (
	tableMetadata = systemtable.TableMetadata
)

type TableOperations struct {
	*BaseOperations[*tableMetadata]
}

func NewTableOperations(access catalogio.CatalogAccess, tableID int) *TableOperations {
	baseOp := NewBaseOperations(access, tableID, systemtable.Tables.Parse, func(t *tableMetadata) *tuple.Tuple {
		return systemtable.Tables.CreateTuple(*t)
	})
	return &TableOperations{
		BaseOperations: baseOp,
	}
}

// findTableMetadata is a generic helper for searching CATALOG_TABLES with a custom predicate.
// Used by GetTableMetadataByID and GetTableMetadataByName to avoid code duplication.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - pred: Predicate function that returns true when the desired table is found
//
// Returns the matching TableMetadata or an error if not found or if catalog access fails.
func (to *TableOperations) findTableMetadata(tx TxContext, pred func(tm *tableMetadata) bool) (*tableMetadata, error) {
	res, err := to.FindOne(tx, pred)

	if err != nil {
		return nil, fmt.Errorf("table not found in catalog: %w", err)
	}

	return res, nil
}

// GetAllTables retrieves metadata for all tables registered in the catalog.
// This includes both user tables and system catalog tables.
//
// Used by commands like SHOW TABLES and for query planning operations that need
// to enumerate available tables.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//
// Returns a slice of TableMetadata for all tables, or an error if the catalog cannot be read.
func (to *TableOperations) GetAllTables(tx TxContext) ([]*tableMetadata, error) {
	return to.FindAll(tx, func(tm *tableMetadata) bool {
		return true
	})
}

// GetTableMetadataByID retrieves complete table metadata from CATALOG_TABLES by table ID.
// Returns TableMetadata containing table name, file path, and primary key column,
// or an error if the table is not found.
func (to *TableOperations) GetTableMetadataByID(tx TxContext, tableID int) (*tableMetadata, error) {
	return to.findTableMetadata(tx, func(tm *tableMetadata) bool {
		return tm.TableID == tableID
	})
}

// GetTableMetadataByName retrieves complete table metadata from CATALOG_TABLES by table name.
// Table name matching is case-insensitive.
// Returns TableMetadata containing table ID, file path, and primary key column,
// or an error if the table is not found.
func (to *TableOperations) GetTableMetadataByName(tx TxContext, tableName string) (*tableMetadata, error) {
	return to.findTableMetadata(tx, func(tm *tableMetadata) bool {
		return strings.EqualFold(tm.TableName, tableName)
	})
}
