package systable

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"strings"
)

// TableMetadata holds persisted metadata for a single table recorded in the system catalog.
// It is used by TableManager to rebuild in-memory catalog state during database startup.
type TableMetadata struct {
	TableID       primitives.FileID   // Unique numeric identifier for the table
	TableName     string              // Canonical table name used in SQL
	FilePath      primitives.Filepath // Heap file name where the table data is stored
	PrimaryKeyCol string              // Name of the primary key column (empty if none or composite)
}

// TablesTable provides accessors and helpers for the CATALOG_TABLES system table.
// Each row in CATALOG_TABLES maps a table id to its storage file and primary key.
type TablesTable struct {
	*BaseOperations[TableMetadata]
}

// NewTablesTable creates a new TablesTable instance.
func NewTablesTable(access catalogio.CatalogAccess, tableID primitives.FileID) *TablesTable {
	return &TablesTable{
		BaseOperations: NewBaseOperations(access, tableID, TablesTableDescriptor),
	}
}

// GetNumFields returns the number of fields in the CATALOG_TABLES schema.
func (tt *TablesTable) GetNumFields() int {
	return 4
}

// GetID extracts the table_id from a catalog tuple and validates tuple arity.
// Returns an error when the tuple does not match the expected schema length.
func (tt *TablesTable) GetID(t *tuple.Tuple) (int, error) {
	if int(t.NumFields()) != tt.GetNumFields() {
		return -1, fmt.Errorf("invalid tuple: expected 4 fields, got %d", t.TupleDesc.NumFields())
	}
	return getIntField(t, 0), nil
}

// findTableMetadata is a generic helper for searching CATALOG_TABLES with a custom predicate.
func (to *TablesTable) findTableMetadata(tx *transaction.TransactionContext, pred func(tm TableMetadata) bool) (TableMetadata, error) {
	res, err := to.FindOne(tx, pred)

	if err != nil {
		return TableMetadata{}, fmt.Errorf("table not found in catalog: %w", err)
	}

	return res, nil
}

// GetAllTables retrieves metadata for all tables registered in the catalog.
// This includes both user tables and system catalog tables.
func (to *TablesTable) GetAll(tx *transaction.TransactionContext) ([]TableMetadata, error) {
	return to.FindAll(tx, func(tm TableMetadata) bool {
		return true
	})
}

// GetTableMetadataByID retrieves complete table metadata from CATALOG_TABLES by table ID.
func (to *TablesTable) GetByID(tx *transaction.TransactionContext, tableID primitives.FileID) (TableMetadata, error) {
	return to.findTableMetadata(tx, func(tm TableMetadata) bool {
		return tm.TableID == tableID
	})
}

// GetTableMetadataByName retrieves complete table metadata from CATALOG_TABLES by table name (case-insensitive).
func (to *TablesTable) GetByName(tx *transaction.TransactionContext, tableName string) (TableMetadata, error) {
	return to.findTableMetadata(tx, func(tm TableMetadata) bool {
		return strings.EqualFold(tm.TableName, tableName)
	})
}

// DeleteTable removes a table entry from the catalog using its table ID.
func (to *TablesTable) DeleteTable(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	return to.DeleteBy(tx, func(tm TableMetadata) bool {
		return tm.TableID == tableID
	})
}
