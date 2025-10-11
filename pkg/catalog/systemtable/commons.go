package systemtable

import (
	"storemy/pkg/catalog/schema"
)

// System table IDs - reserved IDs for internal catalog tables.
// These are fixed IDs that cannot be used by user tables.
const (
	SystemTableTablesID     = 1 // Catalog table storing metadata about all tables
	SystemTableColumnsID    = 2 // Catalog table storing column definitions
	SystemTableStatisticsID = 3 // Catalog table storing table/index statistics
)

// Global instances of system tables.
// These are initialized at startup and provide access to the system catalog.
var (
	Tables          = &TablesTable{}                        // Manages table metadata (names, IDs, file paths)
	Columns         = &ColumnsTable{}                       // Manages column definitions for all tables
	Stats           = &StatsTable{}                         // Manages statistics for query optimization
	AllSystemTables = []SystemTable{Tables, Columns, Stats} // List of all system tables
)

// SystemTable defines the interface that all system catalog tables must implement.
// System tables store metadata about the database schema and are managed internally
// by the database engine, not directly accessible to users for modification.
//
// System tables use the same storage engine (heap pages) as user tables but have
// reserved table IDs and special initialization during database startup.
type SystemTable interface {
	// Schema returns the table's schema definition including all columns and types.
	// Used by the TableManager to register the system table in the catalog.
	Schema() *schema.Schema

	// TableName returns the canonical name of the system table (e.g., "storemy_tables").
	// System table names typically use a prefix to distinguish them from user tables.
	TableName() string

	// FileName returns the physical file name where the table data is stored.
	// System tables follow the same file naming convention as user tables.
	FileName() string

	// PrimaryKey returns the name of the primary key column.
	// Used for indexing and enforcing uniqueness constraints.
	PrimaryKey() string

	// TableIDIndex returns the index of the table ID column.
	TableIDIndex() int
}
