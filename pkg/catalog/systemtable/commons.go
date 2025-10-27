package systemtable

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
)

const (
	InvalidTableID primitives.TableID = 0 // Represents an invalid or uninitialized table ID
)

// Global instances of system tables.
// These are initialized at startup and provide access to the system catalog.
var (
	Tables          = &TablesTable{}
	Columns         = &ColumnsTable{}
	Stats           = &StatsTable{}
	Indexes         = &IndexesTable{}
	ColumnStats     = &ColumnStatsTable{}
	IndexStats      = &IndexStatsTable{}
	AllSystemTables = []SystemTable{Tables, Columns, Stats, Indexes, ColumnStats, IndexStats}
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
