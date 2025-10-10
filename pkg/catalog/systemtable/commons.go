package systemtable

import "storemy/pkg/tuple"

const (
	// TablesTableID is the reserved heap file ID for the CATALOG_TABLES system table
	TablesTableID = 0
	// ColumnsTableID is the reserved heap file ID for the CATALOG_COLUMNS system table
	ColumnsTableID = 1
	// StatsTableID is the reserved heap file ID for the CATALOG_STATS system table
	StatsTableID = 2
)

var (
	Tables  = &TablesTable{}
	Columns = &ColumnsTable{}
	Stats   = &StatsTable{}
)

type SystemTable interface {
	Schema() *tuple.TupleDescription
	ID() int
	TableName() string
	FileName() string
	PrimaryKey() string
}
