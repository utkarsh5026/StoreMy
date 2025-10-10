package systemtable

import "storemy/pkg/tuple"

var (
	Tables  = &TablesTable{}
	Columns = &ColumnsTable{}
	Stats   = &StatsTable{}
)

type SystemTable interface {
	Schema() *tuple.TupleDescription
	TableName() string
	FileName() string
	PrimaryKey() string
}
