package systemtable

import (
	"storemy/pkg/catalog/schema"
)

const (
	SystemTableTablesID     = 1
	SystemTableColumnsID    = 2
	SystemTableStatisticsID = 3
)

var (
	Tables  = &TablesTable{}
	Columns = &ColumnsTable{}
	Stats   = &StatsTable{}
)

type SystemTable interface {
	Schema() *schema.Schema
	TableName() string
	FileName() string
	PrimaryKey() string
}
