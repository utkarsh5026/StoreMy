package tables

import (
	"fmt"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// TableInfo holds metadata about a table
type TableInfo struct {
	File       page.DbFile             // The file storing the table data
	Name       string                  // The table name
	PrimaryKey string                  // Primary key field name
	TupleDesc  *tuple.TupleDescription // Schema of the table
}

// NewTableInfo creates a new table info instance
func NewTableInfo(file page.DbFile, name, primaryKey string) *TableInfo {
	return &TableInfo{
		File:       file,
		Name:       name,
		PrimaryKey: primaryKey,
		TupleDesc:  file.GetTupleDesc(),
	}
}

// GetID returns the table's unique identifier
func (ti *TableInfo) GetID() int {
	return ti.File.GetID()
}

// String returns a string representation of the table info
func (ti *TableInfo) String() string {
	return fmt.Sprintf("Table(%s, id=%d, schema=%s, pk=%s)",
		ti.Name, ti.GetID(), ti.TupleDesc.String(), ti.PrimaryKey)
}
