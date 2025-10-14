package schema

import (
	"storemy/pkg/types"
)

// ColumnDef defines a column for schema building
type ColumnDef struct {
	Name            string
	Type            types.Type
	IsPrimaryKey    bool
	IsAutoIncrement bool
}

// SchemaBuilder helps construct system table schemas with less boilerplate
type SchemaBuilder struct {
	tableID   int
	tableName string
	columns   []ColumnDef
}

// NewSchemaBuilder creates a new schema builder
func NewSchemaBuilder(tableID int, tableName string) *SchemaBuilder {
	return &SchemaBuilder{
		tableID:   tableID,
		tableName: tableName,
		columns:   make([]ColumnDef, 0),
	}
}

// AddColumn adds a regular column
func (sb *SchemaBuilder) AddColumn(name string, fieldType types.Type) *SchemaBuilder {
	sb.columns = append(sb.columns, ColumnDef{
		Name:            name,
		Type:            fieldType,
		IsPrimaryKey:    false,
		IsAutoIncrement: false,
	})
	return sb
}

// AddPrimaryKey adds a primary key column
func (sb *SchemaBuilder) AddPrimaryKey(name string, fieldType types.Type) *SchemaBuilder {
	sb.columns = append(sb.columns, ColumnDef{
		Name:            name,
		Type:            fieldType,
		IsPrimaryKey:    true,
		IsAutoIncrement: false,
	})
	return sb
}

// AddAutoIncrement adds an auto-increment column (implies primary key)
func (sb *SchemaBuilder) AddAutoIncrement(name string) *SchemaBuilder {
	sb.columns = append(sb.columns, ColumnDef{
		Name:            name,
		Type:            types.IntType,
		IsPrimaryKey:    true,
		IsAutoIncrement: true,
	})
	return sb
}

// Build constructs the schema
func (sb *SchemaBuilder) Build() *Schema {
	columns := make([]ColumnMetadata, 0, len(sb.columns))

	for i, colDef := range sb.columns {
		col, _ := NewColumnMetadata(
			colDef.Name,
			colDef.Type,
			i, // position is automatically set
			sb.tableID,
			colDef.IsPrimaryKey,
			colDef.IsAutoIncrement,
		)
		columns = append(columns, *col)
	}

	sch, _ := NewSchema(sb.tableID, sb.tableName, columns)
	return sch
}

// BuildColumns is a convenience function for simple schema creation
func BuildColumns(tableID int, tableName string, defs ...ColumnDef) *Schema {
	builder := NewSchemaBuilder(tableID, tableName)
	for _, def := range defs {
		if def.IsAutoIncrement {
			builder.AddAutoIncrement(def.Name)
		} else if def.IsPrimaryKey {
			builder.AddPrimaryKey(def.Name, def.Type)
		} else {
			builder.AddColumn(def.Name, def.Type)
		}
	}
	return builder.Build()
}
