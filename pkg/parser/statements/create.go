package statements

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// FieldDefinition represents a column definition in a CREATE TABLE statement
type FieldDefinition struct {
	Name         string
	Type         types.Type
	NotNull      bool
	DefaultValue types.Field
}

// CreateStatement represents a SQL CREATE TABLE statement
type CreateStatement struct {
	BaseStatement
	TableName   string
	Fields      []FieldDefinition
	PrimaryKey  string
	IfNotExists bool
}

// NewCreateStatement creates a new CREATE TABLE statement
func NewCreateStatement(tableName string, ifNotExists bool) *CreateStatement {
	return &CreateStatement{
		BaseStatement: NewBaseStatement(CreateTable),
		TableName:     tableName,
		IfNotExists:   ifNotExists,
		Fields:        make([]FieldDefinition, 0),
	}
}

// FieldCount returns the number of fields
func (cts *CreateStatement) FieldCount() int {
	return len(cts.Fields)
}

// AddField adds a field definition to the CREATE TABLE statement
func (cts *CreateStatement) AddField(name string, fieldType types.Type, notNull bool, defaultValue types.Field) {
	cts.Fields = append(cts.Fields, FieldDefinition{
		Name:         name,
		Type:         fieldType,
		NotNull:      notNull,
		DefaultValue: defaultValue,
	})
}

func (cts *CreateStatement) Validate() error {
	if cts.TableName == "" {
		return NewValidationError(CreateTable, "TableName", "table name cannot be empty")
	}

	if len(cts.Fields) == 0 {
		return NewValidationError(CreateTable, "Fields", "at least one field is required")
	}

	fieldNames := make(map[string]bool)
	for i, field := range cts.Fields {
		if field.Name == "" {
			return NewValidationError(CreateTable, fmt.Sprintf("Fields[%d].Name", i), "field name cannot be empty")
		}

		if field.Type.String() == "" {
			return NewValidationError(CreateTable, fmt.Sprintf("Fields[%d].Type", i), "field type cannot be empty")
		}

		if fieldNames[field.Name] {
			return NewValidationError(CreateTable, fmt.Sprintf("Fields[%d].Name", i), fmt.Sprintf("duplicate field name: %s", field.Name))
		}

		fieldNames[field.Name] = true
	}

	if cts.PrimaryKey != "" {
		if !fieldNames[cts.PrimaryKey] {
			return NewValidationError(CreateTable, "PrimaryKey", fmt.Sprintf("primary key field '%s' does not exist", cts.PrimaryKey))
		}
	}

	return nil
}

// String returns a string representation of the CREATE TABLE statement
func (cts *CreateStatement) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")

	if cts.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}

	sb.WriteString(fmt.Sprintf("%s (\n", cts.TableName))

	for i, field := range cts.Fields {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString(fmt.Sprintf("  %s %s", field.Name, field.Type.String()))

		if field.NotNull {
			sb.WriteString(" NOT NULL")
		}

		if field.DefaultValue != nil {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", field.DefaultValue.String()))
		}
	}

	if cts.PrimaryKey != "" {
		sb.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", cts.PrimaryKey))
	}

	sb.WriteString("\n)")

	return sb.String()
}
