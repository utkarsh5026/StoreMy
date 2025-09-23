package statements

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

type FieldDefinition struct {
	Name         string
	Type         types.Type
	NotNull      bool
	DefaultValue types.Field
}

type CreateStatement struct {
	TableName   string
	Fields      []FieldDefinition
	PrimaryKey  string
	IfNotExists bool
}

func NewCreateStatement(tableName string, ifNotExists bool) *CreateStatement {
	return &CreateStatement{
		TableName:   tableName,
		IfNotExists: ifNotExists,
		Fields:      make([]FieldDefinition, 0),
	}
}

func (cts *CreateStatement) SetPrimaryKey(fieldName string) {
	cts.PrimaryKey = fieldName
}

func (cts *CreateStatement) GetType() StatementType {
	return CreateTable
}

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

func (cts *CreateStatement) AddField(name string, fieldType types.Type, notNull bool, defaultValue types.Field) {
	cts.Fields = append(cts.Fields, FieldDefinition{
		Name:         name,
		Type:         fieldType,
		NotNull:      notNull,
		DefaultValue: defaultValue,
	})
}
