package statements

import (
	"fmt"
	"storemy/pkg/parser/plan"
	"storemy/pkg/types"
	"strings"
)

// SetClause represents a field assignment in an UPDATE statement (e.g., field = value)
type SetClause struct {
	FieldName string
	Value     types.Field
}

// UpdateStatement represents a SQL UPDATE statement with SET clauses and optional WHERE clause
type UpdateStatement struct {
	TableStatement
	SetClauses  []SetClause
	WhereClause *plan.FilterNode
}

// NewUpdateStatement creates a new UPDATE statement
func NewUpdateStatement(tableName, alias string) *UpdateStatement {
	return &UpdateStatement{
		TableStatement: NewTableStatement(Update, tableName, alias),
		SetClauses:     make([]SetClause, 0),
		WhereClause:    nil,
	}
}

// AddSetClause adds a SET clause to the UPDATE statement
func (us *UpdateStatement) AddSetClause(fieldName string, value types.Field) {
	us.SetClauses = append(us.SetClauses, SetClause{
		FieldName: fieldName,
		Value:     value,
	})
}

// SetWhereClause sets the WHERE clause filter
func (us *UpdateStatement) SetWhereClause(filter *plan.FilterNode) {
	us.WhereClause = filter
}

// HasWhereClause returns true if the statement has a WHERE clause
func (us *UpdateStatement) HasWhereClause() bool {
	return us.WhereClause != nil
}

// Validate checks if the statement is valid
func (us *UpdateStatement) Validate() error {
	if us.TableName == "" {
		return NewValidationError(Update, "TableName", "table name cannot be empty")
	}

	if len(us.SetClauses) == 0 {
		return NewValidationError(Update, "SetClauses", "at least one SET clause is required")
	}

	for i, clause := range us.SetClauses {
		if clause.FieldName == "" {
			return NewValidationError(Update, fmt.Sprintf("SetClauses[%d].FieldName", i), "field name cannot be empty")
		}
		if clause.Value == nil {
			return NewValidationError(Update, fmt.Sprintf("SetClauses[%d].Value", i), "value cannot be nil")
		}
	}
	return nil
}

// String returns a string representation of the UPDATE statement
func (us *UpdateStatement) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s", us.TableName))

	if us.HasAlias() {
		sb.WriteString(fmt.Sprintf(" %s", us.Alias))
	}

	sb.WriteString(" SET ")
	for i, setClause := range us.SetClauses {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s = %s", setClause.FieldName, setClause.Value.String()))
	}

	if us.HasWhereClause() {
		sb.WriteString(fmt.Sprintf(" WHERE %s", us.WhereClause.String()))
	}

	return sb.String()
}
