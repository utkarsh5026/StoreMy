package statements

import (
	"fmt"
	"storemy/pkg/parser/plan"
	"strings"
)

// DeleteStatement represents a SQL DELETE statement with optional WHERE clause
type DeleteStatement struct {
	TableStatement
	WhereClause *plan.FilterNode
}

// NewDeleteStatement creates a new DELETE statement
func NewDeleteStatement(tableName, alias string) *DeleteStatement {
	return &DeleteStatement{
		TableStatement: NewTableStatement(Delete, tableName, alias),
		WhereClause:    nil,
	}
}

// SetWhereClause sets the WHERE clause filter
func (ds *DeleteStatement) SetWhereClause(filter *plan.FilterNode) {
	ds.WhereClause = filter
}

// GetWhereClause returns the WHERE clause filter (can be nil)
func (ds *DeleteStatement) GetWhereClause() *plan.FilterNode {
	return ds.WhereClause
}

// HasWhereClause returns true if the statement has a WHERE clause
func (ds *DeleteStatement) HasWhereClause() bool {
	return ds.WhereClause != nil
}

// Validate checks if the statement is valid
func (ds *DeleteStatement) Validate() error {
	if ds.TableName == "" {
		return NewValidationError(Delete, "TableName", "table name cannot be empty")
	}
	return nil
}

// String returns a string representation of the DELETE statement
func (ds *DeleteStatement) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DELETE FROM %s", ds.TableName))

	if ds.HasAlias() {
		sb.WriteString(fmt.Sprintf(" %s", ds.Alias))
	}

	if ds.HasWhereClause() {
		sb.WriteString(fmt.Sprintf(" WHERE %s", ds.WhereClause.String()))
	}

	return sb.String()
}
