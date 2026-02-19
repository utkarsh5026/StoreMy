package statements

import "strings"

// statementBuilder wraps strings.Builder with helpers that eliminate
// the repetitive if-then-WriteString pattern in Statement.String() methods.
type statementBuilder struct {
	strings.Builder
}

// writeIf appends s only when cond is true.
func (b *statementBuilder) writeIf(cond bool, s string) {
	if cond {
		b.WriteString(s)
	}
}

// writeClause appends " keyword value" only when value is non-empty.
func (b *statementBuilder) writeClause(keyword, value string) {
	if value != "" {
		b.WriteString(" " + keyword + " " + value)
	}
}

// BaseStatement provides common functionality for all statement types
type BaseStatement struct {
	stmtType StatementType
}

func NewBaseStatement(stmtType StatementType) BaseStatement {
	return BaseStatement{stmtType: stmtType}
}

func (bs *BaseStatement) GetType() StatementType {
	return bs.stmtType
}

// TableStatement provides common functionality for statements that operate on a single table
type TableStatement struct {
	BaseStatement
	TableName string
	Alias     string
}

// NewTableStatement creates a new table statement
func NewTableStatement(stmtType StatementType, tableName, alias string) TableStatement {
	return TableStatement{
		BaseStatement: NewBaseStatement(stmtType),
		TableName:     tableName,
		Alias:         alias,
	}
}

// GetTableName returns the table name
func (ts *TableStatement) GetTableName() string {
	return ts.TableName
}

// GetAlias returns the table alias
func (ts *TableStatement) GetAlias() string {
	if ts.Alias != "" {
		return ts.Alias
	}
	return ts.TableName
}

// HasAlias returns true if the statement has an alias different from the table name
func (ts *TableStatement) HasAlias() bool {
	return ts.Alias != "" && ts.Alias != ts.TableName
}

// requireNonEmpty returns a ValidationError when value is empty.
// It mirrors the statementBuilder pattern used in String() methods.
func (bs *BaseStatement) requireNonEmpty(fieldName, value, msg string) error {
	if value == "" {
		return NewValidationError(bs.stmtType, fieldName, msg)
	}
	return nil
}

// requireNonEmptySlice returns a ValidationError when length is zero.
func (bs *BaseStatement) requireNonEmptySlice(fieldName string, length int, msg string) error {
	if length == 0 {
		return NewValidationError(bs.stmtType, fieldName, msg)
	}
	return nil
}
