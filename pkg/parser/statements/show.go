package statements

import (
	"fmt"
	"strings"
)

// ShowIndexesStatement represents a SQL SHOW INDEXES statement
// Format: SHOW INDEXES [FROM table_name]
type ShowIndexesStatement struct {
	BaseStatement
	TableName string // Optional: if specified, show only indexes for this table
}

// NewShowIndexesStatement creates a new SHOW INDEXES statement
func NewShowIndexesStatement(tableName string) *ShowIndexesStatement {
	return &ShowIndexesStatement{
		BaseStatement: NewBaseStatement(ShowIndexes),
		TableName:     tableName,
	}
}

// Validate checks if the SHOW INDEXES statement is valid
func (sis *ShowIndexesStatement) Validate() error {
	// TableName is optional, so no validation needed
	return nil
}

// String returns a string representation of the SHOW INDEXES statement
func (sis *ShowIndexesStatement) String() string {
	var sb strings.Builder
	sb.WriteString("SHOW INDEXES")

	if sis.TableName != "" {
		sb.WriteString(fmt.Sprintf(" FROM %s", sis.TableName))
	}

	return sb.String()
}
