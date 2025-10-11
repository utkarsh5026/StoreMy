package statements

import (
	"strings"
)

// DropStatement represents a SQL DROP TABLE statement
type DropStatement struct {
	BaseStatement
	TableName string
	IfExists  bool
}

// NewDropStatement creates a new DROP TABLE statement
func NewDropStatement(tableName string, ifExists bool) *DropStatement {
	return &DropStatement{
		BaseStatement: NewBaseStatement(DropTable),
		TableName:     tableName,
		IfExists:      ifExists,
	}
}

func (dts *DropStatement) Validate() error {
	if dts.TableName == "" {
		return NewValidationError(DropTable, "TableName", "table name cannot be empty")
	}
	return nil
}

// String returns a string representation of the DROP TABLE statement
func (dts *DropStatement) String() string {
	var sb strings.Builder
	sb.WriteString("DROP TABLE ")

	if dts.IfExists {
		sb.WriteString("IF EXISTS ")
	}

	sb.WriteString(dts.TableName)
	return sb.String()
}
