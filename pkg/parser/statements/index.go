package statements

import (
	"fmt"
	"storemy/pkg/storage/index"
	"strings"
)

// CreateIndexStatement represents a SQL CREATE INDEX statement
// Format: CREATE INDEX [IF NOT EXISTS] index_name ON table_name(column_name) [USING {HASH|BTREE}]
type CreateIndexStatement struct {
	BaseStatement
	IndexName, TableName, ColumnName string
	IndexType                        index.IndexType
	IfNotExists                      bool
}

// NewCreateIndexStatement creates a new CREATE INDEX statement
func NewCreateIndexStatement(indexName, tableName, columnName string, indexType index.IndexType, ifNotExists bool) *CreateIndexStatement {
	return &CreateIndexStatement{
		BaseStatement: NewBaseStatement(CreateIndex),
		IndexName:     indexName,
		TableName:     tableName,
		ColumnName:    columnName,
		IndexType:     indexType,
		IfNotExists:   ifNotExists,
	}
}

// Validate checks if the CREATE INDEX statement is valid
func (cis *CreateIndexStatement) Validate() error {
	if cis.IndexName == "" {
		return NewValidationError(CreateIndex, "IndexName", "index name cannot be empty")
	}

	if cis.TableName == "" {
		return NewValidationError(CreateIndex, "TableName", "table name cannot be empty")
	}

	if cis.ColumnName == "" {
		return NewValidationError(CreateIndex, "ColumnName", "column name cannot be empty")
	}

	if cis.IndexType == "" {
		cis.IndexType = index.HashIndex
	}

	if cis.IndexType != index.HashIndex && cis.IndexType != index.BTreeIndex {
		return NewValidationError(CreateIndex, "IndexType", fmt.Sprintf("invalid index type: %s (must be HASH or BTREE)", cis.IndexType))
	}

	return nil
}

// String returns a string representation of the CREATE INDEX statement
func (cis *CreateIndexStatement) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE INDEX ")

	if cis.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}

	sb.WriteString(fmt.Sprintf("%s ON %s(%s)", cis.IndexName, cis.TableName, cis.ColumnName))

	if cis.IndexType != "" {
		sb.WriteString(fmt.Sprintf(" USING %s", cis.IndexType))
	}

	return sb.String()
}

// DropIndexStatement represents a SQL DROP INDEX statement
// Format: DROP INDEX [IF EXISTS] index_name [ON table_name]
type DropIndexStatement struct {
	BaseStatement
	IndexName, TableName string
	IfExists             bool
}

// NewDropIndexStatement creates a new DROP INDEX statement
func NewDropIndexStatement(indexName, tableName string, ifExists bool) *DropIndexStatement {
	return &DropIndexStatement{
		BaseStatement: NewBaseStatement(DropIndex),
		IndexName:     indexName,
		TableName:     tableName,
		IfExists:      ifExists,
	}
}

// Validate checks if the DROP INDEX statement is valid
func (dis *DropIndexStatement) Validate() error {
	if dis.IndexName == "" {
		return NewValidationError(DropIndex, "IndexName", "index name cannot be empty")
	}

	return nil
}

// String returns a string representation of the DROP INDEX statement
func (dis *DropIndexStatement) String() string {
	var sb strings.Builder
	sb.WriteString("DROP INDEX ")

	if dis.IfExists {
		sb.WriteString("IF EXISTS ")
	}

	sb.WriteString(dis.IndexName)

	if dis.TableName != "" {
		sb.WriteString(fmt.Sprintf(" ON %s", dis.TableName))
	}

	return sb.String()
}
