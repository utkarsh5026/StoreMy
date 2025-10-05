package statements

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
