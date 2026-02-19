package statements

import (
	"fmt"
	"storemy/pkg/plan"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
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

// Validate checks if the statement is valid
// For DROP TABLE, we just need to ensure that the table name is not empty
func (dts *DropStatement) Validate() error {
	if dts.TableName == "" {
		return NewValidationError(DropTable, "TableName", "table name cannot be empty")
	}
	return nil
}

// String returns a string representation of the DROP TABLE statement
func (dts *DropStatement) String() string {
	var sb statementBuilder
	sb.WriteString("DROP TABLE ")
	sb.writeIf(dts.IfExists, "IF EXISTS ")
	sb.WriteString(dts.TableName)
	return sb.String()
}

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
	var sb statementBuilder
	sb.WriteString("SHOW INDEXES")
	sb.writeClause("FROM", sis.TableName)
	return sb.String()
}

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
func (u *UpdateStatement) AddSetClause(fieldName string, value types.Field) {
	u.SetClauses = append(u.SetClauses, SetClause{
		FieldName: fieldName,
		Value:     value,
	})
}

// SetWhereClause sets the WHERE clause filter
func (u *UpdateStatement) SetWhereClause(filter *plan.FilterNode) {
	u.WhereClause = filter
}

// HasWhereClause returns true if the statement has a WHERE clause
func (u *UpdateStatement) HasWhereClause() bool {
	return u.WhereClause != nil
}

// Validate checks if the statement is valid
func (u *UpdateStatement) Validate() error {
	if u.TableName == "" {
		return NewValidationError(Update, "TableName", "table name cannot be empty")
	}

	if len(u.SetClauses) == 0 {
		return NewValidationError(Update, "SetClauses", "at least one SET clause is required")
	}

	for i, clause := range u.SetClauses {
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
func (u *UpdateStatement) String() string {
	var sb statementBuilder
	fmt.Fprintf(&sb, "UPDATE %s", u.TableName)
	sb.writeIf(u.HasAlias(), fmt.Sprintf(" %s", u.Alias))
	sb.WriteString(" SET ")
	for i, setClause := range u.SetClauses {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s = %s", setClause.FieldName, setClause.Value.String())
	}
	sb.writeIf(u.HasWhereClause(), fmt.Sprintf(" WHERE %s", u.WhereClause.String()))
	return sb.String()
}

// InsertStatement represents a SQL INSERT statement with table name, field names, and values.
type InsertStatement struct {
	BaseStatement
	TableName string          // The target table name for the INSERT operation
	Fields    []string        // Column names to insert data into
	Values    [][]types.Field // Rows of values to be inserted, each row contains fields matching the Fields slice
}

// NewInsertStatement creates a new INSERT statement
func NewInsertStatement(tableName string) *InsertStatement {
	return &InsertStatement{
		BaseStatement: NewBaseStatement(Insert),
		TableName:     tableName,
		Fields:        make([]string, 0),
		Values:        make([][]types.Field, 0),
	}
}

// AddFieldNames sets the field names for the INSERT statement
func (s *InsertStatement) AddFieldNames(fields []string) {
	s.Fields = fields
}

// AddValues adds a row of values to be inserted
func (s *InsertStatement) AddValues(values []types.Field) {
	s.Values = append(s.Values, values)
}

// ValueCount returns the number of rows to be inserted
func (s *InsertStatement) ValueCount() int {
	return len(s.Values)
}

// Validate checks if the statement is valid
func (s *InsertStatement) Validate() error {
	if s.TableName == "" {
		return NewValidationError(Insert, "TableName", "table name cannot be empty")
	}

	if len(s.Values) == 0 {
		return NewValidationError(Insert, "Values", "at least one row of values is required")
	}

	if len(s.Fields) > 0 {
		for i, row := range s.Values {
			if len(row) != len(s.Fields) {
				return NewValidationError(
					Insert,
					fmt.Sprintf("Values[%d]", i),
					fmt.Sprintf("expected %d values, got %d", len(s.Fields), len(row)),
				)
			}
		}
	}

	expectedCount := len(s.Values[0])
	for i, row := range s.Values {
		if len(row) != expectedCount {
			return NewValidationError(
				Insert,
				fmt.Sprintf("Values[%d]", i),
				fmt.Sprintf("all rows must have the same number of values (expected %d, got %d)", expectedCount, len(row)),
			)
		}
	}

	return nil
}

// String returns a string representation of the INSERT statement
func (s *InsertStatement) String() string {
	var sb statementBuilder
	fmt.Fprintf(&sb, "INSERT INTO %s", s.TableName)
	sb.writeIf(len(s.Fields) > 0, fmt.Sprintf(" (%s)", strings.Join(s.Fields, ", ")))

	sb.WriteString(" VALUES ")
	for i, row := range s.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			if val != nil {
				sb.WriteString(val.String())
			} else {
				sb.WriteString("NULL")
			}
		}
		sb.WriteString(")")
	}

	return sb.String()
}

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

func (ds *DeleteStatement) Validate() error {
	if ds.TableName == "" {
		return NewValidationError(Delete, "TableName", "table name cannot be empty")
	}
	return nil
}

func (ds *DeleteStatement) String() string {
	var sb statementBuilder
	fmt.Fprintf(&sb, "DELETE FROM %s", ds.TableName)
	sb.writeIf(ds.HasAlias(), fmt.Sprintf(" %s", ds.Alias))
	sb.writeIf(ds.HasWhereClause(), fmt.Sprintf(" WHERE %s", ds.WhereClause.String()))
	return sb.String()
}

// ExplainOptions contains options for the EXPLAIN command
type ExplainOptions struct {
	Analyze bool   // If true, actually execute the query and show real statistics
	Format  string // Output format: "TEXT", "JSON", etc. (default: "TEXT")
}

// ExplainStatement represents an EXPLAIN command
type ExplainStatement struct {
	BaseStatement
	Statement Statement      // The underlying statement to explain
	Options   ExplainOptions // Options for the explain command
}

// NewExplainStatement creates a new EXPLAIN statement
func NewExplainStatement(stmt Statement, options ExplainOptions) *ExplainStatement {
	return &ExplainStatement{
		BaseStatement: NewBaseStatement(Explain),
		Statement:     stmt,
		Options:       options,
	}
}

func (es *ExplainStatement) GetStatement() Statement {
	return es.Statement
}

func (es *ExplainStatement) GetOptions() ExplainOptions {
	return es.Options
}

func (es *ExplainStatement) String() string {
	result := "EXPLAIN"
	if es.Options.Analyze {
		result += " ANALYZE"
	}
	if es.Options.Format != "" && es.Options.Format != "TEXT" {
		result += " FORMAT " + es.Options.Format
	}
	result += " " + es.Statement.String()
	return result
}

func (es *ExplainStatement) Validate() error {
	if es.Statement == nil {
		return NewValidationError(Explain, "Statement", "EXPLAIN statement must have an underlying statement")
	}
	return es.Statement.Validate()
}

// FieldDefinition represents a column definition in a CREATE TABLE statement
type FieldDefinition struct {
	Name          string
	Type          types.Type
	NotNull       bool
	DefaultValue  types.Field
	AutoIncrement bool
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
		Name:          name,
		Type:          fieldType,
		NotNull:       notNull,
		DefaultValue:  defaultValue,
		AutoIncrement: false,
	})
}

// AddFieldWithAutoInc adds a field definition with auto-increment to the CREATE TABLE statement
func (cts *CreateStatement) AddFieldWithAutoInc(name string, fieldType types.Type, notNull bool, defaultValue types.Field, autoInc bool) {
	cts.Fields = append(cts.Fields, FieldDefinition{
		Name:          name,
		Type:          fieldType,
		NotNull:       notNull,
		DefaultValue:  defaultValue,
		AutoIncrement: autoInc,
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

	autoIncCount := 0
	var autoIncField *FieldDefinition
	for i := range cts.Fields {
		if cts.Fields[i].AutoIncrement {
			autoIncCount++
			autoIncField = &cts.Fields[i]

			if cts.Fields[i].Type != types.IntType {
				return NewValidationError(CreateTable, fmt.Sprintf("Fields[%d].AutoIncrement", i), "auto-increment column must be of type INT")
			}
		}
	}

	if autoIncCount > 1 {
		return NewValidationError(CreateTable, "AutoIncrement", "table can have only one auto-increment column")
	}

	if autoIncCount == 1 && cts.PrimaryKey != "" && autoIncField.Name != cts.PrimaryKey {
		// This is allowed but not recommended - could add a warning system later
		fmt.Println("Warning: AUTO_INCREMENT should be on the primary key only")
	}

	return nil
}

// String returns a string representation of the CREATE TABLE statement
func (cts *CreateStatement) String() string {
	var sb statementBuilder
	sb.WriteString("CREATE TABLE ")
	sb.writeIf(cts.IfNotExists, "IF NOT EXISTS ")
	sb.WriteString(fmt.Sprintf("%s (\n", cts.TableName))

	for i, field := range cts.Fields {
		sb.writeIf(i > 0, ",\n")
		sb.WriteString(fmt.Sprintf("  %s %s", field.Name, field.Type.String()))
		sb.writeIf(field.AutoIncrement, " AUTO_INCREMENT")
		sb.writeIf(field.NotNull, " NOT NULL")
		if field.DefaultValue != nil {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", field.DefaultValue.String()))
		}
	}

	sb.writeIf(cts.PrimaryKey != "", fmt.Sprintf(",\n  PRIMARY KEY (%s)", cts.PrimaryKey))
	sb.WriteString("\n)")

	return sb.String()
}

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
	var sb statementBuilder
	sb.WriteString("CREATE INDEX ")
	sb.writeIf(cis.IfNotExists, "IF NOT EXISTS ")
	sb.WriteString(fmt.Sprintf("%s ON %s(%s)", cis.IndexName, cis.TableName, cis.ColumnName))
	sb.writeClause("USING", string(cis.IndexType))
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

func (dis *DropIndexStatement) Validate() error {
	if dis.IndexName == "" {
		return NewValidationError(DropIndex, "IndexName", "index name cannot be empty")
	}

	return nil
}

func (dis *DropIndexStatement) String() string {
	var sb statementBuilder
	sb.WriteString("DROP INDEX ")
	sb.writeIf(dis.IfExists, "IF EXISTS ")
	sb.WriteString(dis.IndexName)
	sb.writeClause("ON", dis.TableName)
	return sb.String()
}

// SelectStatement represents a SQL SELECT statement with its execution plan
type SelectStatement struct {
	BaseStatement
	Plan *plan.SelectPlan
}

// NewSelectStatement creates a new SELECT statement with the given plan
func NewSelectStatement(plan *plan.SelectPlan) *SelectStatement {
	return &SelectStatement{
		BaseStatement: NewBaseStatement(Select),
		Plan:          plan,
	}
}

// GetPlan returns the execution plan for this SELECT statement
func (ss *SelectStatement) GetPlan() *plan.SelectPlan {
	return ss.Plan
}

// IsSelectAll returns true if the statement uses SELECT *
func (ss *SelectStatement) IsSelectAll() bool {
	return ss.Plan.SelectAll()
}

// HasAggregation returns true if the statement contains aggregation functions
func (ss *SelectStatement) HasAggregation() bool {
	return ss.Plan.HasAgg()
}

// HasGroupBy returns true if the statement has a GROUP BY clause
func (ss *SelectStatement) HasGroupBy() bool {
	return ss.Plan.GroupByField() != ""
}

// HasOrderBy returns true if the statement has an ORDER BY clause
func (ss *SelectStatement) HasOrderBy() bool {
	return ss.Plan.HasOrderBy()
}

// TableCount returns the number of tables in the FROM clause
func (ss *SelectStatement) TableCount() int {
	return len(ss.Plan.Tables())
}

// JoinCount returns the number of JOIN clauses
func (ss *SelectStatement) JoinCount() int {
	return len(ss.Plan.Joins())
}

func (ss *SelectStatement) FilterCount() int {
	return len(ss.Plan.Filters())
}

func (ss *SelectStatement) Validate() error {
	if ss.Plan == nil {
		return NewValidationError(Select, "Plan", "plan cannot be nil")
	}

	if len(ss.Plan.Tables()) == 0 {
		return NewValidationError(Select, "Tables", "at least one table is required in FROM clause")
	}

	return nil
}

func (ss *SelectStatement) String() string {
	return ss.Plan.String()
}
