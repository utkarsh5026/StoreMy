package statements

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// InsertStatement represents a SQL INSERT statement with table name, field names, and values.
type InsertStatement struct {
	TableName string          // The target table name for the INSERT operation
	Fields    []string        // Column names to insert data into
	Values    [][]types.Field // Rows of values to be inserted, each row contains fields matching the Fields slice
}

func NewInsertStatement(tableName string) *InsertStatement {
	return &InsertStatement{
		TableName: tableName,
		Fields:    make([]string, 0),
		Values:    make([][]types.Field, 0),
	}
}

func (s *InsertStatement) AddFieldNames(fields []string) {
	s.Fields = fields
}

func (s *InsertStatement) AddValues(values []types.Field) {
	s.Values = append(s.Values, values)
}

func (s *InsertStatement) GetType() StatementType {
	return Insert
}

func (s *InsertStatement) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s)", s.TableName, strings.Join(s.Fields, ", "))
}
