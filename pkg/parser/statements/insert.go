package statements

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

type InsertStatement struct {
	TableName string
	Fields    []string
	Values    [][]types.Field
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
