package statements

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

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
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s", s.TableName))

	if len(s.Fields) > 0 {
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(s.Fields, ", ")))
	}

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
