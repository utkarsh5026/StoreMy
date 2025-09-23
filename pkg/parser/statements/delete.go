package statements

import (
	"fmt"
	"storemy/pkg/parser/plan"
	"strings"
)

type DeleteStatement struct {
	TableName   string
	Alias       string
	WhereClause *plan.FilterNode
}

func NewDeleteStatement(tableName, alias string) *DeleteStatement {
	return &DeleteStatement{
		TableName: tableName,
		Alias:     alias,
	}
}

func (ds *DeleteStatement) SetWhereClause(filter *plan.FilterNode) {
	ds.WhereClause = filter
}

func (ds *DeleteStatement) GetType() StatementType {
	return Delete
}

func (ds *DeleteStatement) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DELETE FROM %s", ds.TableName))

	if ds.Alias != "" && ds.Alias != ds.TableName {
		sb.WriteString(fmt.Sprintf(" %s", ds.Alias))
	}

	if ds.WhereClause != nil {
		sb.WriteString(fmt.Sprintf(" WHERE %s", ds.WhereClause.String()))
	}

	return sb.String()
}
