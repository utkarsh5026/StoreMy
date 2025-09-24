package statements

import (
	"fmt"
	"storemy/pkg/parser/plan"
	"storemy/pkg/types"
	"strings"
)

type UpdateStatement struct {
	TableName   string
	Alias       string
	SetClauses  []*SetClause
	WhereClause *plan.FilterNode
}

type SetClause struct {
	FieldName string
	Value     types.Field
}

func NewUpdateStatement(tableName, alias string) *UpdateStatement {
	return &UpdateStatement{
		TableName:  tableName,
		Alias:      alias,
		SetClauses: make([]*SetClause, 0),
	}
}

func (us *UpdateStatement) AddSetClause(fieldName string, value types.Field) {
	us.SetClauses = append(us.SetClauses, &SetClause{
		FieldName: fieldName,
		Value:     value,
	})
}

func (us *UpdateStatement) SetWhereClause(filter *plan.FilterNode) {
	us.WhereClause = filter
}

func (us *UpdateStatement) GetType() StatementType {
	return Update
}

func (us *UpdateStatement) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s", us.TableName))

	if us.Alias != "" && us.Alias != us.TableName {
		sb.WriteString(fmt.Sprintf(" %s", us.Alias))
	}

	sb.WriteString(" SET ")
	for i, setClause := range us.SetClauses {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s = %s", setClause.FieldName, setClause.Value.String()))
	}

	if us.WhereClause != nil {
		sb.WriteString(fmt.Sprintf(" WHERE %s", us.WhereClause.String()))
	}

	return sb.String()
}
