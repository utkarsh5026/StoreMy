package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
)

func parseDeleteStatement(l *lexer.Lexer) (*statements.DeleteStatement, error) {
	token := l.NextToken()
	if token.Type != lexer.DELETE {
		return nil, fmt.Errorf("expected DELETE, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.FROM {
		return nil, fmt.Errorf("expected FROM, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", token.Value)
	}

	tableName := token.Value
	alias := tableName

	token = l.NextToken()
	if token.Type == lexer.IDENTIFIER {
		alias = token.Value
		token = l.NextToken()
	}

	statement := statements.NewDeleteStatement(tableName, alias)
	if token.Type == lexer.WHERE {
		filter, err := parseWhereCondition(l)
		if err != nil {
			return nil, err
		}
		statement.SetWhereClause(filter)
	} else {
		l.SetPos(token.Position) // Put it back
	}
	return statement, nil
}

func parseWhereCondition(l *lexer.Lexer) (*plan.FilterNode, error) {
	token := l.NextToken()
	if token.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected field name in WHERE, got %s", token.Value)
	}
	fieldName := token.Value

	token = l.NextToken()
	if token.Type != lexer.OPERATOR {
		return nil, fmt.Errorf("expected operator in WHERE, got %s", token.Value)
	}

	pred, err := parseOperator(token.Value)
	if err != nil {
		return nil, err
	}

	token = l.NextToken()
	var constant string
	switch token.Type {
	case lexer.STRING, lexer.INT:
		constant = token.Value
	default:
		return nil, fmt.Errorf("expected value in WHERE, got %s", token.Value)
	}

	return plan.NewFilterNode("", fieldName, pred, constant), nil
}
