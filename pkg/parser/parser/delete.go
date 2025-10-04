package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
)

func parseDeleteStatement(l *lexer.Lexer) (*statements.DeleteStatement, error) {
	if err := expectTokenSequence(l, lexer.DELETE, lexer.FROM); err != nil {
		return nil, err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	statement := statements.NewDeleteStatement(tableName, alias)

	token := l.NextToken()
	if token.Type == lexer.WHERE {
		filter, err := parseWhereCondition(l)
		if err != nil {
			return nil, err
		}
		statement.SetWhereClause(filter)
	} else {
		l.SetPos(token.Position)
	}

	return statement, nil
}

func parseWhereCondition(l *lexer.Lexer) (*plan.FilterNode, error) {
	fieldName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected field name in WHERE: %w", err)
	}

	opValue, err := parseValueWithType(l, lexer.OPERATOR)
	if err != nil {
		return nil, fmt.Errorf("expected operator in WHERE: %w", err)
	}

	pred, err := parseOperator(opValue)
	if err != nil {
		return nil, err
	}

	constant, err := parseValueWithType(l, lexer.STRING, lexer.INT)
	if err != nil {
		return nil, fmt.Errorf("expected value in WHERE: %w", err)
	}

	return plan.NewFilterNode("", fieldName, pred, constant), nil
}
