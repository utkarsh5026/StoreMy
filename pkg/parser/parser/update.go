package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

func parseUpdateStatement(l *lexer.Lexer) (*statements.UpdateStatement, error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.UPDATE); err != nil {
		return nil, err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.SET); err != nil {
		return nil, err
	}

	stmt := statements.NewUpdateStatement(tableName, alias)
	for {
		token = l.NextToken()
		if err := expectToken(token, lexer.IDENTIFIER); err != nil {
			return nil, err
		}
		fieldName := token.Value

		token = l.NextToken()
		if token.Type != lexer.OPERATOR || token.Value != "=" {
			return nil, fmt.Errorf("expected '=', got %s", token.Value)
		}

		value, err := parseValue(l)
		if err != nil {
			return nil, err
		}

		stmt.AddSetClause(fieldName, value)
		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else {
			l.SetPos(token.Position) // Put it back
			break
		}
	}

	token = l.NextToken()
	if token.Type == lexer.WHERE {
		filter, err := parseWhereCondition(l)
		if err != nil {
			return nil, err
		}
		stmt.SetWhereClause(filter)
	} else {
		l.SetPos(token.Position)
	}

	return stmt, nil
}

func parseTableWithAlias(l *lexer.Lexer) (tableName, alias string, err error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return "", "", err
	}

	tableName = token.Value
	alias = tableName

	token = l.NextToken()
	if token.Type == lexer.IDENTIFIER {
		alias = token.Value
	} else {
		l.SetPos(token.Position)
	}

	return tableName, alias, nil
}
