package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
)

func parseInsertStatement(l *lexer.Lexer) (*statements.InsertStatement, error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.INSERT); err != nil {
		return nil, err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.INTO); err != nil {
		return nil, err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return nil, err
	}

	tableName := token.Value
	statement := statements.NewInsertStatement(tableName)

	token = l.NextToken()
	if token.Type == lexer.LPAREN {
		fields, err := parseFieldList(l)
		if err != nil {
			return nil, err
		}
		statement.AddFieldNames(fields)
		token = l.NextToken()
	}

	if token.Type == lexer.VALUES {
		return parseInsertValues(l, statement)
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT, got %s", token.Value)
	}
}

func parseInsertValues(l *lexer.Lexer, stmt *statements.InsertStatement) (*statements.InsertStatement, error) {
	for {
		token := l.NextToken()
		if err := expectToken(token, lexer.LPAREN); err != nil {
			return nil, err
		}

		values, err := parseValueList(l)
		if err != nil {
			return nil, err
		}

		stmt.AddValues(values)

		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else {
			l.SetPos(token.Position) // Put it back
			break
		}
	}

	return stmt, nil
}

func parseValueList(l *lexer.Lexer) ([]types.Field, error) {
	values := make([]types.Field, 0)

	for {
		value, err := parseValue(l)
		if err != nil {
			return nil, err
		}
		values = append(values, value)

		token := l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else if token.Type == lexer.RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}

	return values, nil
}

func parseFieldList(l *lexer.Lexer) ([]string, error) {
	fields := make([]string, 0)
	for {
		token := l.NextToken()
		if token.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected field name, got %s", token.Value)
		}

		fields = append(fields, token.Value)
		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else if token.Type == lexer.RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected comma or right parenthesis, got %s", token.Value)
		}
	}

	return fields, nil
}
