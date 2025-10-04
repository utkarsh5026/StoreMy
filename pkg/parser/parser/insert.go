package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
)

func parseInsertStatement(l *lexer.Lexer) (*statements.InsertStatement, error) {
	if err := expectTokenSequence(l, lexer.INSERT, lexer.INTO); err != nil {
		return nil, err
	}

	tableName, _, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	statement := statements.NewInsertStatement(tableName)

	token := l.NextToken()
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
	return parseDelimitedList(l, parseValue, lexer.COMMA, lexer.RPAREN)
}

func parseFieldList(l *lexer.Lexer) ([]string, error) {
	return parseDelimitedList(l,
		func(l *lexer.Lexer) (string, error) {
			token := l.NextToken()
			if err := expectToken(token, lexer.IDENTIFIER); err != nil {
				return "", err
			}
			return token.Value, nil
		},
		lexer.COMMA,
		lexer.RPAREN,
	)
}
