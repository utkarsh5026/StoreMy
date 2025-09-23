package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"strconv"
)

func parseInsertStatement(l *lexer.Lexer) (*statements.InsertStatement, error) {
	token := l.NextToken()
	if token.Type != lexer.INSERT {
		return nil, fmt.Errorf("expected INSERT, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.INTO {
		return nil, fmt.Errorf("expected INTO, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", token.Value)
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
		if token.Type != lexer.LPAREN {
			return nil, fmt.Errorf("expected '(', got %s", token.Value)
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

func parseValue(l *lexer.Lexer) (types.Field, error) {
	token := l.NextToken()

	switch token.Type {
	case lexer.STRING:
		return types.NewStringField(token.Value, types.StringMaxSize), nil
	case lexer.INT:
		value, err := strconv.Atoi(token.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value: %s", token.Value)
		}
		return types.NewIntField(int32(value)), nil
	case lexer.NULL:
		return nil, nil // NULL value
	default:
		return nil, fmt.Errorf("expected value, got %s", token.Value)
	}
}
