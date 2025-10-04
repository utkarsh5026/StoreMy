package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
)

type fieldConstraints struct {
	NotNull      bool
	DefaultValue types.Field
}

func parseCreateStatement(l *lexer.Lexer) (*statements.CreateStatement, error) {
	if err := expectTokenSequence(l, lexer.CREATE, lexer.TABLE); err != nil {
		return nil, err
	}

	ifNotExists, err := parseIfNotExists(l)
	if err != nil {
		return nil, err
	}

	tableName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected table name: %w", err)
	}

	stmt := statements.NewCreateStatement(tableName, ifNotExists)

	if err := expectTokenSequence(l, lexer.LPAREN); err != nil {
		return nil, err
	}

	if err := parseTableDefinition(l, stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func parseIfNotExists(l *lexer.Lexer) (bool, error) {
	token := l.NextToken()
	if token.Type != lexer.IF {
		l.SetPos(token.Position)
		return false, nil
	}

	if err := expectTokenSequence(l, lexer.NOT, lexer.EXISTS); err != nil {
		return false, err
	}

	return true, nil
}

func readPrimaryKey(l *lexer.Lexer, stmt *statements.CreateStatement) error {
	if err := expectTokenSequence(l, lexer.KEY, lexer.LPAREN); err != nil {
		return err
	}

	fieldName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return fmt.Errorf("expected field name in PRIMARY KEY: %w", err)
	}

	stmt.SetPrimaryKey(fieldName)
	return expectTokenSequence(l, lexer.RPAREN)
}

func parseTableDefinition(l *lexer.Lexer, stmt *statements.CreateStatement) error {
	for {
		token := l.NextToken()

		switch token.Type {
		case lexer.PRIMARY:
			if err := readPrimaryKey(l, stmt); err != nil {
				return err
			}
		case lexer.IDENTIFIER:
			if err := parseFieldDefinition(l, stmt, token.Value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("expected field definition or PRIMARY KEY, got %s", token.Value)
		}

		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else if token.Type == lexer.RPAREN {
			break
		} else {
			return fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}
	return nil
}

func parseFieldDefinition(l *lexer.Lexer, stmt *statements.CreateStatement, fieldName string) error {
	token := l.NextToken()
	fieldType, err := parseDataType(token)
	if err != nil {
		return err
	}

	constraints, err := parseFieldConstraints(l)
	if err != nil {
		return err
	}

	stmt.AddField(fieldName, fieldType, constraints.NotNull, constraints.DefaultValue)
	return nil
}

func parseFieldConstraints(l *lexer.Lexer) (*fieldConstraints, error) {
	constraints := &fieldConstraints{}

	for {
		token := l.NextToken()

		switch token.Type {
		case lexer.NOT:
			if err := expectTokenSequence(l, lexer.NULL); err != nil {
				return nil, err
			}
			constraints.NotNull = true
		case lexer.DEFAULT:
			defaultValue, err := parseValue(l)
			if err != nil {
				return nil, err
			}
			constraints.DefaultValue = defaultValue
		default:
			l.SetPos(token.Position) // Put it back
			return constraints, nil
		}
	}
}

func parseDataType(token lexer.Token) (types.Type, error) {
	switch token.Type {
	case lexer.INT:
		return types.IntType, nil
	case lexer.VARCHAR, lexer.TEXT:
		return types.StringType, nil
	case lexer.BOOLEAN:
		return types.BoolType, nil
	case lexer.FLOAT:
		return types.FloatType, nil
	default:
		return 0, fmt.Errorf("unknown data type: %s", token.Value)
	}
}
