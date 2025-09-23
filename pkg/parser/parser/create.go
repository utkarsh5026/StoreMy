package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
)

func parseCreateStatement(l *lexer.Lexer) (*statements.CreateStatement, error) {
	// CREATE TABLE [IF NOT EXISTS] table_name (field_def1, field_def2, ..., [PRIMARY KEY (field)])
	token := l.NextToken()
	if err := expectToken(token, lexer.CREATE); err != nil {
		return nil, err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.TABLE); err != nil {
		return nil, err
	}

	ifNotExists := false
	token = l.NextToken()
	if token.Type == lexer.IF {
		token = l.NextToken()
		if err := expectToken(token, lexer.NOT); err != nil {
			return nil, err
		}

		token = l.NextToken()
		if err := expectToken(token, lexer.EXISTS); err != nil {
			return nil, err
		}

		ifNotExists = true
		token = l.NextToken()
	}

	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return nil, err
	}

	tableName := token.Value
	stmt := statements.NewCreateStatement(tableName, ifNotExists)

	token = l.NextToken()
	if token.Type != lexer.LPAREN {
		return nil, fmt.Errorf("expected '(', got %s", token.Value)
	}

	for {
		token = l.NextToken()

		if token.Type == lexer.PRIMARY {
			if err := readPrimaryKey(l, stmt); err != nil {
				return nil, err
			}
		} else if token.Type == lexer.IDENTIFIER {
			fieldName := token.Value

			token = l.NextToken()
			fieldType, err := parseDataType(token)
			if err != nil {
				return nil, err
			}

			notNull := false
			var defaultValue types.Field

			for {
				token = l.NextToken()
				if token.Type == lexer.NOT {
					token = l.NextToken()
					if err := expectToken(token, lexer.NULL); err != nil {
						return nil, err
					}
					notNull = true
				} else if token.Type == lexer.DEFAULT {
					defaultValue, err = parseValue(l)
					if err != nil {
						return nil, err
					}
				} else {
					l.SetPos(token.Position) // Put it back
					break
				}
			}

			stmt.AddField(fieldName, fieldType, notNull, defaultValue)
		}

		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else if token.Type == lexer.RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}

	return stmt, nil
}

func readPrimaryKey(l *lexer.Lexer, stmt *statements.CreateStatement) error {
	token := l.NextToken()
	if err := expectToken(token, lexer.KEY); err != nil {
		return err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.LPAREN); err != nil {
		return err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return err
	}

	stmt.SetPrimaryKey(token.Value)
	token = l.NextToken()
	if err := expectToken(token, lexer.RPAREN); err != nil {
		return err
	}
	return nil
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
