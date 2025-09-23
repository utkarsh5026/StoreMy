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
	if token.Type != lexer.CREATE {
		return nil, fmt.Errorf("expected CREATE, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.TABLE {
		return nil, fmt.Errorf("expected TABLE, got %s", token.Value)
	}

	ifNotExists := false
	token = l.NextToken()
	if token.Type == lexer.IF {
		token = l.NextToken()
		if token.Type != lexer.NOT {
			return nil, fmt.Errorf("expected NOT after IF, got %s", token.Value)
		}
		token = l.NextToken()
		if token.Type != lexer.EXISTS {
			return nil, fmt.Errorf("expected EXISTS after NOT, got %s", token.Value)
		}
		ifNotExists = true
		token = l.NextToken()
	}

	if token.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", token.Value)
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
					if token.Type == lexer.NULL {
						notNull = true
					} else {
						return nil, fmt.Errorf("expected NULL after NOT, got %s", token.Value)
					}
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
	if token.Type != lexer.KEY {
		return fmt.Errorf("expected KEY after PRIMARY, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.LPAREN {
		return fmt.Errorf("expected '(' after PRIMARY KEY, got %s", token.Value)
	}

	token = l.NextToken()
	if token.Type != lexer.IDENTIFIER {
		return fmt.Errorf("expected field name in PRIMARY KEY, got %s", token.Value)
	}

	stmt.SetPrimaryKey(token.Value)
	token = l.NextToken()
	if token.Type != lexer.RPAREN {
		return fmt.Errorf("expected ')' after PRIMARY KEY field, got %s", token.Value)
	}
	return nil
}
