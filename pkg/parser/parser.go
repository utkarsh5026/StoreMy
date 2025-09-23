package parser

import (
	"errors"
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"strconv"
)

type Parser struct {
}

func (p *Parser) ParseStatement(sql string) (statements.Statement, error) {
	lexer := NewLexer(sql)
	token := lexer.NextToken()
	if token.Type == EOF {
		return nil, errors.New("empty statement")
	}

	lexer.pos = 0

	switch token.Type {
	case INSERT:
		return p.parseInsertStatement(lexer)
	case CREATE:
		return p.parseCreateStatement(lexer)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", token.Value)
	}

}

func (p *Parser) parseInsertStatement(lexer *Lexer) (*statements.InsertStatement, error) {
	token := lexer.NextToken()
	if token.Type != INSERT {
		return nil, fmt.Errorf("expected INSERT, got %s", token.Value)
	}

	token = lexer.NextToken()
	if token.Type != INTO {
		return nil, fmt.Errorf("expected INTO, got %s", token.Value)
	}

	token = lexer.NextToken()
	if token.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", token.Value)
	}

	tableName := token.Value
	statement := statements.NewInsertStatement(tableName)

	token = lexer.NextToken()
	if token.Type == LPAREN {
		fields, err := p.parseFieldList(lexer)
		if err != nil {
			return nil, err
		}
		statement.AddFieldNames(fields)
		token = lexer.NextToken()
	}

	if token.Type == VALUES {
		return p.parseInsertValues(lexer, statement)
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT, got %s", token.Value)
	}
}

func (p *Parser) parseCreateStatement(lexer *Lexer) (*statements.CreateStatement, error) {
	// CREATE TABLE [IF NOT EXISTS] table_name (field_def1, field_def2, ..., [PRIMARY KEY (field)])
	token := lexer.NextToken()
	if token.Type != CREATE {
		return nil, fmt.Errorf("expected CREATE, got %s", token.Value)
	}

	token = lexer.NextToken()
	if token.Type != TABLE {
		return nil, fmt.Errorf("expected TABLE, got %s", token.Value)
	}

	ifNotExists := false
	token = lexer.NextToken()
	if token.Type == IF {
		token = lexer.NextToken()
		if token.Type != NOT {
			return nil, fmt.Errorf("expected NOT after IF, got %s", token.Value)
		}
		token = lexer.NextToken()
		if token.Type != EXISTS {
			return nil, fmt.Errorf("expected EXISTS after NOT, got %s", token.Value)
		}
		ifNotExists = true
		token = lexer.NextToken()
	}

	if token.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", token.Value)
	}

	tableName := token.Value
	stmt := statements.NewCreateStatement(tableName, ifNotExists)

	token = lexer.NextToken()
	if token.Type != LPAREN {
		return nil, fmt.Errorf("expected '(', got %s", token.Value)
	}

	for {
		token = lexer.NextToken()

		if token.Type == PRIMARY {
			if err := readPrimaryKey(lexer, stmt); err != nil {
				return nil, err
			}
		} else if token.Type == IDENTIFIER {
			fieldName := token.Value

			token = lexer.NextToken()
			fieldType, err := parseDataType(token)
			if err != nil {
				return nil, err
			}

			notNull := false
			var defaultValue types.Field

			for {
				token = lexer.NextToken()
				if token.Type == NOT {
					token = lexer.NextToken()
					if token.Type == NULL {
						notNull = true
					} else {
						return nil, fmt.Errorf("expected NULL after NOT, got %s", token.Value)
					}
				} else if token.Type == DEFAULT {
					defaultValue, err = parseValue(lexer)
					if err != nil {
						return nil, err
					}
				} else {
					lexer.pos = token.Position // Put it back
					break
				}
			}

			stmt.AddField(fieldName, fieldType, notNull, defaultValue)
		}

		token = lexer.NextToken()
		if token.Type == COMMA {
			continue
		} else if token.Type == RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}

	return stmt, nil
}

func parseValueList(lexer *Lexer) ([]types.Field, error) {
	values := make([]types.Field, 0)

	for {
		value, err := parseValue(lexer)
		if err != nil {
			return nil, err
		}
		values = append(values, value)

		token := lexer.NextToken()
		if token.Type == COMMA {
			continue
		} else if token.Type == RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}

	return values, nil
}

func (p *Parser) parseInsertValues(lexer *Lexer, stmt *statements.InsertStatement) (*statements.InsertStatement, error) {
	for {
		token := lexer.NextToken()
		if token.Type != LPAREN {
			return nil, fmt.Errorf("expected '(', got %s", token.Value)
		}

		values, err := parseValueList(lexer)
		if err != nil {
			return nil, err
		}

		stmt.AddValues(values)

		token = lexer.NextToken()
		if token.Type == COMMA {
			continue
		} else {
			lexer.pos = token.Position // Put it back
			break
		}
	}

	return stmt, nil
}

func (p *Parser) parseFieldList(lexer *Lexer) ([]string, error) {
	fields := make([]string, 0)
	for {
		token := lexer.NextToken()
		if token.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected field name, got %s", token.Value)
		}

		fields = append(fields, token.Value)
		token = lexer.NextToken()
		if token.Type == COMMA {
			continue
		} else if token.Type == RPAREN {
			break
		} else {
			return nil, fmt.Errorf("expected comma or right parenthesis, got %s", token.Value)
		}
	}

	return fields, nil
}

func parseValue(lexer *Lexer) (types.Field, error) {
	token := lexer.NextToken()

	switch token.Type {
	case STRING:
		return types.NewStringField(token.Value, types.StringMaxSize), nil
	case INT:
		value, err := strconv.Atoi(token.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value: %s", token.Value)
		}
		return types.NewIntField(int32(value)), nil
	case NULL:
		return nil, nil // NULL value
	default:
		return nil, fmt.Errorf("expected value, got %s", token.Value)
	}
}

func parseDataType(token Token) (types.Type, error) {
	switch token.Type {
	case INT:
		return types.IntType, nil
	case VARCHAR, TEXT:
		return types.StringType, nil
	case BOOLEAN:
		return types.BoolType, nil
	case FLOAT:
		return types.FloatType, nil
	default:
		return 0, fmt.Errorf("unknown data type: %s", token.Value)
	}
}

func readPrimaryKey(lexer *Lexer, stmt *statements.CreateStatement) error {
	token := lexer.NextToken()
	if token.Type != KEY {
		return fmt.Errorf("expected KEY after PRIMARY, got %s", token.Value)
	}

	token = lexer.NextToken()
	if token.Type != LPAREN {
		return fmt.Errorf("expected '(' after PRIMARY KEY, got %s", token.Value)
	}

	token = lexer.NextToken()
	if token.Type != IDENTIFIER {
		return fmt.Errorf("expected field name in PRIMARY KEY, got %s", token.Value)
	}

	stmt.SetPrimaryKey(token.Value)
	token = lexer.NextToken()
	if token.Type != RPAREN {
		return fmt.Errorf("expected ')' after PRIMARY KEY field, got %s", token.Value)
	}
	return nil
}
