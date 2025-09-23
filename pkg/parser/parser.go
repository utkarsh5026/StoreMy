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

func (p *Parser) parseValueList(lexer *Lexer) ([]types.Field, error) {
	values := make([]types.Field, 0)

	for {
		value, err := p.parseValue(lexer)
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

		values, err := p.parseValueList(lexer)
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

func (p *Parser) parseValue(lexer *Lexer) (types.Field, error) {
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
