package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/types"
	"strconv"
)

// expectToken validates that the given token matches the expected token type.
// Returns an error if the token type doesn't match the expected type.
func expectToken(t lexer.Token, expected lexer.TokenType) error {
	if t.Type != expected {
		return fmt.Errorf("expected %s, got %s", expected.String(), t.Value)
	}
	return nil
}

// parseValue parses the next token from the lexer and converts it to a Field type.
// Supports STRING, INT, and NULL token types.
// Returns the parsed Field or nil for NULL values, along with any parsing errors.
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

// parseOperator converts a string operator into a Predicate type.
// Supports standard comparison operators: =, >, <, >=, <=, !=, <>
// Returns the corresponding Predicate constant and any parsing errors.
func parseOperator(op string) (types.Predicate, error) {
	switch op {
	case "=":
		return types.Equals, nil
	case ">":
		return types.GreaterThan, nil
	case "<":
		return types.LessThan, nil
	case ">=":
		return types.GreaterThanOrEqual, nil
	case "<=":
		return types.LessThanOrEqual, nil
	case "!=", "<>":
		return types.NotEqual, nil
	default:
		return types.Equals, fmt.Errorf("unknown operator: %s", op)
	}
}

func expectTokenSequence(l *lexer.Lexer, expectedTypes ...lexer.TokenType) error {
	for _, expectedType := range expectedTypes {
		token := l.NextToken()
		if err := expectToken(token, expectedType); err != nil {
			return err
		}
	}
	return nil
}

func parseTableWithAlias(l *lexer.Lexer) (tableName, alias string, err error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return "", "", err
	}

	tableName = token.Value
	alias = tableName

	nextToken := l.NextToken()
	if nextToken.Type == lexer.IDENTIFIER {
		alias = nextToken.Value
	} else {
		l.SetPos(nextToken.Position)
	}

	return tableName, alias, nil
}
