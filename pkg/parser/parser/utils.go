package parser

import (
	"fmt"
	"slices"
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

func parseValueWithType(l *lexer.Lexer, acceptedTypes ...lexer.TokenType) (string, error) {
	token := l.NextToken()
	if slices.Contains(acceptedTypes, token.Type) {
		return token.Value, nil
	}
	return "", fmt.Errorf("expected value of type %v, got %s", acceptedTypes, token.Value)
}

// Common list parsing pattern
func parseDelimitedList[T any](
	l *lexer.Lexer,
	parseItem func(*lexer.Lexer) (T, error),
	delimiter lexer.TokenType,
	terminator lexer.TokenType,
) ([]T, error) {
	var items []T

	for {
		item, err := parseItem(l)
		if err != nil {
			return nil, err
		}
		items = append(items, item)

		token := l.NextToken()
		if token.Type == delimiter {
			continue
		} else if token.Type == terminator {
			break
		} else {
			return nil, fmt.Errorf("expected '%s' or '%s', got %s",
				getTokenName(delimiter), getTokenName(terminator), token.Value)
		}
	}

	return items, nil
}

func getTokenName(tokenType lexer.TokenType) string {
	switch tokenType {
	case lexer.COMMA:
		return ","
	case lexer.RPAREN:
		return ")"
	default:
		return string(rune(tokenType))
	}
}
