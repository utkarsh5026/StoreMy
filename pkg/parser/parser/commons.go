package parser

import (
	"fmt"
	"slices"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"strconv"
)

// expectToken validates that the given token matches the expected token type.
func expectToken(t lexer.Token, expected lexer.TokenType) error {
	if t.Type != expected {
		return fmt.Errorf("expected %s, got %s", expected.String(), t.Value)
	}
	return nil
}

// parseValue parses the next token from the lexer and converts it to a Field type.
// Supports STRING, INT, and NULL token types.
func parseValue(l *lexer.Lexer) (types.Field, error) {
	token := l.NextToken()

	switch token.Type {
	case lexer.STRING:
		return types.NewStringField(token.Value, types.StringMaxSize), nil
	case lexer.INT:
		value, err := strconv.ParseInt(token.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value: %s", token.Value)
		}
		return types.NewIntField(value), nil
	case lexer.NULL:
		return nil, nil // NULL value
	default:
		return nil, fmt.Errorf("expected value, got %s", token.Value)
	}
}

// parseOperator converts a string operator into a Predicate type.
// Supports standard comparison operators: =, >, <, >=, <=, !=, <>
func parseOperator(op string) (primitives.Predicate, error) {
	switch op {
	case "=":
		return primitives.Equals, nil
	case ">":
		return primitives.GreaterThan, nil
	case "<":
		return primitives.LessThan, nil
	case ">=":
		return primitives.GreaterThanOrEqual, nil
	case "<=":
		return primitives.LessThanOrEqual, nil
	case "!=", "<>":
		return primitives.NotEqual, nil
	default:
		return primitives.Equals, fmt.Errorf("unknown operator: %s", op)
	}
}

// expectTokenSequence validates that the lexer produces a sequence of tokens
// matching the expected token types in order.
func expectTokenSequence(l *lexer.Lexer, expectedTypes ...lexer.TokenType) error {
	for _, expectedType := range expectedTypes {
		token := l.NextToken()
		if err := expectToken(token, expectedType); err != nil {
			return err
		}
	}
	return nil
}

// parseTableWithAlias parses a table reference with optional alias from the lexer.
// Example: "users u" returns ("users", "u", nil), "users" returns ("users", "users", nil)
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

// parseValueWithType parses the next token and validates it matches one of the accepted types.
func parseValueWithType(l *lexer.Lexer, acceptedTypes ...lexer.TokenType) (string, error) {
	token := l.NextToken()
	if slices.Contains(acceptedTypes, token.Type) {
		return token.Value, nil
	}
	return "", fmt.Errorf("expected value of type %v, got %s", acceptedTypes, token.Value)
}

// parseDelimitedList is a generic helper for parsing comma-separated lists with terminators.
// It repeatedly calls parseItem until it encounters the terminator token.
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
		switch token.Type {
		case delimiter:
			continue
		case terminator:
			break
		default:
			return nil, fmt.Errorf("expected '%s' or '%s', got %s",
				getTokenName(delimiter), getTokenName(terminator), token.Value)
		}
		break
	}

	return items, nil
}

// getTokenName returns a human-readable name for common token types.
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
