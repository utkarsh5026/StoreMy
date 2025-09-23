package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/types"
	"strconv"
)

func expectToken(t lexer.Token, expected lexer.TokenType) error {
	if t.Type != expected {
		return fmt.Errorf("expected %s, got %s", expected.String(), t.Value)
	}
	return nil
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
