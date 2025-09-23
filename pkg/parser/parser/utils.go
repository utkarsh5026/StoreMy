package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
)

func expectToken(t lexer.Token, expected lexer.TokenType) error {
	if t.Type != expected {
		return fmt.Errorf("expected %s, got %s", expected.String(), t.Value)
	}
	return nil
}
