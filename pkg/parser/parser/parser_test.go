package parser

import (
	"storemy/pkg/parser/lexer"
	"testing"
)

func NewLexer(s string) *lexer.Lexer {
	return lexer.NewLexer(s)
}

func TestParseStatement_EmptyStatement(t *testing.T) {
	parser := &Parser{}

	_, err := parser.ParseStatement("")
	if err == nil {
		t.Error("expected error for empty statement")
	}

	if err.Error() != "empty statement" {
		t.Errorf("expected 'empty statement', got %s", err.Error())
	}
}

func TestParseStatement_UnsupportedStatement(t *testing.T) {
	parser := &Parser{}

	_, err := parser.ParseStatement("SELECT * FROM users")
	if err == nil {
		t.Error("expected error for unsupported statement")
	}

	if err.Error() != "unsupported statement type: SELECT" {
		t.Errorf("expected 'unsupported statement type: SELECT', got %s", err.Error())
	}
}
