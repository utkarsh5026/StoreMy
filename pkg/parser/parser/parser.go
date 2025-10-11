package parser

import (
	"errors"
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

func ParseStatement(sql string) (statements.Statement, error) {
	l := lexer.NewLexer(sql)
	token := l.NextToken()
	if token.Type == lexer.EOF {
		return nil, errors.New("empty statement")
	}

	l.SetPos(0)

	switch token.Type {
	case lexer.INSERT:
		return parseInsertStatement(l)
	case lexer.CREATE:
		return parseCreateStatement(l)
	case lexer.DROP:
		return parseDropStatement(l)
	case lexer.DELETE:
		return parseDeleteStatement(l)
	case lexer.UPDATE:
		return parseUpdateStatement(l)
	case lexer.SELECT:
		return parseSelectStatement(l)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", token.Value)
	}

}
