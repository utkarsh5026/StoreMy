package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// parseDropStatement parses a DROP TABLE SQL statement from the lexer tokens.
// It expects the format: DROP TABLE [IF EXISTS] table_name
func parseDropStatement(l *lexer.Lexer) (*statements.DropStatement, error) {
	if err := expectTokenSequence(l, lexer.DROP, lexer.TABLE); err != nil {
		return nil, err
	}

	ifExists := false
	token := l.NextToken()
	if token.Type == lexer.IF {
		l.NextToken()
		token = l.NextToken()
		if token.Type != lexer.EXISTS {
			return nil, fmt.Errorf("expected EXISTS after IF, got: %s", token.Value)
		}
		ifExists = true
		token = l.NextToken()
	} else {
		token = l.NextToken()
	}

	if token.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got: %s", token.Value)
	}

	tableName := token.Value
	stmt := statements.NewDropStatement(tableName, ifExists)

	if err := stmt.Validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}
