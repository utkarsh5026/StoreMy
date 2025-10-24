package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// parseShowStatement parses a SHOW statement from the lexer.
// The SHOW statement is used to display database metadata information.
//
// Syntax:
//
//	SHOW INDEXES [FROM table_name]
//
// Options:
//   - FROM table_name: If specified, shows only indexes for the specified table
//
// Parameters:
//   - l: Lexer instance positioned at the SHOW token
//
// Returns:
//   - statements.Statement: A ShowIndexesStatement containing the optional table name
//   - error: Returns an error if parsing fails or the statement is invalid
func parseShowStatement(l *lexer.Lexer) (statements.Statement, error) {
	if err := expectTokenSequence(l, lexer.SHOW, lexer.INDEXES); err != nil {
		return nil, err
	}

	tableName := ""
	token := l.NextToken()
	if token.Type == lexer.FROM {
		token = l.NextToken()
		if token.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected table name after FROM, got %s", token.Value)
		}
		tableName = token.Value
	} else {
		l.SetPos(token.Position)
	}

	return statements.NewShowIndexesStatement(tableName), nil
}
