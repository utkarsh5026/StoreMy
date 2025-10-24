package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// parseExplainStatement parses an EXPLAIN statement
// Syntax: EXPLAIN [ANALYZE] [FORMAT {TEXT|JSON}] <statement>
func parseExplainStatement(l *lexer.Lexer) (statements.Statement, error) {
	token := l.NextToken()
	if token.Type != lexer.EXPLAIN {
		return nil, fmt.Errorf("expected EXPLAIN, got %s", token.Value)
	}

	options := statements.ExplainOptions{
		Analyze: false,
		Format:  "TEXT",
	}

	token = l.NextToken()
	if token.Type == lexer.ANALYZE {
		options.Analyze = true
		token = l.NextToken()
	}

	if token.Type == lexer.FORMAT {
		formatToken := l.NextToken()
		if formatToken.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected format type (TEXT or JSON), got %s", formatToken.Value)
		}

		formatType := formatToken.Value
		if formatType != "TEXT" && formatType != "JSON" {
			return nil, fmt.Errorf("invalid format type: %s (must be TEXT or JSON)", formatType)
		}
		options.Format = formatType
		token = l.NextToken()
	}

	startPos := token.Position

	// Get the remaining SQL by reconstructing from current position
	// Since the lexer doesn't expose the raw input, we need to work with the tokens
	// For now, we'll parse based on the next token type
	l.SetPos(startPos)

	// Parse the underlying statement
	var stmt statements.Statement
	var err error

	switch token.Type {
	case lexer.SELECT:
		stmt, err = parseSelectStatement(l)
	case lexer.INSERT:
		stmt, err = parseInsertStatement(l)
	case lexer.UPDATE:
		stmt, err = parseUpdateStatement(l)
	case lexer.DELETE:
		stmt, err = parseDeleteStatement(l)
	case lexer.CREATE:
		nextToken := l.NextToken()
		l.SetPos(startPos)
		switch nextToken.Type {
		case lexer.TABLE:
			stmt, err = parseCreateStatement(l)
		case lexer.INDEX:
			stmt, err = parseCreateIndexStatement(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after CREATE in EXPLAIN, got %s", nextToken.Value)
		}
	case lexer.DROP:
		nextToken := l.NextToken()
		l.SetPos(startPos)
		switch nextToken.Type {
		case lexer.TABLE:
			stmt, err = parseDropStatement(l)
		case lexer.INDEX:
			stmt, err = parseDropIndexStatement(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after DROP in EXPLAIN, got %s", nextToken.Value)
		}
	default:
		return nil, fmt.Errorf("EXPLAIN can only be used with SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP statements, got %s", token.Value)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse statement in EXPLAIN: %w", err)
	}

	return statements.NewExplainStatement(stmt, options), nil
}
