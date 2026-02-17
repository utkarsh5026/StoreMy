package parser

import (
	"fmt"
	"storemy/pkg/parser/statements"
)

type ShowIndexesParser struct{}

// parseShowStatement parses a SHOW statement from the
// The SHOW statement is used to display database metadata information.
//
// Syntax:
//
//	SHOW INDEXES [FROM table_name]
func (s *ShowIndexesParser) Parse(l *Lexer) (statements.Statement, error) {
	if err := expectTokenSequence(l, SHOW, INDEXES); err != nil {
		return nil, err
	}

	tableName := ""
	token := l.NextToken()
	if token.Type == FROM {
		var err error
		tableName, err = parseValueWithType(l, IDENTIFIER)
		if err != nil {
			return nil, fmt.Errorf("expected table name after FROM: %w", err)
		}
	} else {
		l.SetPos(token.Position)
	}

	return statements.NewShowIndexesStatement(tableName), nil
}

type ExplainStatementParser struct{}

// parseExplainStatement parses an EXPLAIN statement from the
// The EXPLAIN statement is used to show the execution plan or analyze query performance.
//
// Syntax:
//
//	EXPLAIN [ANALYZE] [FORMAT {TEXT|JSON}] <statement>
//
// Options:
//   - ANALYZE: If specified, executes the statement and includes actual execution statistics
//   - FORMAT: Specifies the output format (TEXT or JSON), defaults to TEXT
//
// Supported statements:
//   - SELECT
//   - INSERT
//   - UPDATE
//   - DELETE
//   - CREATE TABLE/INDEX
//   - DROP TABLE/INDEX
//
// Parameters:
//   - l: Lexer instance positioned at the EXPLAIN token
//
// Returns:
//   - statements.Statement: An ExplainStatement containing the wrapped statement and options
//   - error: Returns an error if parsing fails or the statement is invalid
func (e *ExplainStatementParser) Parse(l *Lexer) (statements.Statement, error) {
	token := l.NextToken()
	if err := expectToken(token, EXPLAIN); err != nil {
		return nil, err
	}

	options, err := parseExplainOptions(l)
	if err != nil {
		return nil, err
	}

	stmt, err := parseExplainableStatement(l)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statement in EXPLAIN: %w", err)
	}

	return statements.NewExplainStatement(stmt, options), nil
}

// parseExplainOptions parses the optional ANALYZE and FORMAT clauses of an EXPLAIN statement.
// Both options are optional and can appear in any order.
//
// Syntax:
//
//	[ANALYZE] [FORMAT {TEXT|JSON}]
//
// Default values:
//   - Analyze: false
//   - Format: "TEXT"
func parseExplainOptions(l *Lexer) (statements.ExplainOptions, error) {
	options := statements.ExplainOptions{
		Analyze: false,
		Format:  "TEXT",
	}

	token := l.NextToken()

	if token.Type == ANALYZE {
		options.Analyze = true
		token = l.NextToken()
	}

	if token.Type == FORMAT {
		formatToken := l.NextToken()
		var formatType string

		switch {
		case formatToken.Type == TEXT:
			formatType = "TEXT"
		case formatToken.Type == IDENTIFIER && formatToken.Value == "JSON":
			formatType = "JSON"
		default:
			return options, fmt.Errorf("invalid format type: %s (must be TEXT or JSON)", formatToken.Value)
		}

		options.Format = formatType
		token = l.NextToken()
	}

	l.SetPos(token.Position)
	return options, nil
}

// parseExplainableStatement parses the statement that follows the EXPLAIN keyword.
// This function determines the type of statement and delegates to the appropriate parser.
func parseExplainableStatement(l *Lexer) (statements.Statement, error) {
	token := l.NextToken()
	startPos := token.Position
	l.SetPos(startPos)

	switch token.Type {
	case SELECT:
		return (&SelectParser{}).Parse(l)
	case INSERT:
		return (&InsertParser{}).Parse(l)
	case UPDATE:
		return (&UpdateParser{}).Parse(l)
	case DELETE:
		return (&DeleteParser{}).Parse(l)
	case CREATE:
		return parseExplainCreateStatement(l, startPos)
	case DROP:
		return parseExplainDropStatement(l, startPos)
	default:
		return nil, fmt.Errorf("EXPLAIN can only be used with SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP statements, got %s", token.Value)
	}
}

// parseExplainCreateStatement parses CREATE TABLE or CREATE INDEX statements within EXPLAIN.
func parseExplainCreateStatement(l *Lexer, startPos int) (statements.Statement, error) {
	nextToken := l.NextToken()
	l.SetPos(startPos)

	switch nextToken.Type {
	case TABLE:
		return (&CreateStatementParser{}).Parse(l)
	case INDEX:
		return (&CreateIndexParser{}).Parse(l)
	default:
		return nil, fmt.Errorf("expected TABLE or INDEX after CREATE in EXPLAIN, got %s", nextToken.Value)
	}
}

// parseExplainDropStatement parses DROP TABLE or DROP INDEX statements within EXPLAIN.
func parseExplainDropStatement(l *Lexer, startPos int) (statements.Statement, error) {
	nextToken := l.NextToken()
	l.SetPos(startPos)

	switch nextToken.Type {
	case TABLE:
		return (&DropStatementParser{}).Parse(l)
	case INDEX:
		return (&DropIndexParser{}).Parse(l)
	default:
		return nil, fmt.Errorf("expected TABLE or INDEX after DROP in EXPLAIN, got %s", nextToken.Value)
	}
}
