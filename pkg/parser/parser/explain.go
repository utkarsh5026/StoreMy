package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// parseExplainStatement parses an EXPLAIN statement from the lexer.
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
func parseExplainStatement(l *lexer.Lexer) (statements.Statement, error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.EXPLAIN); err != nil {
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
//
// Parameters:
//   - l: Lexer instance positioned after the EXPLAIN token
//
// Returns:
//   - statements.ExplainOptions: Structure containing the parsed options
//   - error: Returns an error if FORMAT type is invalid (not TEXT or JSON)
func parseExplainOptions(l *lexer.Lexer) (statements.ExplainOptions, error) {
	options := statements.ExplainOptions{
		Analyze: false,
		Format:  "TEXT",
	}

	token := l.NextToken()

	if token.Type == lexer.ANALYZE {
		options.Analyze = true
		token = l.NextToken()
	}

	if token.Type == lexer.FORMAT {
		formatType, err := parseValueWithType(l, lexer.IDENTIFIER)
		if err != nil {
			return options, fmt.Errorf("expected format type (TEXT or JSON): %w", err)
		}

		if formatType != "TEXT" && formatType != "JSON" {
			return options, fmt.Errorf("invalid format type: %s (must be TEXT or JSON)", formatType)
		}
		options.Format = formatType
		token = l.NextToken()
	}

	l.SetPos(token.Position)
	return options, nil
}

// parseExplainableStatement parses the statement that follows the EXPLAIN keyword.
// This function determines the type of statement and delegates to the appropriate parser.
//
// Supported statement types:
//   - SELECT: Query data from tables
//   - INSERT: Insert data into tables
//   - UPDATE: Update existing data
//   - DELETE: Delete data from tables
//   - CREATE: Create tables or indexes
//   - DROP: Drop tables or indexes
//
// Parameters:
//   - l: Lexer instance positioned at the statement keyword
//
// Returns:
//   - statements.Statement: The parsed statement that will be explained
//   - error: Returns an error if the statement type is not supported for EXPLAIN
func parseExplainableStatement(l *lexer.Lexer) (statements.Statement, error) {
	token := l.NextToken()
	startPos := token.Position
	l.SetPos(startPos)

	switch token.Type {
	case lexer.SELECT:
		return parseSelectStatement(l)
	case lexer.INSERT:
		return parseInsertStatement(l)
	case lexer.UPDATE:
		return parseUpdateStatement(l)
	case lexer.DELETE:
		return parseDeleteStatement(l)
	case lexer.CREATE:
		return parseExplainCreateStatement(l, startPos)
	case lexer.DROP:
		return parseExplainDropStatement(l, startPos)
	default:
		return nil, fmt.Errorf("EXPLAIN can only be used with SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP statements, got %s", token.Value)
	}
}

// parseExplainCreateStatement parses CREATE TABLE or CREATE INDEX statements within EXPLAIN.
// This function determines the specific type of CREATE statement and delegates to the appropriate parser.
//
// Supported CREATE statements:
//   - CREATE TABLE: Creates a new table
//   - CREATE INDEX: Creates a new index
//
// Parameters:
//   - l: Lexer instance positioned at the CREATE token
//   - startPos: Position to reset the lexer to before parsing
//
// Returns:
//   - statements.Statement: The parsed CREATE statement
//   - error: Returns an error if neither TABLE nor INDEX follows CREATE
func parseExplainCreateStatement(l *lexer.Lexer, startPos int) (statements.Statement, error) {
	nextToken := l.NextToken()
	l.SetPos(startPos)

	switch nextToken.Type {
	case lexer.TABLE:
		return parseCreateStatement(l)
	case lexer.INDEX:
		return parseCreateIndexStatement(l)
	default:
		return nil, fmt.Errorf("expected TABLE or INDEX after CREATE in EXPLAIN, got %s", nextToken.Value)
	}
}

// parseExplainDropStatement parses DROP TABLE or DROP INDEX statements within EXPLAIN.
// This function determines the specific type of DROP statement and delegates to the appropriate parser.
//
// Supported DROP statements:
//   - DROP TABLE: Drops an existing table
//   - DROP INDEX: Drops an existing index
//
// Parameters:
//   - l: Lexer instance positioned at the DROP token
//   - startPos: Position to reset the lexer to before parsing
//
// Returns:
//   - statements.Statement: The parsed DROP statement
//   - error: Returns an error if neither TABLE nor INDEX follows DROP
func parseExplainDropStatement(l *lexer.Lexer, startPos int) (statements.Statement, error) {
	nextToken := l.NextToken()
	l.SetPos(startPos)

	switch nextToken.Type {
	case lexer.TABLE:
		return parseDropStatement(l)
	case lexer.INDEX:
		return parseDropIndexStatement(l)
	default:
		return nil, fmt.Errorf("expected TABLE or INDEX after DROP in EXPLAIN, got %s", nextToken.Value)
	}
}
