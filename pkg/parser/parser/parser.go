package parser

import (
	"errors"
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// ParseStatement parses a SQL statement string and returns the corresponding Statement object.
// This is the main entry point for the parser component, which converts SQL text into
// an abstract syntax tree (AST) representation that can be executed by the query planner.
//
// Supported SQL statements:
//   - SELECT: Query data from tables
//   - INSERT: Add new rows to tables
//   - UPDATE: Modify existing rows
//   - DELETE: Remove rows from tables
//   - CREATE TABLE: Define new table schemas
//   - CREATE INDEX: Create indexes on tables
//   - DROP TABLE: Remove tables
//   - DROP INDEX: Remove indexes
//   - EXPLAIN: Show query execution plan
//   - SHOW INDEXES: Display index information
//
// Parameters:
//   - sql: The SQL statement string to parse
//
// Returns:
//   - statements.Statement: The parsed statement object implementing the Statement interface
//   - error: An error if parsing fails or the statement type is unsupported
func ParseStatement(sql string) (statements.Statement, error) {
	l := lexer.NewLexer(sql)
	token := l.NextToken()
	if token.Type == lexer.EOF {
		return nil, errors.New("empty statement")
	}

	switch token.Type {
	case lexer.INSERT:
		l.SetPos(0)
		return parseInsertStatement(l)
	case lexer.CREATE:
		secondToken := l.NextToken()
		l.SetPos(0)
		switch secondToken.Type {
		case lexer.TABLE:
			return parseCreateStatement(l)
		case lexer.INDEX:
			return parseCreateIndexStatement(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after CREATE, got %s", secondToken.Value)
		}
	case lexer.DROP:
		secondToken := l.NextToken()
		l.SetPos(0)
		switch secondToken.Type {
		case lexer.TABLE:
			return parseDropStatement(l)
		case lexer.INDEX:
			return parseDropIndexStatement(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after DROP, got %s", secondToken.Value)
		}
	case lexer.DELETE:
		l.SetPos(0)
		return parseDeleteStatement(l)
	case lexer.UPDATE:
		l.SetPos(0)
		return parseUpdateStatement(l)
	case lexer.SELECT:
		l.SetPos(0)
		return parseSelectStatement(l)
	case lexer.EXPLAIN:
		l.SetPos(0)
		return parseExplainStatement(l)
	case lexer.SHOW:
		l.SetPos(0)
		return parseShowStatement(l)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", token.Value)
	}

}
